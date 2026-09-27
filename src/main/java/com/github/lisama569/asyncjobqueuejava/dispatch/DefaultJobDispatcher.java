package com.github.lisama569.asyncjobqueuejava.dispatch;

import com.github.lisama569.asyncjobqueuejava.job.JobHandler;
import com.github.lisama569.asyncjobqueuejava.job.JobHandlerRegistry;
import com.github.lisama569.asyncjobqueuejava.job.JobRecord;
import com.github.lisama569.asyncjobqueuejava.job.JobRepository;
import com.github.lisama569.asyncjobqueuejava.job.JobStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 调度器核心：
 * <ul>
 *   <li>每个分组一条虚拟线程协程，组内严格串行、按 created_at 顺序领取 PENDING；</li>
 *   <li>全局 Semaphore 保证同时执行的任务数不超过 maxConcurrency；</li>
 *   <li>组锁 + 全局信号量固定加锁顺序（信号量 -> 组锁），避免死锁；</li>
 *   <li>所有状态迁移走带版本号的 CAS，杜绝状态回退与丢失更新；</li>
 *   <li>失败指数退避重试，耗尽后进入 DEAD 并保留可区分原因的失败信息。</li>
 * </ul>
 */
@Component
public class DefaultJobDispatcher implements JobDispatcher {

    private static final Logger log = LoggerFactory.getLogger(DefaultJobDispatcher.class);

    private final JobRepository repository;
    private final JobHandlerRegistry handlers;
    private final JobQueueProperties properties;
    private final CapacityGauge capacity;
    private final TransactionTemplate tx;

    private final Semaphore concurrency;
    private final ConcurrentHashMap<String, GroupRunner> runners = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> groupLocks = new ConcurrentHashMap<>();
    private final ExecutorService executor;
    private volatile boolean stopping = false;

    public DefaultJobDispatcher(JobRepository repository, JobHandlerRegistry handlers,
                                JobQueueProperties properties, CapacityGauge capacity,
                                TransactionTemplate tx) {
        this.repository = repository;
        this.handlers = handlers;
        this.properties = properties;
        this.capacity = capacity;
        this.tx = tx;
        this.concurrency = new Semaphore(properties.getMaxConcurrency(), true);
        java.util.concurrent.ThreadFactory vt = Thread.ofVirtual()
                .name("job-vt-", 0)
                .uncaughtExceptionHandler((t, e) ->
                        log.error("[DISPATCHER] 虚拟线程未捕获异常 thread={}", t, e))
                .factory();
        this.executor = Executors.newThreadPerTaskExecutor(vt);
        log.info("[DISPATCHER] 初始化 maxConcurrency={} queueCapacity={}",
                properties.getMaxConcurrency(), properties.getQueueCapacity());
    }

    @Override
    public void notifyGroup(String groupKey) {
        if (stopping) {
            return;
        }
        runners.compute(groupKey, (g, existing) -> {
            if (existing == null) {
                GroupRunner runner = new GroupRunner(g);
                executor.submit(runner::run);
                log.debug("[DISPATCHER] 启动分组协程 group={}", g);
                return runner;
            }
            existing.wake();
            return existing;
        });
    }

    @Override
    public void dispatch(JobRecord job) {
        if (stopping) {
            return;
        }
        executor.submit(() -> runDirect(job.getId()));
    }

    /**
     * 重试定时器到点 / 恢复的直接执行路径：仍然经过全局信号量与组锁，
     * 因此与同组的排队任务不会并发。
     */
    private void runDirect(String jobId) {
        JobRecord fresh = repository.findById(jobId).orElse(null);
        if (fresh == null || fresh.getStatus() != JobStatus.RETRY_WAITING) {
            return;
        }
        if (!acquirePermit()) {
            return;
        }
        try {
            synchronized (groupLock(fresh.getGroupKey())) {
                fresh = repository.findById(jobId).orElse(null);
                if (fresh == null || fresh.getStatus() != JobStatus.RETRY_WAITING) {
                    return;
                }
                if (!transition(fresh, JobStatus.RUNNING, List.of(JobStatus.RETRY_WAITING))) {
                    return;
                }
                execute(fresh);
            }
        } finally {
            concurrency.release();
        }
    }

    private Object groupLock(String group) {
        return groupLocks.computeIfAbsent(group, g -> new Object());
    }

    private boolean acquirePermit() {
        boolean interrupted = false;
        try {
            while (!stopping) {
                try {
                    if (concurrency.tryAcquire(200, TimeUnit.MILLISECONDS)) {
                        return true;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                    return false;
                }
            }
            return false;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void execute(JobRecord job) {
        JobHandler handler = handlers.find(job.getJobType()).orElse(null);
        if (handler == null) {
            if (stopping) {
                return;
            }
            finishFailure(job, new IllegalStateException(
                    "未注册的 jobType: " + job.getJobType()), true);
            return;
        }
        Instant started = Instant.now();
        try {
            log.info("[EXEC] begin jobId={} group={} type={} attempt={}/{}",
                    job.getId(), job.getGroupKey(), job.getJobType(),
                    job.getAttempts(), job.getMaxAttempts());
            handler.handle(job);
            long cost = Duration.between(started, Instant.now()).toMillis();
            JobRecord latest = repository.findById(job.getId()).orElse(job);
            latest.setStatus(JobStatus.SUCCEEDED);
            latest.setFinishedAt(Instant.now());
            if (transition(latest, JobStatus.SUCCEEDED, List.of(JobStatus.RUNNING))) {
                capacity.release();
                log.info("[EXEC] end jobId={} result=SUCCEEDED costMs={} attempts={}",
                        job.getId(), cost, latest.getAttempts());
            }
        } catch (Exception e) {
            if (stopping) {
                // 进程关闭导致的中断/异常：不做失败与死信判定，
                // RUNNING 任务保留在库中，由下次启动恢复重投（at-least-once）
                log.warn("[EXEC] jobId={} 在关闭过程中结束，状态保留给重启恢复: {}",
                        job.getId(), e.toString());
                return;
            }
            finishFailure(job, e, false);
        }
    }

    /**
     * 失败收口：决定重试还是死信。
     * @param nonRetryable true 表示不再重试（如无对应处理器）
     */
    private void finishFailure(JobRecord job, Exception error, boolean nonRetryable) {
        JobRecord latest = repository.findById(job.getId()).orElse(job);
        if (latest.getStatus() != JobStatus.RUNNING) {
            // 已被其他路径改状态（理论上不会发生），不动终态
            log.warn("[RETRY] jobId={} 失败收口时状态已为 {} 放弃迁移", job.getId(), latest.getStatus());
            return;
        }
        // attempts 在进入 RUNNING 时已 +1，即“当前已完成的执行次数”
        int attempts = latest.getAttempts();
        String errMsg = error.getClass().getSimpleName() + ": "
                + String.valueOf(error.getMessage());
        latest.setLastError(errMsg);

        if (!nonRetryable && attempts < latest.getMaxAttempts()) {
            // 递增退避：以“下一次”等待时长为 backoff_millis
            long backoff = Math.min(
                    (long) (latest.getBackoffMillis() * properties.getBackoffMultiplier()),
                    properties.getBackoffMaxMillis());
            Instant nextRunAt = Instant.now().plusMillis(backoff);
            latest.setBackoffMillis(backoff);
            latest.setNextRunAt(nextRunAt);
            latest.setFailureReason("第 " + attempts + " 次执行失败，计划于 "
                    + backoff + "ms 后进行第 " + (attempts + 1) + " 次执行；原因=" + errMsg);
            boolean ok = transition(latest, JobStatus.RETRY_WAITING, List.of(JobStatus.RUNNING));
            if (ok) {
                log.warn("[RETRY] jobId={} 第{}/{}次执行失败，{}ms 后重试 reason={}",
                        latest.getId(), attempts, latest.getMaxAttempts(), backoff, errMsg);
            }
            // RETRY_WAITING 仍占队列容量但不占并发许可（许可已在调用方释放）
        } else {
            String reason = nonRetryable
                    ? "不可重试错误，直接进入死信；原因=" + errMsg
                    : "重试耗尽(" + attempts + "/" + latest.getMaxAttempts()
                            + ")，进入死信；最后一次原因=" + errMsg;
            latest.setFailureReason(reason);
            latest.setFinishedAt(Instant.now());
            latest.setNextRunAt(null);
            if (transition(latest, JobStatus.DEAD, List.of(JobStatus.RUNNING))) {
                capacity.release();
                log.error("[DEAD] jobId={} group={} attempts={} reason={}",
                        latest.getId(), latest.getGroupKey(), attempts, reason);
            }
        }
    }

    private boolean transition(JobRecord job, JobStatus to, List<JobStatus> expected) {
        JobStatus from = expected.get(0);
        // 进入 RUNNING 代表一次真实执行开始：执行计数 +1
        if (to == JobStatus.RUNNING) {
            job.setAttempts(job.getAttempts() + 1);
        }
        // 关键：先把目标状态写入内存对象，compareAndUpdate 才会把“新状态”持久化
        job.setStatus(to);
        boolean ok = repository.compareAndUpdate(job, expected);
        if (ok) {
            log.info("[STATE] jobId={} {} -> {}", job.getId(), from, to);
        } else {
            log.warn("[STATE] jobId={} 迁移 {} -> {} 被拒绝(当前版本/状态不匹配)",
                    job.getId(), from, to);
        }
        return ok;
    }

    /**
     * 优雅关闭：不再领取新任务，等待在执行任务结束（由重启恢复兜底中断任务）。
     */
    public void shutdown(long awaitMillis) {
        stopping = true;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(awaitMillis, TimeUnit.MILLISECONDS)) {
                log.warn("[DISPATCHER] 等待在执行任务超时({}ms)，剩余 RUNNING 任务将在下次启动时恢复",
                        awaitMillis);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    /**
     * 单个分组的串行消费协程。许可顺序固定为：全局并发许可 -> 组锁，
     * 组内同一时刻只有一个任务执行，不同分组各自并行。
     */
    private class GroupRunner {
        private final String group;
        private final Semaphore wakeSignal = new Semaphore(0);

        GroupRunner(String group) {
            this.group = group;
        }

        void wake() {
            wakeSignal.release();
        }

        void run() {
            log.info("[DISPATCHER] 分组协程开始运行 group={}", group);
            JobRecord directClaim = null;
            try {
            while (!stopping) {
                // 先取全局并发许可（真实并发上限）
                if (!acquirePermit()) {
                    return;
                }
                try {
                    synchronized (groupLock(group)) {
                        JobRecord job = directClaim;
                        directClaim = null;
                        if (job == null) {
                            Optional<JobRecord> opt = tx.execute(st ->
                                    repository.acquireNextPending(group));
                            if (opt.isPresent()) {
                                job = opt.get();
                            } else {
                                // 组内暂时没活：退出前与 notifyGroup 做原子对账，
                                // 杜绝“新任务提交后无人唤醒”
                                if (wakeSignal.tryAcquire()) {
                                    continue;
                                }
                                final JobRecord[] rechecked = new JobRecord[1];
                                final boolean[] removed = new boolean[1];
                                runners.compute(group, (g, self) -> {
                                    if (self != GroupRunner.this) {
                                        return self;
                                    }
                                    Optional<JobRecord> again = tx.execute(s ->
                                            repository.acquireNextPending(group));
                                    if (again.isPresent()) {
                                        rechecked[0] = again.get();
                                        return self;
                                    }
                                    removed[0] = true;
                                    return null;
                                });
                                if (removed[0]) {
                                    log.info("[DISPATCHER] 分组协程退出 group={}", group);
                                    return;
                                }
                                job = rechecked[0];
                            }
                        }
                        if (!transition(job, JobStatus.RUNNING, List.of(JobStatus.PENDING))) {
                            // 任务在领取瞬间被取消，跳过
                            continue;
                        }
                        // 持有许可与组锁执行：组内串行，组间并行
                        execute(job);
                    }
                } finally {
                    concurrency.release();
                }
            }
            } catch (Throwable t) {
                log.error("[DISPATCHER] 分组协程异常退出 group={}", group, t);
            } finally {
                log.info("[DISPATCHER] 分组协程终止 group={} stopping={}", group, stopping);
            }
        }
    }
}
