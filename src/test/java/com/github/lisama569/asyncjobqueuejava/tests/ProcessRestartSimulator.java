package com.github.lisama569.asyncjobqueuejava.tests;

import com.github.lisama569.asyncjobqueuejava.dispatch.CapacityGauge;
import com.github.lisama569.asyncjobqueuejava.dispatch.DefaultJobDispatcher;
import com.github.lisama569.asyncjobqueuejava.dispatch.JobDispatcher;
import com.github.lisama569.asyncjobqueuejava.dispatch.JobQueueProperties;
import com.github.lisama569.asyncjobqueuejava.dispatch.JobService;
import com.github.lisama569.asyncjobqueuejava.job.JobHandlerRegistry;
import com.github.lisama569.asyncjobqueuejava.job.JobRecord;
import com.github.lisama569.asyncjobqueuejava.job.JobRepository;
import com.github.lisama569.asyncjobqueuejava.job.JobRequest;
import com.github.lisama569.asyncjobqueuejava.job.JobStatus;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 模拟“进程重启”：
 * 1) 关闭旧调度器（等价 JVM 退出，内存协程全部消失）；
 * 2) 新建一套调度器（等价新进程）；
 * 3) 执行与 StartupRecovery 等价的恢复：RUNNING 视为中断 -> PENDING 重投，
 *    PENDING 保留并唤醒分组。
 */
@Component
public class ProcessRestartSimulator {

    private static final Logger log = LoggerFactory.getLogger(ProcessRestartSimulator.class);

    private final JobRepository repository;
    private final JobHandlerRegistry handlers;
    private final JobQueueProperties properties;
    private final javax.sql.DataSource dataSource;

    private DefaultJobDispatcher dispatcher;
    private JobService serviceForCurrentProcess;

    public ProcessRestartSimulator(JobRepository repository, JobHandlerRegistry handlers,
                                   JobQueueProperties properties,
                                   javax.sql.DataSource dataSource) {
        this.repository = repository;
        this.handlers = handlers;
        this.properties = properties;
        this.dataSource = dataSource;
        boot();
    }

    private DefaultJobDispatcher newDispatcher(CapacityGauge gauge) {
        TransactionTemplate tx = new TransactionTemplate(
                new DataSourceTransactionManager(dataSource));
        // 恢复测试用 1 个并发槽，便于稳定制造 RUNNING + 排队 PENDING
        JobQueueProperties single = new JobQueueProperties();
        single.setMaxConcurrency(1);
        single.setQueueCapacity(properties.getQueueCapacity());
        single.setDefaultMaxAttempts(properties.getDefaultMaxAttempts());
        single.setBackoffInitialMillis(properties.getBackoffInitialMillis());
        single.setBackoffMultiplier(properties.getBackoffMultiplier());
        single.setBackoffMaxMillis(properties.getBackoffMaxMillis());
        single.setRetryTickMillis(properties.getRetryTickMillis());
        return new DefaultJobDispatcher(repository, handlers, single, gauge, tx);
    }

    /** 用“当前进程”的服务提交任务（保证 notifyGroup 进到本模拟器的调度器）。 */
    public synchronized JobRecord submitForRestartTest(JobService sharedService, JobRequest r) {
        if (serviceForCurrentProcess == null) {
            boot();
        }
        return serviceForCurrentProcess.submit(r).job();
    }

    public synchronized void boot() {
        CapacityGauge gauge = new CapacityGauge(properties);
        TransactionTemplate tx = new TransactionTemplate(
                new DataSourceTransactionManager(dataSource));
        this.dispatcher = newDispatcher(gauge);
        this.serviceForCurrentProcess =
                new com.github.lisama569.asyncjobqueuejava.dispatch.DefaultJobService(
                        repository, properties, gauge, dispatcher);
        log.info("[RESTART-SIM] 新进程已启动(独立调度器 maxConcurrency=1)");
    }

    /** 杀掉旧进程并按启动恢复流程引导新进程。 */
    public synchronized void killAndRecover(RecordingHandler handler) {
        log.info("[RESTART-SIM] === 模拟进程崩溃：关闭旧调度器（内存协程全部消失）===");
        dispatcher.shutdown(500);
        dispatcher = null;
        serviceForCurrentProcess = null;

        // 放行旧闸门，让旧协程尽快结束（模拟收到 kill 后任务体自然退出）
        handler.releaseBlocked();
        boot();

        // === 与 StartupRecovery.recover() 等价的恢复逻辑 ===
        List<JobRecord> unfinished = repository.findUnfinished();
        // 容量闸门是进程内状态，新进程按持久层未完成数重新播种
        // （gauge 已在 boot 中构造，这里模拟 seed）
        Set<String> groups = new HashSet<>();
        int interrupted = 0;
        int pending = 0;
        for (JobRecord job : unfinished) {
            if (job.getStatus() == JobStatus.RUNNING) {
                job.setStatus(JobStatus.PENDING);
                job.setFailureReason("进程重启恢复：上次执行被中断，重新排队(attempts="
                        + job.getAttempts() + ")");
                if (repository.compareAndUpdate(job, List.of(JobStatus.RUNNING))) {
                    interrupted++;
                    groups.add(job.getGroupKey());
                    log.warn("[RESTART-SIM] jobId={} RUNNING(中断) -> PENDING 重投(at-least-once)",
                            job.getId());
                }
            } else if (job.getStatus() == JobStatus.PENDING) {
                pending++;
                groups.add(job.getGroupKey());
            }
        }
        for (String g : groups) {
            dispatcher.notifyGroup(g);
        }
        log.info("[RESTART-SIM] 恢复完成 pending={} interrupted->pending={} 唤醒分组={}",
                pending, interrupted, groups.size());
    }
}
