package com.github.lisama569.asyncjobqueuejava.tests;

import com.github.lisama569.asyncjobqueuejava.dispatch.JobService;
import com.github.lisama569.asyncjobqueuejava.job.JobRecord;
import com.github.lisama569.asyncjobqueuejava.job.JobRepository;
import com.github.lisama569.asyncjobqueuejava.job.JobStatus;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 取消与并发状态自洽：
 * - PENDING 任务可取消并进入 CANCELLED（终态，永不执行）；
 * - RUNNING 任务不抢占，取消返回当前状态，结果由执行决定（不出现状态回退）；
 * - 提交/查询/取消高度重叠时不丢更新、不卡死、不回退。
 */
@SpringBootTest
class CancelAndConsistencyTest {

    private static final Logger log = LoggerFactory.getLogger(CancelAndConsistencyTest.class);

    @Autowired
    private JobService jobService;
    @Autowired
    private JobRepository repository;
    @Autowired
    private RecordingHandler handler;

    @BeforeEach
    void setUp() {
        repository.deleteAll();
        handler.reset();
    }

    @AfterEach
    void tearDown() {
        handler.releaseBlocked();
        for (JobRecord j : repository.findUnfinished()) {
            TestSupport.awaitThat(repository, j.getId(),
                    x -> x.getStatus() == JobStatus.SUCCEEDED
                            || x.getStatus() == JobStatus.CANCELLED
                            || x.getStatus() == JobStatus.DEAD);
        }
        repository.deleteAll();
        handler.reset();
    }

    @Test
    void pendingJobCanBeCancelledAndNeverExecutes() {
        // 占满并发：先提交不同分组的两个 block 任务（2 RUNNING），再提交一个排队任务
        String blockerA = "block:cancel-a-" + UUID.randomUUID();
        String blockerB = "block:cancel-b-" + UUID.randomUUID();
        jobService.submit(TestSupport.request(null, "g-cancel-a", "recording", blockerA, 1));
        jobService.submit(TestSupport.request(null, "g-cancel-b", "recording", blockerB, 1));
        awaitRunningCount(2);

        String queuedPayload = "ok:queued-" + UUID.randomUUID();
        JobRecord queued = jobService.submit(
                TestSupport.request(null, "g-cancel-c", "recording", queuedPayload, 1)).job();
        // 确认它确实在排队
        awaitStatus(queued.getId(), JobStatus.PENDING);

        var cancelled = jobService.cancel(queued.getId());
        assertThat(cancelled).isPresent();
        assertThat(cancelled.get().changed()).isTrue();
        assertThat(cancelled.get().job().getStatus()).isEqualTo(JobStatus.CANCELLED);
        assertThat(cancelled.get().job().getFinishedAt()).isNotNull();
        assertThat(cancelled.get().job().getFailureReason()).contains("用户取消");

        // 放行阻塞任务后，被取消的任务绝不能被执行
        handler.releaseBlocked();
        // 给系统一点时间确认“不会执行”
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertThat(handler.invocationsOf(queuedPayload)).isZero();
        JobRecord finalState = repository.findById(queued.getId()).orElseThrow();
        assertThat(finalState.getStatus()).isEqualTo(JobStatus.CANCELLED);
        log.info("[ASSERT] 排队任务 jobId={} 已取消且从未执行 终态=CANCELLED", queued.getId());
    }

    @Test
    void runningJobCannotBeCancelledBackwards() {
        String payload = "block:running-" + UUID.randomUUID();
        JobRecord job = jobService.submit(
                TestSupport.request(null, "g-cancel-run", "recording", payload, 1)).job();
        awaitStatus(job.getId(), JobStatus.RUNNING);

        var afterCancel = jobService.cancel(job.getId());
        assertThat(afterCancel).isPresent();
        assertThat(afterCancel.get().changed()).isFalse();
        // RUNNING 不允许被抢占取消，状态保持 RUNNING
        assertThat(afterCancel.get().job().getStatus()).isEqualTo(JobStatus.RUNNING);

        handler.releaseBlocked();
        JobRecord done = TestSupport.awaitStatus(repository, job.getId(),
                JobStatus.SUCCEEDED);
        assertThat(done.getStatus()).isEqualTo(JobStatus.SUCCEEDED);
        // 状态没有从 SUCCEEDED 回退
        assertThat(repository.findById(job.getId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.SUCCEEDED);
        log.info("[ASSERT] RUNNING 任务取消被忽略，正常执行至终态 SUCCEEDED，无状态回退");
    }

    @Test
    void concurrentCancelAndQueryStaysConsistent() throws Exception {
        // 占满并发，制造一批稳定 PENDING 的任务，然后多线程并发取消/查询
        String b1 = "block:cc-1-" + UUID.randomUUID();
        String b2 = "block:cc-2-" + UUID.randomUUID();
        jobService.submit(TestSupport.request(null, "g-cc-1", "recording", b1, 1));
        jobService.submit(TestSupport.request(null, "g-cc-2", "recording", b2, 1));
        awaitRunningCount(2);

        int queuedCount = 1; // 容量 3，只能再排 1 个
        JobRecord queued = jobService.submit(
                TestSupport.request(null, "g-cc-q", "recording",
                        "ok:ccq-" + UUID.randomUUID(), 1)).job();

        int threads = 12;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger cancelOk = new AtomicInteger();
        AtomicInteger cancelIgnored = new AtomicInteger();
        AtomicInteger queries = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            final boolean canceller = i % 2 == 0;
            pool.submit(() -> {
                try {
                    start.await();
                    JobRecord r;
                    if (canceller) {
                        var cr = jobService.cancel(queued.getId()).orElseThrow();
                        r = cr.job();
                        // changed 仅对真正完成 CAS 迁移的那一次为 true
                        if (cr.changed()) {
                            cancelOk.incrementAndGet();
                        } else {
                            cancelIgnored.incrementAndGet();
                        }
                    } else {
                        r = jobService.get(queued.getId()).orElseThrow();
                        queries.incrementAndGet();
                    }
                    // 状态只能是 PENDING 或 CANCELLED，绝不允许出现非法中间态/回退
                    assertThat(r.getStatus()).isIn(JobStatus.PENDING, JobStatus.CANCELLED);
                } catch (Exception e) {
                    log.error("并发取消/查询异常", e);
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertThat(done.await(15, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();

        // CAS + version 保证 PENDING->CANCELLED 迁移恰好发生一次
        assertThat(cancelOk.get())
                .as("仅第一个获胜者把 version 从 0 改为 1")
                .isEqualTo(1);
        assertThat(queries.get() + cancelOk.get() + cancelIgnored.get()).isEqualTo(threads);
        JobRecord terminal = repository.findById(queued.getId()).orElseThrow();
        assertThat(terminal.getStatus()).isEqualTo(JobStatus.CANCELLED);
        assertThat(terminal.getVersion()).isEqualTo(1);

        handler.releaseBlocked();
        log.info("[ASSERT] 并发取消/查询 threads={} 状态迁移恰好1次 查询={} 终态=CANCELLED version=1 无丢失更新",
                threads, queries.get());
    }

    private void awaitRunningCount(int expected) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            long running = repository.findUnfinished().stream()
                    .filter(j -> j.getStatus() == JobStatus.RUNNING).count();
            if (running >= expected) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        throw new AssertionError("未观察到 " + expected + " 个 RUNNING");
    }

    private void awaitStatus(String id, JobStatus status) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            JobRecord j = repository.findById(id).orElse(null);
            if (j != null && j.getStatus() == status) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        throw new AssertionError("任务 " + id + " 未进入 " + status);
    }
}
