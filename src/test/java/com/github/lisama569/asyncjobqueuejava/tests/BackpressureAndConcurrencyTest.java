package com.github.lisama569.asyncjobqueuejava.tests;

import com.github.lisama569.asyncjobqueuejava.dispatch.JobService;
import com.github.lisama569.asyncjobqueuejava.job.JobHandler;
import com.github.lisama569.asyncjobqueuejava.job.JobHandlerRegistry;
import com.github.lisama569.asyncjobqueuejava.dispatch.QueueFullException;
import com.github.lisama569.asyncjobqueuejava.job.JobRecord;
import com.github.lisama569.asyncjobqueuejava.job.JobRepository;
import com.github.lisama569.asyncjobqueuejava.job.JobStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 并发与背压：
 * - 容量真实生效：占满后提交得到明确的 QueueFullException（HTTP 503），不静默丢弃；
 * - 任务完成后名额归还，系统恢复可提交；
 * - 全局并发上限真实生效：同时在执行的任务数不超过 maxConcurrency。
 */
@SpringBootTest
class BackpressureAndConcurrencyTest {

    private static final Logger log =
            LoggerFactory.getLogger(BackpressureAndConcurrencyTest.class);

    /** 测试配置：maxConcurrency=2, queueCapacity=3 -> 总在途容量 3 */
    private static final int CAPACITY = 3;
    private static final int MAX_CONCURRENCY = 2;

    @Autowired
    private JobService jobService;
    @Autowired
    private JobRepository repository;
    @Autowired
    private RecordingHandler handler;
    @Autowired
    private JobHandlerRegistry registry;

    @BeforeEach
    void setUp() {
        repository.deleteAll();
        handler.reset();
    }

    @AfterEach
    void tearDown() {
        handler.releaseBlocked();
        // 等所有任务终结，避免泄漏到其它测试类共享上下文
        for (JobRecord j : repository.findUnfinished()) {
            TestSupport.awaitThat(repository, j.getId(),
                    x -> x.getStatus() == JobStatus.SUCCEEDED
                            || x.getStatus() == JobStatus.CANCELLED
                            || x.getStatus() == JobStatus.DEAD);
        }
        repository.deleteAll();
        handler.reset();
    }

    private String blockPayload(String tag) {
        return "block:" + tag + "-" + UUID.randomUUID();
    }

    @Test
    void queueFullGivesExplicitBackpressureAndRecoversAfterDrain() throws Exception {
        // 占满 3 个在途名额：前两个分到不同分组 -> 2 个 RUNNING（打满并发）；
        // 第三个无许可可用 -> PENDING 排队。
        List<String> payloads = new ArrayList<>();
        for (int i = 0; i < CAPACITY; i++) {
            String payload = blockPayload("fill-" + i);
            payloads.add(payload);
            jobService.submit(TestSupport.request(null, "g-bp-" + i, "recording", payload, 1));
        }
        // 等前两个进入 RUNNING（并发上限真实生效，第三个只能 PENDING）
        awaitRunningCount(2, 10_000);

        // 容量已满：明确背压，而不是静默排队或丢弃
        String overflow = blockPayload("overflow");
        assertThatThrownBy(() -> jobService.submit(
                TestSupport.request(null, "g-bp-overflow", "recording", overflow, 1)))
                .isInstanceOf(QueueFullException.class)
                .hasMessageContaining("队列已满");
        log.info("[ASSERT] 容量打满 inFlight={} 新提交被明确拒绝(QUEUE_FULL) 而非静默丢弃",
                CAPACITY);

        // 溢出任务确实没有被偷偷接收
        assertThat(repository.findUnfinished())
                .as("被背压拒绝的任务不能出现在队列里")
                .allMatch(j -> !overflow.equals(j.getPayload()));

        // 放行，已接受的任务全部完成
        handler.releaseBlocked();
        for (String p : payloads) {
            awaitPayloadFinished(p);
        }

        // 名额归还后系统恢复可提交
        String after = blockPayload("after-drain");
        JobRecord accepted = jobService.submit(
                TestSupport.request(null, "g-bp2", "recording", after, 1)).job();
        handler.releaseBlocked();
        JobRecord done = TestSupport.awaitStatus(repository, accepted.getId(),
                JobStatus.SUCCEEDED);
        assertThat(done.getStatus()).isEqualTo(JobStatus.SUCCEEDED);
        log.info("[ASSERT] 队列排空后新任务正常提交并完成 jobId={}", done.getId());
    }

    @Test
    void concurrencyLimitIsActuallyEnforced() throws Exception {
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        CountDownLatch canFinish = new CountDownLatch(1);

        // 提交多于并发上限的任务，统计同时执行的峰值
        int total = MAX_CONCURRENCY * 3;
        String type = "concurrency-probe-" + UUID.randomUUID();
        registry.register(new JobHandler() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public void handle(JobRecord job) {
                int now = active.incrementAndGet();
                maxObserved.accumulateAndGet(now, Math::max);
                log.info("[PROBE] jobId={} active={} 进入执行", job.getId(), now);
                try {
                    // 已超过容量的提交会被背压；这里先放行 recording 不影响本类型
                    if (!canFinish.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("probe 等待超时");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    active.decrementAndGet();
                }
            }
        });

        // 容量只有 3：先占满(2 运行+1 排队)，再发更多只能背压
        int submitted = 0;
        List<String> acceptedIds = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            try {
                JobRecord j = jobService.submit(TestSupport.request(null, "g-probe-" + i,
                        type, "p" + i, 1)).job();
                acceptedIds.add(j.getId());
                submitted++;
            } catch (QueueFullException expected) {
                // 背压
            }
        }
        assertThat(submitted).isLessThanOrEqualTo(CAPACITY);

        // 等全部被接受的任务都进入过执行后再放行
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline
                && maxObserved.get() < Math.min(submitted, MAX_CONCURRENCY)) {
            Thread.sleep(10);
        }
        canFinish.countDown();

        for (String id : acceptedIds) {
            TestSupport.awaitStatus(repository, id, JobStatus.SUCCEEDED);
        }
        log.info("[ASSERT] 并发上限验证 提交={} 接受={} 观测峰值并发={} 上限={}",
                total, submitted, maxObserved.get(), MAX_CONCURRENCY);
        assertThat(maxObserved.get())
                .as("同时在执行的任务数不得超过 maxConcurrency")
                .isLessThanOrEqualTo(MAX_CONCURRENCY);
    }

    @Test
    void concurrentSubmitsNeverExceedCapacity() throws Exception {
        int threads = 24;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch finishAll = new CountDownLatch(threads);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            final int idx = i;
            pool.submit(() -> {
                try {
                    start.await();
                    try {
                        jobService.submit(TestSupport.request(null, "g-race",
                                "recording", blockPayload("race-" + idx), 1));
                        accepted.incrementAndGet();
                    } catch (QueueFullException e) {
                        rejected.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishAll.countDown();
                }
            });
        }
        start.countDown();
        assertThat(finishAll.await(15, TimeUnit.SECONDS)).isTrue();

        log.info("[ASSERT] 并发{}提交: 接受={} 背压拒绝={} 容量={}",
                threads, accepted.get(), rejected.get(), CAPACITY);
        assertThat(accepted.get())
                .as("接受数绝不能超过容量")
                .isLessThanOrEqualTo(CAPACITY);
        assertThat(accepted.get() + rejected.get()).isEqualTo(threads);

        handler.releaseBlocked();
    }

    private void awaitRunningCount(int expected, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            long running = repository.findUnfinished().stream()
                    .filter(j -> j.getStatus() == JobStatus.RUNNING).count();
            if (running >= expected) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("未在超时内观察到 " + expected + " 个 RUNNING");
    }

    private void awaitPayloadFinished(String payload) {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(15));
        while (Instant.now().isBefore(deadline)) {
            boolean finished = repository.findUnfinished().stream()
                    .noneMatch(j -> payload.equals(j.getPayload()));
            if (finished) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        throw new AssertionError("payload=" + payload + " 未在超时内终结");
    }
}
