package com.github.lisama569.asyncjobqueuejava.tests;

import com.github.lisama569.asyncjobqueuejava.dispatch.JobService;
import com.github.lisama569.asyncjobqueuejava.job.JobHandler;
import com.github.lisama569.asyncjobqueuejava.job.JobHandlerRegistry;
import com.github.lisama569.asyncjobqueuejava.job.JobRecord;
import com.github.lisama569.asyncjobqueuejava.job.JobRepository;
import com.github.lisama569.asyncjobqueuejava.job.JobRequest;
import com.github.lisama569.asyncjobqueuejava.job.JobStatus;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
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
 * 分组语义：
 * - 同一分组内严格按提交顺序串行执行（不重叠）；
 * - 不同分组之间可以并行（出现时间重叠）。
 */
@SpringBootTest
class GroupOrderingTest {

    private static final Logger log = LoggerFactory.getLogger(GroupOrderingTest.class);

    @Autowired
    private JobService jobService;
    @Autowired
    private JobRepository repository;
    @Autowired
    private JobHandlerRegistry registry;

    private String probeType;

    private JobRequest req(String group, String payload) {
        return TestSupport.request(null, group, probeType, payload, 1);
    }

    @BeforeEach
    void setUp() {
        repository.deleteAll();
    }

    @AfterEach
    void tearDown() {
        repository.deleteAll();
    }

    /** 注册一个记录进入/离开顺序与时间重叠的探针处理器 */
    private Probe registerProbe() {
        probeType = "order-probe-" + UUID.randomUUID();
        Probe probe = new Probe(probeType);
        registry.register(probe);
        return probe;
    }

    @Test
    void sameGroupRunsSeriallyInSubmitOrder() {
        Probe probe = registerProbe();
        // 用不阻塞、但执行很快的探针：在容量(3)内分两批提交，
        // 每批等完成后再提交下一批，验证整组的严格串行与提交顺序。
        probe.setNoBlock(true);
        String g = "g-serial";
        int n = 6;
        String[] payloads = new String[n];
        int submitted = 0;
        // 第一批 3 个（打满容量）
        for (int i = 0; i < 3; i++) {
            payloads[i] = "seq-" + i;
            jobService.submit(req(g, payloads[i]));
            submitted++;
        }
        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(10))
                .until(() -> repository.findUnfinished().isEmpty());
        // 第二批
        for (int i = 3; i < n; i++) {
            payloads[i] = "seq-" + i;
            jobService.submit(req(g, payloads[i]));
            submitted++;
        }
        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(10))
                .until(() -> repository.findByStatus(JobStatus.SUCCEEDED, 100).size() >= n);

        assertThat(submitted).isEqualTo(n);
        // 同组内任意时刻不重叠
        assertThat(probe.maxActive()).isEqualTo(1);
        // 执行顺序严格等于提交顺序
        assertThat(probe.order()).containsExactly(payloads);
        log.info("[ASSERT] 同组串行顺序: {}", probe.order());
    }

    @Test
    void differentGroupsRunInParallel() throws Exception {
        Probe probe = registerProbe();
        CountDownLatch entered = new CountDownLatch(2);
        probe.onEnter = entered::countDown;

        jobService.submit(req("g-parallel-A", "a"));
        jobService.submit(req("g-parallel-B", "b"));

        // 两个不同分组的任务必须同时处于执行中（重叠）
        assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(probe.maxActive())
                .as("不同分组应能并行，观测峰值并发 >= 2")
                .isGreaterThanOrEqualTo(2);
        probe.release();

        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(15))
                .until(() -> repository.findByStatus(JobStatus.SUCCEEDED, 100).size() >= 2);
        assertThat(probe.order()).containsExactlyInAnyOrder("a", "b");
        log.info("[ASSERT] 不同分组并行执行 峰值并发={}", probe.maxActive());
    }

    /**
     * 探针处理器：统计当前在执行数、峰值并发、进入顺序；
     * 默认所有任务阻塞在一个共享闸门上，便于制造重叠，release 后依次快速完成。
     */
    private static class Probe implements JobHandler {
        private final String type;
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger maxActive = new AtomicInteger();
        private final CopyOnWriteArrayList<String> order = new CopyOnWriteArrayList<>();
        private final CountDownLatch gate = new CountDownLatch(1);
        private volatile boolean noBlock;
        volatile Runnable onEnter;

        void setNoBlock(boolean v) {
            this.noBlock = v;
        }

        Probe(String type) {
            this.type = type;
        }

        @Override
        public String type() {
            return type;
        }

        void release() {
            gate.countDown();
        }

        int maxActive() {
            return maxActive.get();
        }

        List<String> order() {
            return List.copyOf(order);
        }

        @Override
        public void handle(JobRecord job) {
            int now = active.incrementAndGet();
            maxActive.accumulateAndGet(now, Math::max);
            order.add(job.getPayload());
            log.info("[ORDER-PROBE] payload={} active={}", job.getPayload(), now);
            if (onEnter != null) {
                onEnter.run();
            }
            try {
                if (noBlock) {
                    // 微小停顿，放大同组重叠检测窗口
                    Thread.sleep(5);
                } else {
                    gate.await(10, TimeUnit.SECONDS);
                    Thread.sleep(5);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                active.decrementAndGet();
            }
        }
    }
}
