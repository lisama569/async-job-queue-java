package com.github.lisama569.asyncjobqueuejava.tests;

import com.github.lisama569.asyncjobqueuejava.dispatch.JobService;
import com.github.lisama569.asyncjobqueuejava.dispatch.SubmitResult;
import com.github.lisama569.asyncjobqueuejava.job.JobRecord;
import com.github.lisama569.asyncjobqueuejava.job.JobRepository;
import com.github.lisama569.asyncjobqueuejava.job.JobStatus;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 幂等重复提交：同一幂等键无论串行还是并发提交，都只产生一次任务与一次执行。
 */
@SpringBootTest
class IdempotentSubmitTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotentSubmitTest.class);

    @Autowired
    private JobService jobService;
    @Autowired
    private JobRepository repository;
    @Autowired
    private RecordingHandler handler;

    @Test
    void sameIdempotencyKeyExecutedOnce() {
        String key = "idem-serial-" + System.nanoTime();
        SubmitResult first = jobService.submit(
                TestSupport.request(key, "g-idem-serial", "recording", "ok:t1", null));
        SubmitResult second = jobService.submit(
                TestSupport.request(key, "g-idem-serial", "recording", "ok:t1", null));
        SubmitResult third = jobService.submit(
                TestSupport.request(key, "g-idem-serial", "recording", "ok:t1", null));

        assertThat(first.duplicate()).isFalse();
        assertThat(second.duplicate()).isTrue();
        assertThat(third.duplicate()).isTrue();
        assertThat(second.job().getId()).isEqualTo(first.job().getId());
        assertThat(third.job().getId()).isEqualTo(first.job().getId());

        JobRecord done = TestSupport.awaitStatus(repository, first.job().getId(),
                JobStatus.SUCCEEDED);
        assertThat(done.getAttempts()).isEqualTo(1);
        assertThat(handler.invocationsOf("ok:t1")).isEqualTo(1);
        log.info("[ASSERT] 串行重复提交 jobId={} 状态={} 执行次数=1", done.getId(), done.getStatus());
    }

    @Test
    void concurrentSameIdempotencyKeyFoldedToOne() throws Exception {
        String key = "idem-concurrent-" + System.nanoTime();
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> ids = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Boolean> duplicates = new ConcurrentLinkedQueue<>();
        CountDownLatch done = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    SubmitResult r = jobService.submit(
                            TestSupport.request(key, "g-idem-conc", "recording",
                                    "ok:conc", null));
                    ids.add(r.job().getId());
                    duplicates.add(r.duplicate());
                } catch (Exception e) {
                    log.error("并发提交异常", e);
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertThat(done.await(20, TimeUnit.SECONDS)).isTrue();
        pool.shutdownNow();

        Set<String> uniqueIds = new HashSet<>(ids);
        log.info("[ASSERT] 并发提交 threads={} 唯一任务数={} 标记重复数={}",
                threads, uniqueIds.size(), duplicates.stream().filter(Boolean::booleanValue).count());
        assertThat(uniqueIds).hasSize(1);
        assertThat(duplicates).hasSize(threads);

        String onlyId = uniqueIds.iterator().next();
        JobRecord finished = TestSupport.awaitStatus(repository, onlyId, JobStatus.SUCCEEDED);
        assertThat(finished.getAttempts()).isEqualTo(1);
        // 关键断言：真实处理器只被调用一次
        assertThat(handler.invocationsOf("ok:conc")).isEqualTo(1);
    }
}
