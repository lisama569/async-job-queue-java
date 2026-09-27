package com.github.lisama569.asyncjobqueuejava.tests;

import com.github.lisama569.asyncjobqueuejava.dispatch.JobService;
import com.github.lisama569.asyncjobqueuejava.job.JobRecord;
import com.github.lisama569.asyncjobqueuejava.job.JobRepository;
import com.github.lisama569.asyncjobqueuejava.job.JobStatus;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 失败重试与死信：
 * - 有限次数、递增间隔自动重试；
 * - 耗尽后进入 DEAD，并保留可区分原因的失败信息；
 * - flaky 任务在重试若干次后成功。
 */
@SpringBootTest
class RetryAndDeadLetterTest {

    private static final Logger log = LoggerFactory.getLogger(RetryAndDeadLetterTest.class);

    @Autowired
    private JobService jobService;
    @Autowired
    private JobRepository repository;
    @Autowired
    private RecordingHandler handler;

    @Test
    void alwaysFailingJobRetriesThenGoesDead() {
        String payload = "fail:dead-" + System.nanoTime();
        JobRecord job = jobService.submit(
                TestSupport.request("idem-dead-" + System.nanoTime(), "g-dead",
                        "recording", payload, 4)).job();

        JobRecord dead = TestSupport.awaitStatus(repository, job.getId(), JobStatus.DEAD);

        // 执行次数 = maxAttempts：初始 1 次 + 3 次重试
        assertThat(dead.getAttempts()).isEqualTo(4);
        assertThat(handler.invocationsOf(payload)).isEqualTo(4);
        // 失败信息可区分“重试耗尽”以及最后一次异常原因
        assertThat(dead.getFailureReason())
                .contains("重试耗尽(4/4)")
                .contains("IllegalStateException")
                .contains(payload);
        assertThat(dead.getLastError()).contains("模拟永久失败");
        assertThat(dead.getFinishedAt()).isNotNull();

        // 死信列表能查到
        assertThat(jobService.listDead(50)).extracting(JobRecord::getId)
                .contains(dead.getId());

        log.info("[ASSERT] 死信任务 jobId={} 最终状态=DEAD 执行次数=4 reason={}",
                dead.getId(), dead.getFailureReason());
    }

    @Test
    void flakyJobSucceedsAfterRetries() {
        String payload = "flaky:2:ok-" + System.nanoTime();
        JobRecord job = jobService.submit(
                TestSupport.request("idem-flaky-" + System.nanoTime(), "g-flaky",
                        "recording", payload, 5)).job();

        JobRecord done = TestSupport.awaitStatus(repository, job.getId(),
                JobStatus.SUCCEEDED);
        assertThat(done.getAttempts()).isEqualTo(3);
        assertThat(handler.invocationsOf(payload)).isEqualTo(3);
        log.info("[ASSERT] 瞬时失败任务 jobId={} 最终状态=SUCCEEDED 执行次数=3",
                done.getId());
    }

    @Test
    void unknownJobTypeGoesDeadImmediately() {
        JobRecord job = jobService.submit(
                TestSupport.request("idem-unknown-" + System.nanoTime(), "g-unknown",
                        "no-such-type", "x", 5)).job();

        JobRecord dead = TestSupport.awaitStatus(repository, job.getId(), JobStatus.DEAD);
        assertThat(dead.getAttempts()).isEqualTo(1);
        assertThat(dead.getFailureReason()).contains("不可重试")
                .contains("未注册的 jobType");
        log.info("[ASSERT] 未知类型任务 jobId={} 直接进入死信 reason={}",
                dead.getId(), dead.getFailureReason());
    }
}
