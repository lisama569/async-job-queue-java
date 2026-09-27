package com.github.lisama569.asyncjobqueuejava.tests;

import com.github.lisama569.asyncjobqueuejava.dispatch.JobService;
import com.github.lisama569.asyncjobqueuejava.job.JobRecord;
import com.github.lisama569.asyncjobqueuejava.job.JobRepository;
import com.github.lisama569.asyncjobqueuejava.job.JobRequest;
import com.github.lisama569.asyncjobqueuejava.job.JobStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 重启恢复：
 * - 排队中的 PENDING 任务在“进程重启”后不丢失，恢复执行；
 * - 执行中被中断的 RUNNING 任务重启后重投（at-least-once，处理器幂等保证不产生重复副作用）。
 */
@SpringBootTest(classes = RestartTestApplication.class)
class RestartRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryTest.class);

    @Autowired
    private JobService jobService;
    @Autowired
    private JobRepository repository;
    @Autowired
    private ProcessRestartSimulator simulator;
    @Autowired
    private RecordingHandler handler;

    @BeforeEach
    void cleanupBefore() {
        repository.deleteAll();
        handler.reset();
    }

    @AfterEach
    void cleanupAfter() {
        repository.deleteAll();
        handler.reset();
    }

    private JobRequest req(String group, String payload) {
        return TestSupport.request(null, group, "recording", payload, 1);
    }

    @Test
    void pendingJobsSurviveRestart() {
        String g = "g-restart-pending";
        String p0 = "block:r0-" + System.nanoTime();
        String p1 = "ok:r1-" + System.nanoTime();
        String p2 = "ok:r2-" + System.nanoTime();

        // 第一个任务占用唯一的(模拟进程内)执行槽并阻塞，后两个处于 PENDING
        JobRecord first = simulator.submitForRestartTest(jobService, req(g, p0));
        awaitStatus(first.getId(), JobStatus.RUNNING);
        JobRecord j1 = simulator.submitForRestartTest(jobService, req(g, p1));
        JobRecord j2 = simulator.submitForRestartTest(jobService, req(g, p2));
        awaitStatus(j1.getId(), null); // 确认已落库 PENDING
        awaitStatus(j2.getId(), null);

        // 旧进程死亡：内存协程消失，PENDING 仍在库中
        simulator.killAndRecover(handler);
        // 新进程的阻塞闸门放行，重投的第一个任务即可成功完成
        handler.releaseBlocked();

        // 重启后第一个中断任务被重投（新进程不再阻塞），排队任务依次恢复
        JobRecord f0 = TestSupport.awaitStatus(repository, first.getId(), JobStatus.SUCCEEDED);
        JobRecord f1 = TestSupport.awaitStatus(repository, j1.getId(), JobStatus.SUCCEEDED);
        JobRecord f2 = TestSupport.awaitStatus(repository, j2.getId(), JobStatus.SUCCEEDED);

        // 被中断的任务至少执行一次（可能旧进程已执行过副作用），排队任务恰好一次
        assertThat(handler.invocationsOf(p0)).isGreaterThanOrEqualTo(1);
        assertThat(handler.invocationsOf(p1)).isEqualTo(1);
        assertThat(handler.invocationsOf(p2)).isEqualTo(1);
        log.info("[ASSERT] 重启后 PENDING 不丢失并恢复成功 ids=[{},{},{}]",
                f0.getId(), f1.getId(), f2.getId());
    }

    @Test
    void interruptedRunningJobIsRedeliveredAfterRestart() {
        String g = "g-restart-running";
        String p = "block:redeliver-" + System.nanoTime();
        JobRecord job = simulator.submitForRestartTest(jobService, req(g, p));
        awaitStatus(job.getId(), JobStatus.RUNNING);

        // 在任务执行中“杀掉进程”（DB 里残留 RUNNING），随后按启动流程恢复
        simulator.killAndRecover(handler);
        handler.releaseBlocked();

        JobRecord done = TestSupport.awaitStatus(repository, job.getId(), JobStatus.SUCCEEDED);
        assertThat(handler.invocationsOf(p)).isGreaterThanOrEqualTo(1);
        assertThat(done.getStatus()).isEqualTo(JobStatus.SUCCEEDED);
        log.info("[ASSERT] 中断任务 jobId={} 重启后重投完成 总执行次数={} (at-least-once)",
                done.getId(), handler.invocationsOf(p));
    }

    private void awaitStatus(String id, JobStatus status) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            JobRecord j = repository.findById(id).orElse(null);
            if (status == null) {
                if (j != null) {
                    return;
                }
            } else if (j != null && j.getStatus() == status) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        throw new AssertionError("任务 " + id + " 未在超时前进入 " + status);
    }
}
