package com.github.lisama569.asyncjobqueuejava.handler;

import com.github.lisama569.asyncjobqueuejava.job.JobHandler;
import com.github.lisama569.asyncjobqueuejava.job.JobRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 内置示例处理器（jobType=echo），用于本地验证：
 * <ul>
 *   <li>payload 以 "always-fail:" 开头：每次执行都抛异常，用于验证重试与死信；</li>
 *   <li>payload 以 "fail-2:" 开头：前 2 次执行失败，第 3 次成功，用于验证重试恢复成功；</li>
 *   <li>payload 以 "sleep:&lt;毫秒&gt;" 开头：睡眠指定时长后成功，便于制造在途任务验证背压；</li>
 *   <li>其他：直接成功。</li>
 * </ul>
 */
@Component
public class EchoJobHandler implements JobHandler {

    private static final Logger log = LoggerFactory.getLogger(EchoJobHandler.class);

    @Override
    public String type() {
        return "echo";
    }

    @Override
    public void handle(JobRecord job) throws InterruptedException {
        String payload = job.getPayload() == null ? "" : job.getPayload();
        int attemptAfterThis = job.getAttempts() + 1;
        if (payload.startsWith("always-fail:")) {
            throw new IllegalStateException("模拟持续失败 payload=" + payload);
        }
        if (payload.startsWith("fail-2:") && attemptAfterThis <= 2) {
            throw new IllegalStateException("模拟前两次失败 attempt=" + attemptAfterThis
                    + " payload=" + payload);
        }
        if (payload.startsWith("sleep:")) {
            long ms = Long.parseLong(payload.substring("sleep:".length()).trim());
            log.info("[ECHO] jobId={} 睡眠 {}ms 模拟慢任务 attempt={}", job.getId(), ms, attemptAfterThis);
            Thread.sleep(ms);
            log.info("[ECHO] jobId={} 慢任务完成 payload={}", job.getId(), payload);
            return;
        }
        log.info("[ECHO] jobId={} 执行成功 payload={} attempts={}",
                job.getId(), payload, attemptAfterThis);
    }
}
