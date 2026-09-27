package com.github.lisama569.asyncjobqueuejava.tests;

import com.github.lisama569.asyncjobqueuejava.job.JobHandler;
import com.github.lisama569.asyncjobqueuejava.job.JobRecord;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 测试专用处理器：按 payload 约定行为，并统计每个任务的真实执行次数。
 * payload 约定：
 * <pre>
 *   ok:<key>            立即成功
 *   block:<key>         阻塞在当前闸门上，直到测试放行（重启后用新闸门模拟“新进程继续执行”）
 *   fail:<key>          永远失败
 *   flaky:<n>:<key>     前 n 次失败，之后成功
 * </pre>
 */
@Component
public class RecordingHandler implements JobHandler {

    private static final Logger log = LoggerFactory.getLogger(RecordingHandler.class);

    private final Map<String, AtomicInteger> invocations = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> failCounts = new ConcurrentHashMap<>();
    private volatile CountDownLatch gate = new CountDownLatch(1);

    @Override
    public String type() {
        return "recording";
    }

    public int invocationsOf(String key) {
        return invocations.getOrDefault(key, new AtomicInteger()).get();
    }

    /** 让所有 block 任务继续执行 */
    public void releaseBlocked() {
        gate.countDown();
    }

    /** 模拟“新进程”：换新闸门（旧进程 block 的等待随协程销毁而消失） */
    public void newGateForRestart() {
        releaseBlocked();
        gate = new CountDownLatch(1);
    }

    public void reset() {
        invocations.clear();
        failCounts.clear();
        releaseBlocked();
        gate = new CountDownLatch(1);
    }

    @Override
    public void handle(JobRecord job) {
        String p = job.getPayload() == null ? "" : job.getPayload();
        String[] parts = p.split(":", 3);
        String mode = parts.length > 0 ? parts[0] : "ok";
        int attempt = invocations.computeIfAbsent(p, k -> new AtomicInteger())
                .incrementAndGet();
        log.info("[TEST-HANDLER] jobId={} payload={} attempt={}", job.getId(), p, attempt);

        switch (mode) {
            case "block" -> {
                log.info("[TEST-HANDLER] jobId={} payload={} 阻塞等待放行...", job.getId(), p);
                try {
                    if (!gate.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("block 等待超时 payload=" + p);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("block 被中断", e);
                }
                log.info("[TEST-HANDLER] jobId={} payload={} 放行后执行成功 attempt={}",
                        job.getId(), p, attempt);
            }
            case "fail" -> {
                log.warn("[TEST-HANDLER] jobId={} payload={} attempt={} 模拟失败(将触发重试)",
                        job.getId(), p, attempt);
                throw new IllegalStateException("模拟永久失败 payload=" + p
                        + " attempt=" + attempt);
            }
            case "flaky" -> {
                int need = parts.length > 1 ? Integer.parseInt(parts[1]) : 1;
                int failed = failCounts.computeIfAbsent(p, k -> new AtomicInteger())
                        .incrementAndGet();
                if (failed <= need) {
                    log.warn("[TEST-HANDLER] jobId={} payload={} attempt={} 第{}次失败(共需失败{}次)",
                            job.getId(), p, attempt, failed, need);
                    throw new IllegalStateException("模拟瞬时失败 payload=" + p
                            + " failedTimes=" + failed);
                }
                log.info("[TEST-HANDLER] jobId={} payload={} 第{}次尝试成功",
                        job.getId(), p, attempt);
            }
            default -> log.info("[TEST-HANDLER] jobId={} payload={} 立即成功 attempt={}",
                    job.getId(), p, attempt);
        }
    }
}
