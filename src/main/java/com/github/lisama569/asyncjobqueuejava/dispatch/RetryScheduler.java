package com.github.lisama569.asyncjobqueuejava.dispatch;

import com.github.lisama569.asyncjobqueuejava.job.JobRecord;
import com.github.lisama569.asyncjobqueuejava.job.JobRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 定时扫描到点的 RETRY_WAITING 任务并重新调度。
 * 扫描间隔即重试调度的最大额外延迟。
 */
@Component
public class RetryScheduler {

    private static final Logger log = LoggerFactory.getLogger(RetryScheduler.class);

    private final JobRepository repository;
    private final JobDispatcher dispatcher;
    private final JobQueueProperties properties;
    private ScheduledExecutorService timer;

    public RetryScheduler(JobRepository repository, JobDispatcher dispatcher,
                          JobQueueProperties properties) {
        this.repository = repository;
        this.dispatcher = dispatcher;
        this.properties = properties;
    }

    @PostConstruct
    public void start() {
        timer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "retry-scheduler");
            t.setDaemon(true);
            return t;
        });
        long tick = Math.max(10, properties.getRetryTickMillis());
        timer.scheduleWithFixedDelay(this::tickSafely, tick, tick, TimeUnit.MILLISECONDS);
        log.info("[RETRY] 重试调度器启动 tickMillis={}", tick);
    }

    private void tickSafely() {
        try {
            List<JobRecord> due = repository.findDueRetries(Instant.now(),
                    Math.max(1, properties.getMaxConcurrency()));
            for (JobRecord job : due) {
                // 到点：RETRY_WAITING -> RUNNING 的 CAS 发生在 dispatcher 内，
                // 取消/关闭等并发下状态不再匹配则自然跳过
                dispatcher.dispatch(job);
            }
        } catch (Exception e) {
            log.warn("[RETRY] 扫描周期异常（下个周期继续）: {}", e.toString());
        }
    }

    @PreDestroy
    public void stop() {
        if (timer != null) {
            timer.shutdownNow();
        }
    }
}
