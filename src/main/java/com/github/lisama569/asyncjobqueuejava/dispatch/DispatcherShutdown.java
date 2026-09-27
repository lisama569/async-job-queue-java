package com.github.lisama569.asyncjobqueuejava.dispatch;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Spring 关闭时优雅停止调度器：不再接受新执行，等待在执行任务结束。
 * 未完成任务由下次启动的 StartupRecovery 兜底，不会丢失。
 */
@Component
public class DispatcherShutdown {

    private static final Logger log = LoggerFactory.getLogger(DispatcherShutdown.class);

    private final DefaultJobDispatcher dispatcher;
    private final JobQueueProperties properties;

    public DispatcherShutdown(DefaultJobDispatcher dispatcher, JobQueueProperties properties) {
        this.dispatcher = dispatcher;
        this.properties = properties;
    }

    @PreDestroy
    public void shutdown() {
        long await = Math.max(500, properties.getBackoffMaxMillis());
        log.info("[SHUTDOWN] 调度器开始优雅关闭 awaitMillis={}", await);
        dispatcher.shutdown(await);
        log.info("[SHUTDOWN] 调度器已停止");
    }
}
