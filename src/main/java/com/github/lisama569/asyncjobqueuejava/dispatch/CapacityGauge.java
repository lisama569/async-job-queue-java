package com.github.lisama569.asyncjobqueuejava.dispatch;

import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 真实生效的队列容量闸门：统计“已被系统接受但尚未终态”的任务数
 * * （排队中 + 执行中 + 重试等待中）。打满后提交方立即得到背压拒绝。
 */
@Component
public class CapacityGauge {
    private static final Logger log = LoggerFactory.getLogger(CapacityGauge.class);

    private final int capacity;
    private final AtomicInteger admitted = new AtomicInteger(0);

    public CapacityGauge(JobQueueProperties properties) {
        this.capacity = properties.getQueueCapacity();
    }

    /** @return true 获得一个名额；false 队列已满，调用方应拒绝提交 */
    public boolean tryAcquire() {
        int prev = admitted.getAndIncrement();
        if (prev >= capacity) {
            admitted.decrementAndGet();
            log.warn("[CAPACITY] 队列已满 capacity={} inFlight={} 提交被拒绝(背压)",
                    capacity, prev);
            return false;
        }
        return true;
    }

    /** 任务进入终态/被取消后归还名额 */
    public void release() {
        int now = admitted.updateAndGet(v -> v > 0 ? v - 1 : 0);
        log.debug("[CAPACITY] release inFlight={}/{}", now, capacity);
    }

    /** 重启恢复时用持久层未完成任务数初始化，保证恢复后容量语义仍然成立 */
    public void seed(int unfinishedCount) {
        admitted.set(Math.min(unfinishedCount, capacity));
        log.info("[CAPACITY] 恢复未完成任务计数 inFlight={}/{}", admitted.get(), capacity);
    }

    public int inFlight() {
        return admitted.get();
    }

    public int capacity() {
        return capacity;
    }
}
