package com.github.lisama569.asyncjobqueuejava.dispatch;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 队列与重试相关配置（async-job 前缀）。
 */
@ConfigurationProperties(prefix = "async-job")
public class JobQueueProperties {
    private int maxConcurrency = 0;
    private int queueCapacity = 0;
    private int defaultMaxAttempts = 0;
    private long backoffInitialMillis = 0;
    private double backoffMultiplier = 1.0;
    private long backoffMaxMillis = 0;
    private long retryTickMillis = 0;

    public int getMaxConcurrency() { return maxConcurrency; }
    public void setMaxConcurrency(int v) { this.maxConcurrency = v; }
    public int getQueueCapacity() { return queueCapacity; }
    public void setQueueCapacity(int v) { this.queueCapacity = v; }
    public int getDefaultMaxAttempts() { return defaultMaxAttempts; }
    public void setDefaultMaxAttempts(int v) { this.defaultMaxAttempts = v; }
    public long getBackoffInitialMillis() { return backoffInitialMillis; }
    public void setBackoffInitialMillis(long v) { this.backoffInitialMillis = v; }
    public double getBackoffMultiplier() { return backoffMultiplier; }
    public void setBackoffMultiplier(double v) { this.backoffMultiplier = v; }
    public long getBackoffMaxMillis() { return backoffMaxMillis; }
    public void setBackoffMaxMillis(long v) { this.backoffMaxMillis = v; }
    public long getRetryTickMillis() { return retryTickMillis; }
    public void setRetryTickMillis(long v) { this.retryTickMillis = v; }
}
