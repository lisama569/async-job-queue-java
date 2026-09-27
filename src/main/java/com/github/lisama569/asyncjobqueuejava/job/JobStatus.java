package com.github.lisama569.asyncjobqueuejava.job;

/**
 * 任务状态机：
 * PENDING -> RUNNING -> SUCCEEDED
 *                    -> RETRY_WAITING -> (再次) RUNNING
 *                    -> CANCELLED
 * PENDING -> CANCELLED
 * RUNNING/RETRY_WAITING -> DEAD
 */
public enum JobStatus {
    PENDING,
    RUNNING,
    RETRY_WAITING,
    SUCCEEDED,
    CANCELLED,
    DEAD
}
