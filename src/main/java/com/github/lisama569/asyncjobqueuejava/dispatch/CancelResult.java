package com.github.lisama569.asyncjobqueuejava.dispatch;

import com.github.lisama569.asyncjobqueuejava.job.JobRecord;

/**
 * 取消结果：
 * @param changed 本次调用是否真正完成了状态迁移（CAS 获胜）
 * @param job     取消后的最新任务快照
 */
public record CancelResult(boolean changed, JobRecord job) {
}
