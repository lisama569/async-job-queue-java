package com.github.lisama569.asyncjobqueuejava.dispatch;

import com.github.lisama569.asyncjobqueuejava.job.JobRecord;

/**
 * 提交结果：duplicate=true 表示命中幂等键，返回的是已有任务，没有产生第二次执行。
 */
public record SubmitResult(JobRecord job, boolean duplicate) {
}
