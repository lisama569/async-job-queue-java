package com.github.lisama569.asyncjobqueuejava.dispatch;

import com.github.lisama569.asyncjobqueuejava.job.JobRecord;
import com.github.lisama569.asyncjobqueuejava.job.JobRequest;
import java.util.List;
import java.util.Optional;

/**
 * 任务提交/查询/取消的应用服务。
 */
public interface JobService {
    /** 提交任务；相同幂等键返回已有任务，不产生第二次执行。 */
    SubmitResult submit(JobRequest request);

    Optional<JobRecord> get(String id);

    /**
     * 取消排队中/重试等待中的任务。
     * @return 任务不存在返回 empty；CancelResult.changed 标识本次调用是否真正完成迁移
     */
    Optional<CancelResult> cancel(String id);

    List<JobRecord> listDead(int limit);
}
