package com.github.lisama569.asyncjobqueuejava.job;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * 任务持久化仓储。
 */
public interface JobRepository {
    /** 插入新任务；若幂等键已存在则返回已有任务。 */
    JobRecord insertIfAbsent(JobRecord job);

    Optional<JobRecord> findById(String id);

    Optional<JobRecord> findByIdempotencyKey(String key);

    /**
     * 乐观条件更新：仅当当前状态属于 expected 且 version 匹配时生效。
     * @return 是否更新成功
     */
    boolean compareAndUpdate(JobRecord job, List<JobStatus> expected);

    /** 取出同一分组中创建最早、且可执行的 PENDING 任务（加跳过锁）。 */
    Optional<JobRecord> acquireNextPending(String groupKey);

    /** 启动恢复：所有 PENDING 任务；以及中断在 RUNNING 的任务（退回重试）。 */
    List<JobRecord> findRecoverable();

    /** 到点可重试的任务（RETRY_WAITING 且 next_run_at <= now）。 */
    List<JobRecord> findDueRetries(Instant now, int limit);

    /** 所有未结束任务（PENDING/RETRY_WAITING/RUNNING）。 */
    List<JobRecord> findUnfinished();

    List<JobRecord> findByStatus(JobStatus status, int limit);

    /** 清空任务（测试/重置场景使用）。 */
    void deleteAll();
}
