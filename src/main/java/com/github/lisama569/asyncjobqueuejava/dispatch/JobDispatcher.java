package com.github.lisama569.asyncjobqueuejava.dispatch;

import com.github.lisama569.asyncjobqueuejava.job.JobRecord;

/**
 * 调度入口：负责唤醒分组队列与执行任务。
 */
public interface JobDispatcher {
    /** 唤醒某个分组（有新任务或恢复时调用）。 */
    void notifyGroup(String groupKey);

    /** 直接调度一个任务（重试定时器到点时使用）。 */
    void dispatch(JobRecord job);
}
