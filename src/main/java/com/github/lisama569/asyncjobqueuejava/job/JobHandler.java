package com.github.lisama569.asyncjobqueuejava.job;

/**
 * 任务处理器：业务实现本接口。实现方应保证副作用幂等
 * （崩溃恢复场景下同一个任务可能被执行超过一次）。
 */
public interface JobHandler {
    String type();
    void handle(JobRecord job) throws Exception;
}
