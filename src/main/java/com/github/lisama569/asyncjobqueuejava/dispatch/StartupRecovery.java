package com.github.lisama569.asyncjobqueuejava.dispatch;

import com.github.lisama569.asyncjobqueuejava.job.JobRecord;
import com.github.lisama569.asyncjobqueuejava.job.JobRepository;
import com.github.lisama569.asyncjobqueuejava.job.JobStatus;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * 进程启动后恢复未完成任务：
 * <ul>
 *   <li>PENDING：仍在排队，按分组唤醒继续按序执行；</li>
 *   <li>RUNNING：上次进程在执行中退出（可能已产生副作用），
 *       退回 PENDING 重投一次。处理器需实现幂等，避免重复副作用；</li>
 *   <li>RETRY_WAITING：保留退避计划，由 RetryScheduler 到点拉起。</li>
 * </ul>
 */
@Component
public class StartupRecovery {

    private static final Logger log = LoggerFactory.getLogger(StartupRecovery.class);

    private final JobRepository repository;
    private final JobDispatcher dispatcher;
    private final CapacityGauge capacity;

    public StartupRecovery(JobRepository repository, JobDispatcher dispatcher,
                           CapacityGauge capacity) {
        this.repository = repository;
        this.dispatcher = dispatcher;
        this.capacity = capacity;
    }

    @EventListener(ApplicationReadyEvent.class)
    @Order(0)
    public void recover() {
        List<JobRecord> unfinished = repository.findUnfinished();
        capacity.seed(unfinished.size());

        Set<String> groupsToWake = new HashSet<>();
        int recoveredRunning = 0;
        int pending = 0;
        int waiting = 0;

        for (JobRecord job : unfinished) {
            if (job.getStatus() == JobStatus.RUNNING) {
                // 崩溃在执行中途：回到 PENDING 等待重投（at-least-once，依赖处理器幂等）
                job.setStatus(JobStatus.PENDING);
                job.setFailureReason("进程重启恢复：上次执行被中断，重新排队(attempts="
                        + job.getAttempts() + ")");
                boolean reset = repository.compareAndUpdate(
                        job, List.of(JobStatus.RUNNING));
                if (reset) {
                    recoveredRunning++;
                    groupsToWake.add(job.getGroupKey());
                    log.warn("[RECOVERY] jobId={} group={} RUNNING(中断) -> PENDING 重新排队",
                            job.getId(), job.getGroupKey());
                }
            } else if (job.getStatus() == JobStatus.PENDING) {
                pending++;
                groupsToWake.add(job.getGroupKey());
            } else {
                waiting++;
            }
        }

        for (String group : groupsToWake) {
            dispatcher.notifyGroup(group);
        }
        log.info("[RECOVERY] 恢复完成 pending={} interrupted->pending={} retryWaiting={} 唤醒分组数={}",
                pending, recoveredRunning, waiting, groupsToWake.size());
    }
}
