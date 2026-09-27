package com.github.lisama569.asyncjobqueuejava.web;

import com.github.lisama569.asyncjobqueuejava.job.JobRecord;

/**
 * HTTP 视图对象（只读快照）。
 */
public final class JobViews {
    private JobViews() {
    }

    public record JobView(
            String id,
            String idempotencyKey,
            String groupKey,
            String jobType,
            String payload,
            String status,
            int attempts,
            int maxAttempts,
            Long nextRetryInMillis,
            String lastError,
            String failureReason,
            String createdAt,
            String updatedAt,
            String finishedAt,
            long version,
            boolean duplicate) {

        static JobView of(JobRecord j, boolean duplicate) {
            long nextIn = j.getNextRunAt() == null ? -1
                    : Math.max(0, java.time.Duration.between(
                            java.time.Instant.now(), j.getNextRunAt()).toMillis());
            return new JobView(j.getId(), j.getIdempotencyKey(), j.getGroupKey(),
                    j.getJobType(), j.getPayload(), j.getStatus().name(),
                    j.getAttempts(), j.getMaxAttempts(),
                    j.getNextRunAt() == null ? null : nextIn,
                    j.getLastError(), j.getFailureReason(),
                    String.valueOf(j.getCreatedAt()), String.valueOf(j.getUpdatedAt()),
                    j.getFinishedAt() == null ? null : String.valueOf(j.getFinishedAt()),
                    j.getVersion(), duplicate);
        }
    }
}
