package com.github.lisama569.asyncjobqueuejava.tests;

import com.github.lisama569.asyncjobqueuejava.job.JobRecord;
import com.github.lisama569.asyncjobqueuejava.job.JobRepository;
import com.github.lisama569.asyncjobqueuejava.job.JobStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Predicate;
import org.awaitility.Awaitility;

/**
 * 测试辅助：构造请求、轮询等待终态。
 */
public final class TestSupport {

    private TestSupport() {
    }

    public static com.github.lisama569.asyncjobqueuejava.job.JobRequest request(
            String idemKey, String group, String type, String payload, Integer maxAttempts) {
        var r = new com.github.lisama569.asyncjobqueuejava.job.JobRequest();
        r.setIdempotencyKey(idemKey);
        r.setGroupKey(group);
        r.setJobType(type);
        r.setPayload(payload);
        r.setMaxAttempts(maxAttempts);
        return r;
    }

    public static JobRecord awaitStatus(JobRepository repo, String id, JobStatus status) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(20))
                .until(() -> repo.findById(id)
                        .map(j -> j.getStatus() == status).orElse(false));
        return repo.findById(id).orElseThrow();
    }

    public static JobRecord awaitThat(JobRepository repo, String id,
                                      Predicate<JobRecord> predicate) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(20))
                .until(() -> repo.findById(id).filter(predicate).isPresent());
        return repo.findById(id).orElseThrow();
    }

    public static Instant now() {
        return Instant.now();
    }
}
