package com.github.lisama569.asyncjobqueuejava.dispatch;

import com.github.lisama569.asyncjobqueuejava.job.JobRecord;
import com.github.lisama569.asyncjobqueuejava.job.JobRepository;
import com.github.lisama569.asyncjobqueuejava.job.JobRequest;
import com.github.lisama569.asyncjobqueuejava.job.JobStatus;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DefaultJobService implements JobService {

    private static final Logger log = LoggerFactory.getLogger(DefaultJobService.class);

    private final JobRepository repository;
    private final JobQueueProperties properties;
    private final CapacityGauge capacity;
    private final JobDispatcher dispatcher;

    public DefaultJobService(JobRepository repository, JobQueueProperties properties,
                             CapacityGauge capacity, JobDispatcher dispatcher) {
        this.repository = repository;
        this.properties = properties;
        this.capacity = capacity;
        this.dispatcher = dispatcher;
    }

    @Override
    public SubmitResult submit(JobRequest request) {
        validate(request);

        // 1) 幂等键快速通道：已存在则直接返回，绝不产生第二次执行
        if (request.getIdempotencyKey() != null && !request.getIdempotencyKey().isBlank()) {
            Optional<JobRecord> existing = repository.findByIdempotencyKey(
                    request.getIdempotencyKey());
            if (existing.isPresent()) {
                JobRecord e = existing.get();
                log.info("[JOB] idempotentHit jobId={} idempotencyKey={} status={} 重复提交不重复执行",
                        e.getId(), e.getIdempotencyKey(), e.getStatus());
                return new SubmitResult(e, true);
            }
        }

        // 2) 背压闸门：容量满了立即明确拒绝，不静默排队/丢弃。
        //    但若是同一个幂等键的并发重复提交（别人正在插入），
        //    再查一次幂等键命中则直接折叠为已有任务，不应把重复请求当成背压。
        if (!capacity.tryAcquire()) {
            if (request.getIdempotencyKey() != null && !request.getIdempotencyKey().isBlank()) {
                Optional<JobRecord> raced =
                        repository.findByIdempotencyKey(request.getIdempotencyKey());
                if (raced.isPresent()) {
                    log.info("[JOB] idempotentFullHit jobId={} status={} 容量已满但命中幂等键，折叠为已有任务",
                            raced.get().getId(), raced.get().getStatus());
                    return new SubmitResult(raced.get(), true);
                }
            }
            throw new QueueFullException("任务队列已满(capacity=" + capacity.capacity()
                    + ", inFlight=" + capacity.inFlight() + ")，请稍后重试");
        }

        // 3) 构造 PENDING 任务并落库
        Instant now = Instant.now();
        JobRecord job = new JobRecord();
        job.setId(UUID.randomUUID().toString().replace("-", ""));
        job.setIdempotencyKey(blankToNull(request.getIdempotencyKey()));
        job.setGroupKey(request.getGroupKey().trim());
        job.setJobType(request.getJobType().trim());
        job.setPayload(request.getPayload());
        job.setStatus(JobStatus.PENDING);
        job.setAttempts(0);
        job.setMaxAttempts(request.getMaxAttempts() != null && request.getMaxAttempts() > 0
                ? request.getMaxAttempts() : properties.getDefaultMaxAttempts());
        job.setBackoffMillis(properties.getBackoffInitialMillis());
        job.setCreatedAt(now);
        job.setUpdatedAt(now);
        job.setVersion(0);

        JobRecord saved = repository.insertIfAbsent(job);

        // 4) 并发下幂等键被别的线程抢先插入：归还名额，返回已有任务
        if (!saved.getId().equals(job.getId())) {
            capacity.release();
            log.info("[JOB] idempotentRace jobId={} idempotencyKey={} status={} 并发重复提交被幂等折叠",
                    saved.getId(), saved.getIdempotencyKey(), saved.getStatus());
            return new SubmitResult(saved, true);
        }

        log.info("[JOB] accepted jobId={} idempotencyKey={} group={} type={} status=PENDING inFlight={}/{}",
                saved.getId(), saved.getIdempotencyKey(), saved.getGroupKey(),
                saved.getJobType(), capacity.inFlight(), capacity.capacity());

        // 5) 唤醒该分组的串行队列
        dispatcher.notifyGroup(saved.getGroupKey());
        return new SubmitResult(saved, false);
    }

    @Override
    public Optional<JobRecord> get(String id) {
        return repository.findById(id);
    }

    @Override
    public Optional<CancelResult> cancel(String id) {
        Optional<JobRecord> found = repository.findById(id);
        if (found.isEmpty()) {
            return Optional.empty();
        }
        JobRecord job = found.get();
        JobStatus from = job.getStatus();
        // 只允许取消尚未开始执行 / 重试等待中的任务；
        // RUNNING 不抢占（避免任务副作用不可知），SUCCEEDED/DEAD/CANCELLED 已终态。
        boolean ok = false;
        if (from == JobStatus.PENDING || from == JobStatus.RETRY_WAITING) {
            job.setStatus(JobStatus.CANCELLED);
            job.setFinishedAt(Instant.now());
            job.setFailureReason(from == JobStatus.PENDING
                    ? "用户取消：任务仍在排队中，尚未执行"
                    : "用户取消：任务处于重试等待中(attempts=" + job.getAttempts() + ")");
            ok = repository.compareAndUpdate(job, List.of(from));
        }
        if (ok) {
            capacity.release();
            log.info("[JOB] cancelSucceeded jobId={} group={} {} -> CANCELLED",
                    job.getId(), job.getGroupKey(), from);
            return Optional.of(new CancelResult(true, job));
        }
        JobRecord latest = repository.findById(id).orElse(job);
        log.info("[JOB] cancelIgnored jobId={} currentStatus={} (仅 PENDING/RETRY_WAITING 可取消)",
                id, latest.getStatus());
        return Optional.of(new CancelResult(false, latest));
    }

    @Override
    public List<JobRecord> listDead(int limit) {
        return repository.findByStatus(JobStatus.DEAD, Math.max(1, Math.min(limit, 200)));
    }

    private void validate(JobRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("请求体不能为空");
        }
        if (request.getGroupKey() == null || request.getGroupKey().isBlank()) {
            throw new IllegalArgumentException("groupKey 不能为空");
        }
        if (request.getJobType() == null || request.getJobType().isBlank()) {
            throw new IllegalArgumentException("jobType 不能为空");
        }
    }

    private static String blankToNull(String s) {
        return (s == null || s.isBlank()) ? null : s.trim();
    }
}
