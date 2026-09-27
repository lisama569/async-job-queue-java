package com.github.lisama569.asyncjobqueuejava.web;

import com.github.lisama569.asyncjobqueuejava.dispatch.JobService;
import com.github.lisama569.asyncjobqueuejava.dispatch.SubmitResult;
import com.github.lisama569.asyncjobqueuejava.dispatch.QueueFullException;
import com.github.lisama569.asyncjobqueuejava.job.JobRecord;
import com.github.lisama569.asyncjobqueuejava.job.JobRequest;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * 任务提交 / 查询 / 取消 / 死信列表的 HTTP 接口。
 */
@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private static final Logger log = LoggerFactory.getLogger(JobController.class);

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    /**
     * 提交任务。相同 idempotencyKey 的重复提交返回 200 + duplicate=true 的同一任务；
     * 队列/并发容量打满返回 503 + 明确错误（背压）。
     */
    @PostMapping
    public ResponseEntity<JobViews.JobView> submit(@RequestBody JobRequest request) {
        SubmitResult result = jobService.submit(request);
        HttpStatus status = result.duplicate() ? HttpStatus.OK : HttpStatus.CREATED;
        return ResponseEntity.status(status)
                .body(JobViews.JobView.of(result.job(), result.duplicate()));
    }

    @GetMapping("/{id}")
    public ResponseEntity<JobViews.JobView> get(@PathVariable String id) {
        Optional<JobRecord> job = jobService.get(id);
        return job.map(j -> ResponseEntity.ok(JobViews.JobView.of(j, false)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/cancel")
    public ResponseEntity<JobViews.JobView> cancel(@PathVariable String id) {
        Optional<com.github.lisama569.asyncjobqueuejava.dispatch.CancelResult> r =
                jobService.cancel(id);
        return r.map(c -> ResponseEntity.ok(JobViews.JobView.of(c.job(), false)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/dead")
    public List<JobViews.JobView> dead(@RequestParam(defaultValue = "50") int limit) {
        return jobService.listDead(limit).stream()
                .map(j -> JobViews.JobView.of(j, false))
                .toList();
    }

    @ExceptionHandler(QueueFullException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public java.util.Map<String, Object> onQueueFull(QueueFullException e) {
        log.warn("[HTTP] 503 backpressure: {}", e.getMessage());
        return java.util.Map.of(
                "error", "QUEUE_FULL",
                "message", e.getMessage(),
                "retryable", true);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public java.util.Map<String, Object> onBadRequest(IllegalArgumentException e) {
        return java.util.Map.of("error", "BAD_REQUEST", "message", e.getMessage());
    }
}
