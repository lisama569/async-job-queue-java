package com.github.lisama569.asyncjobqueuejava.job;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class JdbcJobRepository implements JobRepository {

    private static final String COLS =
            "id, idempotency_key, group_key, job_type, payload, status, attempts, "
            + "max_attempts, backoff_millis, next_run_at, last_error, failure_reason, "
            + "created_at, updated_at, finished_at, version";

    private final JdbcTemplate jdbc;

    public JdbcJobRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    @Transactional
    public JobRecord insertIfAbsent(JobRecord job) {
        // 幂等键为空：直接插入
        if (job.getIdempotencyKey() == null || job.getIdempotencyKey().isBlank()) {
            insert(job);
            return job;
        }
        // 先查：命中则直接返回已有任务，不产生第二次执行
        Optional<JobRecord> existing = findByIdempotencyKey(job.getIdempotencyKey());
        if (existing.isPresent()) {
            return existing.get();
        }
        try {
            insert(job);
            return job;
        } catch (DuplicateKeyException e) {
            // 并发提交同一个幂等键：唯一索引兜底
            return findByIdempotencyKey(job.getIdempotencyKey()).orElseThrow();
        }
    }

    private void insert(JobRecord j) {
        jdbc.update("INSERT INTO async_job (" + COLS + ") VALUES (" + placeholders(COLS) + ")",
                j.getId(), j.getIdempotencyKey(), j.getGroupKey(), j.getJobType(), j.getPayload(),
                j.getStatus().name(), j.getAttempts(), j.getMaxAttempts(), j.getBackoffMillis(),
                ts(j.getNextRunAt()), j.getLastError(), j.getFailureReason(),
                ts(j.getCreatedAt()), ts(j.getUpdatedAt()), ts(j.getFinishedAt()), j.getVersion());
    }

    @Override
    public Optional<JobRecord> findById(String id) {
        List<JobRecord> list = jdbc.query(
                "SELECT " + COLS + " FROM async_job WHERE id = ?", new JobRowMapper(), id);
        return list.stream().findFirst();
    }

    @Override
    public Optional<JobRecord> findByIdempotencyKey(String key) {
        List<JobRecord> list = jdbc.query(
                "SELECT " + COLS + " FROM async_job WHERE idempotency_key = ?",
                new JobRowMapper(), key);
        return list.stream().findFirst();
    }

    @Override
    public boolean compareAndUpdate(JobRecord job, List<JobStatus> expected) {
        String inSql = String.join(",", expected.stream().map(s -> "?").toList());
        Instant now = Instant.now();
        job.setUpdatedAt(now);
        int rows = jdbc.update(
                "UPDATE async_job SET status = ?, attempts = ?, backoff_millis = ?, "
                + "next_run_at = ?, last_error = ?, failure_reason = ?, updated_at = ?, "
                + "finished_at = ?, version = version + 1 "
                + "WHERE id = ? AND version = ? AND status IN (" + inSql + ")",
                ps -> {
                    int i = 1;
                    ps.setString(i++, job.getStatus().name());
                    ps.setInt(i++, job.getAttempts());
                    ps.setLong(i++, job.getBackoffMillis());
                    ps.setTimestamp(i++, ts(job.getNextRunAt()));
                    ps.setString(i++, truncate(job.getLastError()));
                    ps.setString(i++, truncate(job.getFailureReason()));
                    ps.setTimestamp(i++, ts(now));
                    ps.setTimestamp(i++, ts(job.getFinishedAt()));
                    ps.setString(i++, job.getId());
                    ps.setLong(i++, job.getVersion());
                    for (JobStatus s : expected) {
                        ps.setString(i++, s.name());
                    }
                });
        if (rows > 0) {
            job.setVersion(job.getVersion() + 1);
        }
        return rows > 0;
    }

    @Override
    @Transactional
    public Optional<JobRecord> acquireNextPending(String groupKey) {
        // 同分组取最早的 PENDING；FOR UPDATE 行锁防止多个分组协程重复领取
        List<JobRecord> locked = jdbc.query(
                "SELECT " + COLS + " FROM async_job WHERE group_key = ? AND status = 'PENDING' "
                + "ORDER BY created_at ASC, id ASC FETCH FIRST 1 ROWS ONLY FOR UPDATE",
                new JobRowMapper(), groupKey);
        return locked.stream().findFirst();
    }

    @Override
    public List<JobRecord> findRecoverable() {
        // PENDING 仍排队；RUNNING 是上次进程崩溃时正在执行的任务
        return jdbc.query("SELECT " + COLS + " FROM async_job "
                + "WHERE status IN ('PENDING','RUNNING') ORDER BY created_at ASC",
                new JobRowMapper());
    }

    @Override
    public List<JobRecord> findDueRetries(Instant now, int limit) {
        return jdbc.query("SELECT " + COLS + " FROM async_job "
                + "WHERE status = 'RETRY_WAITING' AND next_run_at IS NOT NULL "
                + "AND next_run_at <= ? ORDER BY next_run_at ASC FETCH FIRST ? ROWS ONLY",
                new JobRowMapper(), ts(now), limit);
    }

    @Override
    public List<JobRecord> findUnfinished() {
        return jdbc.query("SELECT " + COLS + " FROM async_job "
                + "WHERE status IN ('PENDING','RUNNING','RETRY_WAITING') ORDER BY created_at ASC",
                new JobRowMapper());
    }

    @Override
    public List<JobRecord> findByStatus(JobStatus status, int limit) {
        return jdbc.query("SELECT " + COLS + " FROM async_job WHERE status = ? "
                + "ORDER BY updated_at DESC FETCH FIRST ? ROWS ONLY",
                new JobRowMapper(), status.name(), limit);
    }

    @Override
    public void deleteAll() {
        jdbc.update("DELETE FROM async_job");
    }

    private static String placeholders(String cols) {
        int n = cols.split(",").length;
        return String.join(",", java.util.Collections.nCopies(n, "?"));
    }

    private static Timestamp ts(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static String truncate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= 4000 ? s : s.substring(0, 4000);
    }

    private static Instant instant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }

    private static class JobRowMapper implements RowMapper<JobRecord> {
        @Override
        public JobRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            JobRecord j = new JobRecord();
            j.setId(rs.getString("id"));
            j.setIdempotencyKey(rs.getString("idempotency_key"));
            j.setGroupKey(rs.getString("group_key"));
            j.setJobType(rs.getString("job_type"));
            j.setPayload(rs.getString("payload"));
            j.setStatus(JobStatus.valueOf(rs.getString("status")));
            j.setAttempts(rs.getInt("attempts"));
            j.setMaxAttempts(rs.getInt("max_attempts"));
            j.setBackoffMillis(rs.getLong("backoff_millis"));
            j.setNextRunAt(instant(rs.getTimestamp("next_run_at")));
            j.setLastError(rs.getString("last_error"));
            j.setFailureReason(rs.getString("failure_reason"));
            j.setCreatedAt(instant(rs.getTimestamp("created_at")));
            j.setUpdatedAt(instant(rs.getTimestamp("updated_at")));
            j.setFinishedAt(instant(rs.getTimestamp("finished_at")));
            j.setVersion(rs.getLong("version"));
            return j;
        }
    }
}
