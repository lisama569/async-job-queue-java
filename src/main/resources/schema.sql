-- H2 文件库，应用启动时自动执行（spring.sql.init.mode=always）
CREATE TABLE IF NOT EXISTS async_job (
    id               VARCHAR(64)  PRIMARY KEY,
    idempotency_key  VARCHAR(128),
    group_key        VARCHAR(128) NOT NULL,
    job_type         VARCHAR(64)  NOT NULL,
    payload          CLOB,
    status           VARCHAR(32)  NOT NULL,
    attempts         INT          NOT NULL DEFAULT 0,
    max_attempts     INT          NOT NULL,
    backoff_millis   BIGINT       NOT NULL,
    next_run_at      TIMESTAMP,
    last_error       CLOB,
    failure_reason   CLOB,
    created_at       TIMESTAMP    NOT NULL,
    updated_at       TIMESTAMP    NOT NULL,
    finished_at      TIMESTAMP,
    version          BIGINT       NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_async_job_idem ON async_job (idempotency_key);
CREATE INDEX IF NOT EXISTS idx_async_job_group ON async_job (group_key, created_at);
CREATE INDEX IF NOT EXISTS idx_async_job_retry ON async_job (status, next_run_at);
