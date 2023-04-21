CREATE TABLE ad_impression (
    user_id       VARCHAR(32),
    lineitem_id   BIGINT,
    unit_id       BIGINT,
    revenue       DECIMAL(27,9),
    created_at    TIMESTAMP
);

CREATE TABLE ad_click (
    user_id       VARCHAR(32),
    lineitem_id   BIGINT,
    unit_id       BIGINT,
    revenue       DECIMAL(27,9),
    created_at    TIMESTAMP
);

CREATE TABLE ad_lineitem_staging (
    id             BIGINT,
    revenue_type   VARCHAR(16),
    created_at     TIMESTAMP,
    updated_at     TIMESTAMP
);

CREATE TABLE ad_lineitem (
    id             BIGINT PRIMARY KEY,
    revenue_type   VARCHAR(16),
    created_at     TIMESTAMP,
    updated_at     TIMESTAMP
);

CREATE TABLE unit_staging (
    id           BIGINT,
    unit_type    VARCHAR(16),
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP
);

CREATE TABLE unit (
    id   BIGINT PRIMARY KEY,
    unit_type    VARCHAR(16),
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP
);

CREATE TABLE m1_d_ad_unit_metrics (
    lineitem_id        BIGINT,
    revenue_type       VARCHAR(16),
    unit_type          VARCHAR(16),
    unit_id            BIGINT,
    data_at            TIMESTAMP,
    impression_count   BIGINT,
    click_count        BIGINT,
    revenue_sum        DECIMAL(27,9),
    created_at         TIMESTAMP,
    updated_at         TIMESTAMP
);

CREATE TABLE ad_lineitem_scheduler (
    staging_count   BIGINT,
    query_count   BIGINT,
    started_at    TIMESTAMP,
    end_at     TIMESTAMP
);

CREATE TABLE unit_scheduler (
    staging_count   BIGINT,
    query_count   BIGINT,
    started_at    TIMESTAMP,
    end_at     TIMESTAMP
);