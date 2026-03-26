-- ══════════════════════════════════════════════════════════════════════════════
--  GitHub Event Intelligence System — MySQL Schema
--  Run once to initialize the database
--  Compatible with MySQL 5.7+ / MariaDB 10.3+
-- ══════════════════════════════════════════════════════════════════════════════

CREATE DATABASE IF NOT EXISTS event_intelligence
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE event_intelligence;

-- ─── Core Events Table ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS events (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    github_event_id VARCHAR(50)   NOT NULL,
    event_type      VARCHAR(60)   NOT NULL,
    actor_login     VARCHAR(120)  NOT NULL,
    actor_avatar    VARCHAR(512)  DEFAULT NULL,
    repo_name       VARCHAR(300)  NOT NULL,
    repo_url        VARCHAR(512)  DEFAULT NULL,
    payload         JSON          DEFAULT NULL,  -- raw GitHub payload
    created_at      DATETIME      NOT NULL,
    ingested_at     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- ── NLP Analysis Fields ──────────────────────────────────────────────────
    sentiment_score FLOAT         DEFAULT NULL,  -- -1.0 to +1.0
    sentiment_label VARCHAR(20)   DEFAULT 'neutral',
    subjectivity    FLOAT         DEFAULT NULL,  -- 0.0 to 1.0
    keywords        JSON          DEFAULT NULL,  -- ["array", "of", "words"]
    category        VARCHAR(40)   DEFAULT NULL,  -- code | community | discussion ...
    language        VARCHAR(60)   DEFAULT NULL,  -- Python | JavaScript | etc.
    commit_message  TEXT          DEFAULT NULL,  -- extracted commit messages
    impact_score    FLOAT         DEFAULT 0.0,   -- calculated engagement score

    UNIQUE KEY uq_github_event_id (github_event_id),
    INDEX idx_event_type   (event_type),
    INDEX idx_actor_login  (actor_login),
    INDEX idx_repo_name    (repo_name(191)),
    INDEX idx_created_at   (created_at),
    INDEX idx_category     (category),
    INDEX idx_language     (language),
    INDEX idx_sentiment    (sentiment_label)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- ─── Hourly Stats Aggregation ─────────────────────────────────────────────────
-- Pre-aggregated stats for fast dashboard queries
CREATE TABLE IF NOT EXISTS hourly_stats (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    stat_date   DATE        NOT NULL,
    stat_hour   TINYINT     NOT NULL,   -- 0..23
    event_type  VARCHAR(60) NOT NULL,
    event_count INT         NOT NULL DEFAULT 0,
    repo_count  INT         NOT NULL DEFAULT 0,
    user_count  INT         NOT NULL DEFAULT 0,

    UNIQUE KEY uq_hour_type (stat_date, stat_hour, event_type),
    INDEX idx_stat_date (stat_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ─── Trending Repositories ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trending_repos (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    repo_name       VARCHAR(300)  NOT NULL,
    repo_url        VARCHAR(512)  DEFAULT NULL,
    total_events    INT           NOT NULL DEFAULT 0,
    push_count      INT           NOT NULL DEFAULT 0,
    star_count      INT           NOT NULL DEFAULT 0,
    fork_count      INT           NOT NULL DEFAULT 0,
    pr_count        INT           NOT NULL DEFAULT 0,
    issue_count     INT           NOT NULL DEFAULT 0,
    primary_language VARCHAR(60)  DEFAULT NULL,
    avg_sentiment   FLOAT         DEFAULT NULL,
    first_seen      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen       DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uq_repo_name (repo_name(191)),
    INDEX idx_total_events  (total_events DESC),
    INDEX idx_last_seen     (last_seen)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- ─── NLP Keyword Frequency ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS keyword_freq (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    keyword     VARCHAR(100)  NOT NULL,
    frequency   INT           NOT NULL DEFAULT 1,
    category    VARCHAR(40)   DEFAULT NULL,
    last_seen   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uq_keyword (keyword),
    INDEX idx_frequency (frequency DESC),
    INDEX idx_category  (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ─── System Health Log ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS system_log (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    service     VARCHAR(50)   NOT NULL,   -- poller | processor | api
    log_level   VARCHAR(10)   NOT NULL,   -- INFO | WARN | ERROR
    message     TEXT          NOT NULL,
    created_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_service    (service),
    INDEX idx_log_level  (log_level),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ─── Useful Views ─────────────────────────────────────────────────────────────

-- Recent events (last 24 hours)
CREATE OR REPLACE VIEW v_recent_events AS
    SELECT id, github_event_id, event_type, actor_login, actor_avatar,
           repo_name, created_at, sentiment_label, sentiment_score,
           keywords, category, language, impact_score
    FROM events
    WHERE created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
    ORDER BY created_at DESC;


-- Event type distribution (last 24 h)
CREATE OR REPLACE VIEW v_event_type_dist AS
    SELECT event_type,
           COUNT(*) AS cnt,
           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
    FROM events
    WHERE created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
    GROUP BY event_type
    ORDER BY cnt DESC;


-- Top contributors (last 24 h)
CREATE OR REPLACE VIEW v_top_contributors AS
    SELECT actor_login, actor_avatar,
           COUNT(*) AS event_count,
           COUNT(DISTINCT repo_name) AS repos_touched
    FROM events
    WHERE created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
    GROUP BY actor_login, actor_avatar
    ORDER BY event_count DESC
    LIMIT 20;