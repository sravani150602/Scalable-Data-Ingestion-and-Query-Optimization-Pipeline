-- Initial schema migration for the Data Ingestion Pipeline
-- Creates tables and composite indexes for optimized query patterns

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Core ingestion records table
CREATE TABLE IF NOT EXISTS ingestion_records (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id VARCHAR(100) NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100),
    payload JSONB NOT NULL DEFAULT '{}',
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 0,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    checksum VARCHAR(64),
    error_message TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    processing_time_ms FLOAT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP,
    region VARCHAR(50),
    tags JSONB DEFAULT '[]',
    metadata_extra JSONB DEFAULT '{}'
);

-- Composite indexes for optimized query patterns
CREATE INDEX IF NOT EXISTS ix_source_status_created ON ingestion_records (source_type, status, created_at);
CREATE INDEX IF NOT EXISTS ix_category_created ON ingestion_records (category, created_at);
CREATE INDEX IF NOT EXISTS ix_status_priority ON ingestion_records (status, priority, created_at);
CREATE INDEX IF NOT EXISTS ix_source_category ON ingestion_records (source_type, category);
CREATE INDEX IF NOT EXISTS ix_region_status ON ingestion_records (region, status, created_at);
CREATE INDEX IF NOT EXISTS ix_processed_updated ON ingestion_records (processed, updated_at);
CREATE INDEX IF NOT EXISTS ix_source_id ON ingestion_records (source_id);

-- Pipeline runs tracking table
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_config VARCHAR(50) NOT NULL,
    records_ingested INTEGER NOT NULL DEFAULT 0,
    records_failed INTEGER NOT NULL DEFAULT 0,
    total_size_bytes BIGINT NOT NULL DEFAULT 0,
    avg_latency_ms FLOAT,
    p95_latency_ms FLOAT,
    p99_latency_ms FLOAT,
    throughput_rps FLOAT,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'running',
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS ix_pipeline_runs_config_started ON pipeline_runs (pipeline_config, started_at);

-- Query analysis results table
CREATE TABLE IF NOT EXISTS query_analyses (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_name VARCHAR(200) NOT NULL,
    query_text TEXT NOT NULL,
    pipeline_config VARCHAR(50) NOT NULL,
    execution_time_ms FLOAT NOT NULL,
    planning_time_ms FLOAT,
    rows_returned INTEGER,
    scan_type VARCHAR(50),
    index_used VARCHAR(200),
    shared_hit_blocks INTEGER,
    shared_read_blocks INTEGER,
    query_plan JSONB,
    analyzed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_query_analysis_config ON query_analyses (pipeline_config, analyzed_at);
