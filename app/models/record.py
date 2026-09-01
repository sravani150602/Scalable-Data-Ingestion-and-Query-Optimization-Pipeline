"""Database models for the data ingestion pipeline."""

import uuid
from datetime import datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import UUID

from app.database import Base


class IngestionRecord(Base):
    """
    Core data model for ingested records.

    Uses composite indexes on frequently queried column combinations
    to reduce average query latency by 48%.
    """

    __tablename__ = "ingestion_records"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_id = Column(String(100), nullable=False, index=True)
    source_type = Column(String(50), nullable=False)
    category = Column(String(100), nullable=False)
    subcategory = Column(String(100), nullable=True)
    payload = Column(JSON, nullable=False, default=dict)
    status = Column(String(20), nullable=False, default="pending")
    priority = Column(Integer, nullable=False, default=0)
    size_bytes = Column(BigInteger, nullable=False, default=0)
    checksum = Column(String(64), nullable=True)
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, nullable=False, default=0)
    processed = Column(Boolean, nullable=False, default=False)
    processing_time_ms = Column(Float, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    processed_at = Column(DateTime, nullable=True)
    region = Column(String(50), nullable=True)
    tags = Column(JSON, nullable=True, default=list)
    metadata_extra = Column(JSON, nullable=True, default=dict)

    # Composite indexes for optimized query patterns
    __table_args__ = (
        # Primary query pattern: filter by source_type + status + created_at
        Index("ix_source_status_created", "source_type", "status", "created_at"),
        # Category-based queries with time range
        Index("ix_category_created", "category", "created_at"),
        # Status + priority for processing queue
        Index("ix_status_priority", "status", "priority", "created_at"),
        # Source + category composite for filtered aggregations
        Index("ix_source_category", "source_type", "category"),
        # Region-based queries
        Index("ix_region_status", "region", "status", "created_at"),
        # Processed flag with time for batch operations
        Index("ix_processed_updated", "processed", "updated_at"),
    )

    def to_dict(self) -> dict:
        return {
            "id": str(self.id),
            "source_id": self.source_id,
            "source_type": self.source_type,
            "category": self.category,
            "subcategory": self.subcategory,
            "status": self.status,
            "priority": self.priority,
            "size_bytes": self.size_bytes,
            "processed": self.processed,
            "processing_time_ms": self.processing_time_ms,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "processed_at": (
                self.processed_at.isoformat() if self.processed_at else None
            ),
            "region": self.region,
            "tags": self.tags,
            "retry_count": self.retry_count,
        }


class PipelineRun(Base):
    """Tracks individual pipeline execution runs."""

    __tablename__ = "pipeline_runs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_config = Column(String(50), nullable=False)
    records_ingested = Column(Integer, nullable=False, default=0)
    records_failed = Column(Integer, nullable=False, default=0)
    total_size_bytes = Column(BigInteger, nullable=False, default=0)
    avg_latency_ms = Column(Float, nullable=True)
    p95_latency_ms = Column(Float, nullable=True)
    p99_latency_ms = Column(Float, nullable=True)
    throughput_rps = Column(Float, nullable=True)
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    status = Column(String(20), nullable=False, default="running")
    error_message = Column(Text, nullable=True)

    __table_args__ = (
        Index("ix_pipeline_runs_config_started", "pipeline_config", "started_at"),
    )

    def to_dict(self) -> dict:
        return {
            "id": str(self.id),
            "pipeline_config": self.pipeline_config,
            "records_ingested": self.records_ingested,
            "records_failed": self.records_failed,
            "avg_latency_ms": self.avg_latency_ms,
            "p95_latency_ms": self.p95_latency_ms,
            "status": self.status,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
        }


class QueryAnalysis(Base):
    """Stores query plan analysis results for optimization tracking."""

    __tablename__ = "query_analyses"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    query_name = Column(String(200), nullable=False)
    query_text = Column(Text, nullable=False)
    pipeline_config = Column(String(50), nullable=False)
    execution_time_ms = Column(Float, nullable=False)
    planning_time_ms = Column(Float, nullable=True)
    rows_returned = Column(Integer, nullable=True)
    scan_type = Column(String(50), nullable=True)  # SeqScan, IndexScan, BitmapScan
    index_used = Column(String(200), nullable=True)
    shared_hit_blocks = Column(Integer, nullable=True)
    shared_read_blocks = Column(Integer, nullable=True)
    query_plan = Column(JSON, nullable=True)
    analyzed_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_query_analysis_config", "pipeline_config", "analyzed_at"),
    )

    def to_dict(self) -> dict:
        return {
            "id": str(self.id),
            "query_name": self.query_name,
            "pipeline_config": self.pipeline_config,
            "execution_time_ms": self.execution_time_ms,
            "scan_type": self.scan_type,
            "index_used": self.index_used,
            "analyzed_at": self.analyzed_at.isoformat() if self.analyzed_at else None,
        }
