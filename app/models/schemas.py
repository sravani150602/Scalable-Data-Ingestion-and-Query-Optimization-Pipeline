"""Pydantic schemas for API request/response validation."""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class RecordCreate(BaseModel):
    """Schema for creating a new ingestion record."""

    source_id: str = Field(..., max_length=100)
    source_type: str = Field(..., max_length=50)
    category: str = Field(..., max_length=100)
    subcategory: Optional[str] = Field(None, max_length=100)
    payload: Dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=0, ge=0, le=10)
    size_bytes: int = Field(default=0, ge=0)
    region: Optional[str] = Field(None, max_length=50)
    tags: Optional[List[str]] = Field(default_factory=list)
    metadata_extra: Optional[Dict[str, Any]] = Field(default_factory=dict)


class RecordBatchCreate(BaseModel):
    """Schema for batch record ingestion."""

    records: List[RecordCreate] = Field(..., min_length=1, max_length=10000)
    pipeline_config: str = Field(default="balanced")

    @field_validator("pipeline_config")
    @classmethod
    def validate_config(cls, v):
        valid = [
            "balanced",
            "high_throughput",
            "low_latency",
            "batch_optimized",
            "realtime",
            "analytical",
        ]
        if v not in valid:
            raise ValueError(f"Invalid pipeline config. Must be one of: {valid}")
        return v


class RecordResponse(BaseModel):
    """Schema for a single record response."""

    id: UUID
    source_id: str
    source_type: str
    category: str
    subcategory: Optional[str] = None
    status: str
    priority: int
    size_bytes: int
    processed: bool
    processing_time_ms: Optional[float] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    region: Optional[str] = None
    tags: Optional[List[str]] = None
    retry_count: int = 0

    class Config:
        from_attributes = True


class BatchIngestionResponse(BaseModel):
    """Response for batch ingestion operations."""

    total_records: int
    ingested: int
    failed: int
    avg_latency_ms: float
    p95_latency_ms: float
    pipeline_config: str
    pipeline_run_id: UUID


class RecordQuery(BaseModel):
    """Query parameters for filtering records."""

    source_type: Optional[str] = None
    category: Optional[str] = None
    status: Optional[str] = None
    region: Optional[str] = None
    processed: Optional[bool] = None
    priority_min: Optional[int] = None
    priority_max: Optional[int] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None
    limit: int = Field(default=100, ge=1, le=10000)
    offset: int = Field(default=0, ge=0)
    order_by: str = Field(default="created_at")
    order_dir: str = Field(default="desc")


class QueryPlanResponse(BaseModel):
    """Response containing query execution plan analysis."""

    query_name: str
    execution_time_ms: float
    planning_time_ms: Optional[float] = None
    rows_returned: Optional[int] = None
    scan_type: Optional[str] = None
    index_used: Optional[str] = None
    shared_hit_blocks: Optional[int] = None
    shared_read_blocks: Optional[int] = None
    plan: Optional[Dict[str, Any]] = None


class PipelineStatsResponse(BaseModel):
    """Pipeline performance statistics."""

    total_records: int
    records_by_status: Dict[str, int]
    avg_processing_time_ms: Optional[float] = None
    table_size: Optional[str] = None
    index_size: Optional[str] = None
    sequential_scans: Optional[int] = None
    index_scans: Optional[int] = None


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    database: str
    version: str
    timestamp: datetime


class QueryOptimizationReport(BaseModel):
    """Report comparing query performance across pipeline configurations."""

    query_name: str
    configurations: Dict[str, Dict[str, Any]]
    best_config: str
    latency_improvement_pct: float
