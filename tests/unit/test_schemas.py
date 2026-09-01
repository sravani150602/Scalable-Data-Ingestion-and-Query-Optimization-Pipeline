"""Unit tests for Pydantic schemas."""

from datetime import datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.models.schemas import (
    BatchIngestionResponse,
    HealthResponse,
    PipelineStatsResponse,
    RecordBatchCreate,
    RecordCreate,
    RecordQuery,
    RecordResponse,
)


class TestRecordCreate:
    def test_valid_record(self):
        record = RecordCreate(
            source_id="src-001",
            source_type="api",
            category="transactions",
        )
        assert record.source_id == "src-001"
        assert record.priority == 0

    def test_with_all_fields(self):
        record = RecordCreate(
            source_id="src-002",
            source_type="webhook",
            category="events",
            subcategory="login",
            payload={"key": "value"},
            priority=8,
            size_bytes=2048,
            region="us-east-1",
            tags=["important"],
            metadata_extra={"env": "prod"},
        )
        assert record.priority == 8
        assert record.tags == ["important"]

    def test_missing_required_fields(self):
        with pytest.raises(ValidationError):
            RecordCreate(source_type="api", category="test")

    def test_priority_bounds(self):
        with pytest.raises(ValidationError):
            RecordCreate(source_id="x", source_type="api", category="test", priority=11)

        with pytest.raises(ValidationError):
            RecordCreate(source_id="x", source_type="api", category="test", priority=-1)

    def test_size_non_negative(self):
        with pytest.raises(ValidationError):
            RecordCreate(
                source_id="x", source_type="api", category="test", size_bytes=-100
            )

    def test_defaults(self):
        record = RecordCreate(source_id="x", source_type="api", category="test")
        assert record.priority == 0
        assert record.size_bytes == 0
        assert record.tags == []
        assert record.payload == {}

    def test_max_length_source_id(self):
        record = RecordCreate(source_id="a" * 100, source_type="api", category="test")
        assert len(record.source_id) == 100


class TestRecordBatchCreate:
    def test_valid_batch(self):
        records = [
            RecordCreate(source_id=f"src-{i}", source_type="api", category="test")
            for i in range(5)
        ]
        batch = RecordBatchCreate(records=records, pipeline_config="balanced")
        assert len(batch.records) == 5

    def test_empty_batch_rejected(self):
        with pytest.raises(ValidationError):
            RecordBatchCreate(records=[], pipeline_config="balanced")

    def test_invalid_config(self):
        records = [RecordCreate(source_id="x", source_type="api", category="test")]
        with pytest.raises(ValidationError):
            RecordBatchCreate(records=records, pipeline_config="invalid")

    def test_all_valid_configs(self):
        valid = [
            "balanced",
            "high_throughput",
            "low_latency",
            "batch_optimized",
            "realtime",
            "analytical",
        ]
        records = [RecordCreate(source_id="x", source_type="api", category="test")]
        for config in valid:
            batch = RecordBatchCreate(records=records, pipeline_config=config)
            assert batch.pipeline_config == config

    def test_default_config(self):
        records = [RecordCreate(source_id="x", source_type="api", category="test")]
        batch = RecordBatchCreate(records=records)
        assert batch.pipeline_config == "balanced"


class TestRecordQuery:
    def test_defaults(self):
        query = RecordQuery()
        assert query.limit == 100
        assert query.offset == 0
        assert query.order_by == "created_at"
        assert query.order_dir == "desc"

    def test_custom_params(self):
        query = RecordQuery(
            source_type="api",
            status="completed",
            limit=50,
            offset=10,
        )
        assert query.source_type == "api"
        assert query.limit == 50

    def test_limit_bounds(self):
        with pytest.raises(ValidationError):
            RecordQuery(limit=0)
        with pytest.raises(ValidationError):
            RecordQuery(limit=10001)

    def test_offset_non_negative(self):
        with pytest.raises(ValidationError):
            RecordQuery(offset=-1)


class TestRecordResponse:
    def test_from_dict(self):
        data = {
            "id": uuid4(),
            "source_id": "src-001",
            "source_type": "api",
            "category": "test",
            "status": "completed",
            "priority": 5,
            "size_bytes": 1024,
            "processed": True,
            "retry_count": 0,
        }
        response = RecordResponse(**data)
        assert response.source_id == "src-001"
        assert response.processed is True


class TestBatchIngestionResponse:
    def test_valid_response(self):
        response = BatchIngestionResponse(
            total_records=100,
            ingested=98,
            failed=2,
            avg_latency_ms=5.5,
            p95_latency_ms=12.0,
            pipeline_config="balanced",
            pipeline_run_id=uuid4(),
        )
        assert response.ingested == 98
        assert response.failed == 2


class TestHealthResponse:
    def test_valid(self):
        response = HealthResponse(
            status="healthy",
            database="connected",
            version="1.0.0",
            timestamp=datetime.utcnow(),
        )
        assert response.status == "healthy"


class TestPipelineStatsResponse:
    def test_valid(self):
        response = PipelineStatsResponse(
            total_records=50000,
            records_by_status={"completed": 49000, "failed": 1000},
            avg_processing_time_ms=5.2,
        )
        assert response.total_records == 50000
