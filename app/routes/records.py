"""API routes for record ingestion and querying."""

import logging
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.models.schemas import (
    BatchIngestionResponse,
    PipelineStatsResponse,
    RecordBatchCreate,
    RecordCreate,
    RecordResponse,
)
from app.services.ingestion_service import IngestionService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/records", tags=["records"])


def get_db():
    """Dependency placeholder — overridden in main.py."""
    raise NotImplementedError("Database dependency not configured")


def get_ingestion_service():
    """Dependency placeholder — overridden in main.py."""
    raise NotImplementedError("Service dependency not configured")


@router.post("/", response_model=RecordResponse, status_code=201)
def create_record(
    record_data: RecordCreate,
    db: Session = Depends(get_db),
    service: IngestionService = Depends(get_ingestion_service),
):
    """Ingest a single record."""
    try:
        record = service.ingest_single(db, record_data.model_dump())
        db.commit()
        return RecordResponse.model_validate(record)
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to ingest record: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch", response_model=BatchIngestionResponse)
def create_batch(
    batch_data: RecordBatchCreate,
    db: Session = Depends(get_db),
    service: IngestionService = Depends(get_ingestion_service),
):
    """Ingest a batch of records using the specified pipeline configuration."""
    try:
        records_dicts = [r.model_dump() for r in batch_data.records]
        pipeline_run, latencies = service.ingest_batch(
            db, records_dicts, batch_data.pipeline_config
        )
        db.commit()

        latencies.sort()
        return BatchIngestionResponse(
            total_records=len(records_dicts),
            ingested=pipeline_run.records_ingested,
            failed=pipeline_run.records_failed,
            avg_latency_ms=pipeline_run.avg_latency_ms or 0,
            p95_latency_ms=pipeline_run.p95_latency_ms or 0,
            pipeline_config=batch_data.pipeline_config,
            pipeline_run_id=pipeline_run.id,
        )
    except Exception as e:
        db.rollback()
        logger.error(f"Batch ingestion failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=List[RecordResponse])
def list_records(
    source_type: Optional[str] = None,
    category: Optional[str] = None,
    status: Optional[str] = None,
    region: Optional[str] = None,
    processed: Optional[bool] = None,
    priority_min: Optional[int] = None,
    priority_max: Optional[int] = None,
    created_after: Optional[datetime] = None,
    created_before: Optional[datetime] = None,
    limit: int = Query(default=100, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
    order_by: str = Query(default="created_at"),
    order_dir: str = Query(default="desc"),
    db: Session = Depends(get_db),
    service: IngestionService = Depends(get_ingestion_service),
):
    """Query records with filters optimized by composite indexes."""
    filters = {
        "source_type": source_type,
        "category": category,
        "status": status,
        "region": region,
        "processed": processed,
        "priority_min": priority_min,
        "priority_max": priority_max,
        "created_after": created_after,
        "created_before": created_before,
        "order_by": order_by,
        "order_dir": order_dir,
    }
    records = service.query_records(db, filters, limit=limit, offset=offset)
    return [RecordResponse.model_validate(r) for r in records]


@router.get("/{record_id}", response_model=RecordResponse)
def get_record(
    record_id: UUID,
    db: Session = Depends(get_db),
):
    """Get a specific record by ID."""
    from app.models.record import IngestionRecord

    record = db.query(IngestionRecord).filter(IngestionRecord.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")
    return RecordResponse.model_validate(record)


@router.get("/stats/summary", response_model=PipelineStatsResponse)
def get_stats(
    db: Session = Depends(get_db),
    service: IngestionService = Depends(get_ingestion_service),
):
    """Get pipeline statistics."""
    stats = service.get_stats(db)
    return PipelineStatsResponse(**stats)
