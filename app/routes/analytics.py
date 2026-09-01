"""API routes for query optimization and analytics."""

import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.services.ingestion_service import QueryOptimizer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/analytics", tags=["analytics"])


def get_db():
    raise NotImplementedError("Database dependency not configured")


def get_query_optimizer():
    raise NotImplementedError("Optimizer dependency not configured")


@router.post("/benchmark/{config_name}")
def run_benchmark(
    config_name: str,
    db: Session = Depends(get_db),
    optimizer: QueryOptimizer = Depends(get_query_optimizer),
):
    """Run benchmark queries for a specific pipeline configuration."""
    valid_configs = [
        "balanced",
        "high_throughput",
        "low_latency",
        "batch_optimized",
        "realtime",
        "analytical",
    ]
    if config_name not in valid_configs:
        raise HTTPException(400, f"Invalid config. Must be one of: {valid_configs}")

    try:
        results = optimizer.run_benchmark(db, config_name)
        db.commit()
        return {
            "config": config_name,
            "queries_analyzed": len(results),
            "results": [
                {
                    "query_name": r.query_name,
                    "execution_time_ms": r.execution_time_ms,
                    "scan_type": r.scan_type,
                    "index_used": r.index_used,
                }
                for r in results
            ],
        }
    except Exception as e:
        db.rollback()
        logger.error(f"Benchmark failed: {e}")
        raise HTTPException(500, str(e))


@router.get("/compare")
def compare_configurations(
    db: Session = Depends(get_db),
    optimizer: QueryOptimizer = Depends(get_query_optimizer),
):
    """Compare query performance across all pipeline configurations."""
    try:
        comparison = optimizer.compare_configurations(db)
        return {"comparison": comparison}
    except Exception as e:
        logger.error(f"Comparison failed: {e}")
        raise HTTPException(500, str(e))


@router.get("/index-usage")
def get_index_usage(
    db: Session = Depends(get_db),
):
    """Get index usage statistics for the ingestion_records table."""
    from sqlalchemy import text

    try:
        result = db.execute(text("""
            SELECT
                indexrelname AS index_name,
                idx_scan AS scans,
                idx_tup_read AS tuples_read,
                idx_tup_fetch AS tuples_fetched,
                pg_size_pretty(pg_relation_size(indexrelid)) AS size
            FROM pg_stat_user_indexes
            WHERE relname = 'ingestion_records'
            ORDER BY idx_scan DESC
        """))
        indexes = [dict(row._mapping) for row in result.fetchall()]
        return {"table": "ingestion_records", "indexes": indexes}
    except Exception as e:
        logger.error(f"Failed to get index usage: {e}")
        raise HTTPException(500, str(e))


@router.get("/table-stats")
def get_table_stats(
    db: Session = Depends(get_db),
):
    """Get table statistics from PostgreSQL."""
    from sqlalchemy import text

    try:
        result = db.execute(text("""
            SELECT
                n_live_tup AS row_count,
                n_dead_tup AS dead_rows,
                seq_scan AS sequential_scans,
                idx_scan AS index_scans,
                n_tup_ins AS inserts,
                pg_size_pretty(pg_total_relation_size('ingestion_records')) AS total_size,
                pg_size_pretty(pg_indexes_size('ingestion_records')) AS index_size
            FROM pg_stat_user_tables
            WHERE relname = 'ingestion_records'
        """))
        row = result.fetchone()
        if row:
            return dict(row._mapping)
        return {"error": "Table not found"}
    except Exception as e:
        logger.error(f"Failed to get table stats: {e}")
        raise HTTPException(500, str(e))
