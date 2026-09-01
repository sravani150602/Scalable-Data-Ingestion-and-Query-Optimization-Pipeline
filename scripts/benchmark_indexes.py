"""Measure query latency before and after applying the composite indexes."""

import argparse
import json
import statistics
import time

from sqlalchemy import create_engine, text

from app.config import DatabaseConfig
from app.services.ingestion_service import QueryOptimizer

INDEX_DEFINITIONS = {
    "ix_source_status_created": "(source_type, status, created_at DESC)",
    "ix_category_created": "(category, created_at DESC)",
    "ix_status_priority": "(status, priority DESC, created_at)",
    "ix_source_category": "(source_type, category)",
    "ix_region_status": "(region, status, created_at DESC)",
    "ix_processed_updated": "(processed, updated_at)",
}


def set_indexes(connection, enabled):
    for name, columns in INDEX_DEFINITIONS.items():
        statement = (
            f"CREATE INDEX IF NOT EXISTS {name} ON ingestion_records {columns}"
            if enabled
            else f"DROP INDEX IF EXISTS {name}"
        )
        connection.execute(text(statement))
    connection.execute(text("ANALYZE ingestion_records"))
    connection.commit()


def measure(connection, repetitions):
    samples = []
    plans = {}
    for name, query in QueryOptimizer.BENCHMARK_QUERIES.items():
        query_samples = []
        for _ in range(repetitions):
            started = time.perf_counter()
            connection.execute(text(query["sql"]), query["params"]).fetchall()
            query_samples.append((time.perf_counter() - started) * 1000)
        plan = connection.execute(
            text(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query['sql']}"),
            query["params"],
        ).scalar_one()
        samples.extend(query_samples)
        plans[name] = plan[0]["Plan"].get("Node Type")
    return {
        "mean_ms": statistics.mean(samples),
        "p95_ms": sorted(samples)[int(len(samples) * 0.95)],
        "plans": plans,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repetitions", type=int, default=10)
    parser.add_argument("--output", default="benchmark-results/index-comparison.json")
    args = parser.parse_args()
    engine = create_engine(DatabaseConfig().url)
    with engine.connect() as connection:
        set_indexes(connection, False)
        without_indexes = measure(connection, args.repetitions)
        set_indexes(connection, True)
        with_indexes = measure(connection, args.repetitions)
    improvement = (
        (without_indexes["mean_ms"] - with_indexes["mean_ms"])
        / without_indexes["mean_ms"]
        * 100
    )
    result = {
        "without_indexes": without_indexes,
        "with_indexes": with_indexes,
        "latency_reduction_pct": round(improvement, 2),
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
