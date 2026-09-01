"""
Simulation script for load testing the data ingestion pipeline.

Generates and ingests 50K+ records across 6 pipeline configurations,
then runs query benchmarks to measure and compare performance.
"""

import time
import uuid
import random
import json
import argparse
import logging
import statistics
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SOURCE_TYPES = ["api", "webhook", "file_upload", "stream", "batch_import"]
CATEGORIES = ["transactions", "user_events", "system_logs", "analytics", "notifications"]
SUBCATEGORIES = ["payment", "login", "error", "click", "alert", "signup", "purchase", "view"]
REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]


def generate_record() -> dict:
    """Generate a random ingestion record."""
    return {
        "source_id": f"src-{uuid.uuid4().hex[:8]}",
        "source_type": random.choice(SOURCE_TYPES),
        "category": random.choice(CATEGORIES),
        "subcategory": random.choice(SUBCATEGORIES),
        "payload": {
            "value": random.uniform(0, 10000),
            "user_id": f"user-{random.randint(1, 10000)}",
            "session_id": uuid.uuid4().hex[:16],
            "action": random.choice(["create", "read", "update", "delete"]),
        },
        "priority": random.randint(0, 10),
        "size_bytes": random.randint(100, 50000),
        "region": random.choice(REGIONS),
        "tags": random.sample(["important", "audit", "debug", "production", "test"], k=random.randint(1, 3)),
        "metadata_extra": {"version": "1.0", "env": random.choice(["prod", "staging", "dev"])},
    }


def send_batch(base_url: str, records: list, config: str) -> dict:
    """Send a batch of records to the API."""
    start = time.time()
    response = requests.post(
        f"{base_url}/api/v1/records/batch",
        json={"records": records, "pipeline_config": config},
        timeout=120,
    )
    duration = (time.time() - start) * 1000

    if response.status_code == 200:
        data = response.json()
        data["request_duration_ms"] = duration
        return data
    else:
        return {"error": response.text, "status_code": response.status_code, "request_duration_ms": duration}


def run_simulation(base_url: str, total_records: int, concurrency: int):
    """Run the full ingestion simulation."""
    configs = ["balanced", "high_throughput", "low_latency", "batch_optimized", "realtime", "analytical"]
    records_per_config = total_records // len(configs)
    batch_size = 500

    all_results = {}

    for config_name in configs:
        logger.info(f"\n{'='*60}")
        logger.info(f"Pipeline Configuration: {config_name}")
        logger.info(f"{'='*60}")

        records = [generate_record() for _ in range(records_per_config)]
        batches = [records[i:i+batch_size] for i in range(0, len(records), batch_size)]

        config_start = time.time()
        batch_results = []

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {
                executor.submit(send_batch, base_url, batch, config_name): i
                for i, batch in enumerate(batches)
            }

            for future in as_completed(futures):
                result = future.result()
                batch_results.append(result)
                if len(batch_results) % 5 == 0:
                    logger.info(f"  Progress: {len(batch_results)}/{len(batches)} batches")

        config_duration = time.time() - config_start

        # Aggregate results
        total_ingested = sum(r.get("ingested", 0) for r in batch_results if "error" not in r)
        total_failed = sum(r.get("failed", 0) for r in batch_results if "error" not in r)
        latencies = [r.get("avg_latency_ms", 0) for r in batch_results if "error" not in r and r.get("avg_latency_ms")]
        p95_latencies = [r.get("p95_latency_ms", 0) for r in batch_results if "error" not in r and r.get("p95_latency_ms")]

        config_result = {
            "records_sent": records_per_config,
            "records_ingested": total_ingested,
            "records_failed": total_failed,
            "total_time_seconds": round(config_duration, 2),
            "throughput_rps": round(total_ingested / config_duration, 2) if config_duration > 0 else 0,
        }

        if latencies:
            config_result["avg_latency_ms"] = round(statistics.mean(latencies), 2)
        if p95_latencies:
            config_result["p95_latency_ms"] = round(statistics.mean(p95_latencies), 2)

        all_results[config_name] = config_result
        logger.info(f"  Result: {json.dumps(config_result, indent=2)}")

    # Run query benchmarks
    logger.info(f"\n{'='*60}")
    logger.info("Running Query Benchmarks")
    logger.info(f"{'='*60}")

    for config_name in configs:
        try:
            response = requests.post(f"{base_url}/api/v1/analytics/benchmark/{config_name}", timeout=60)
            if response.status_code == 200:
                logger.info(f"  {config_name}: {json.dumps(response.json(), indent=2)}")
        except Exception as e:
            logger.error(f"  Benchmark failed for {config_name}: {e}")

    # Compare configurations
    try:
        response = requests.get(f"{base_url}/api/v1/analytics/compare", timeout=30)
        if response.status_code == 200:
            logger.info(f"\nConfiguration Comparison:")
            logger.info(json.dumps(response.json(), indent=2))
    except Exception as e:
        logger.error(f"Comparison failed: {e}")

    # Print summary
    print("\n" + "=" * 60)
    print("SIMULATION SUMMARY")
    print("=" * 60)
    print(json.dumps(all_results, indent=2))

    total_processed = sum(r["records_ingested"] for r in all_results.values())
    print(f"\nTotal records processed: {total_processed}")
    print(f"Target: 50,000+ {'✓ PASS' if total_processed >= 50000 else '✗ FAIL'}")

    return all_results


def run_concurrent_test(base_url: str, concurrency: int = 150):
    """Test concurrent request handling at 150 concurrent requests."""
    logger.info(f"\nConcurrent Request Test ({concurrency} concurrent)")

    records = [generate_record() for _ in range(concurrency)]
    latencies = []

    def send_single(record):
        start = time.time()
        response = requests.post(
            f"{base_url}/api/v1/records/",
            json=record,
            timeout=30,
        )
        return (time.time() - start) * 1000, response.status_code

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(send_single, r) for r in records]
        for future in as_completed(futures):
            latency, status = future.result()
            latencies.append(latency)

    latencies.sort()
    p95 = latencies[int(len(latencies) * 0.95)]
    print(f"\nConcurrent Test Results ({concurrency} requests):")
    print(f"  Avg: {statistics.mean(latencies):.2f}ms")
    print(f"  p95: {p95:.2f}ms {'✓ PASS' if p95 < 60 else '✗ FAIL'} (target: <60ms)")
    print(f"  Max: {max(latencies):.2f}ms")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate data ingestion pipeline")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--total-records", type=int, default=54000)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--concurrent-test", action="store_true", help="Run concurrent request test")
    args = parser.parse_args()

    if args.concurrent_test:
        run_concurrent_test(args.base_url, concurrency=150)
    else:
        run_simulation(args.base_url, args.total_records, args.concurrency)
