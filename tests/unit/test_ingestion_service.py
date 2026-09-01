"""Unit tests for the ingestion service."""

from app.config import PipelineConfig
from app.services.ingestion_service import IngestionService


class TestIngestionService:
    def test_ingest_single_record(self, test_session, sample_record):
        service = IngestionService(None, PipelineConfig())
        record = service.ingest_single(test_session, sample_record)
        test_session.commit()

        assert record.id is not None
        assert record.source_id == "test-src-001"
        assert record.status == "completed"
        assert record.processed is True
        assert record.processing_time_ms is not None
        assert record.checksum is not None

    def test_ingest_single_sets_checksum(self, test_session, sample_record):
        service = IngestionService(None, PipelineConfig())
        record = service.ingest_single(test_session, sample_record)
        test_session.commit()

        assert record.checksum is not None
        assert len(record.checksum) == 64

    def test_ingest_single_processing_time(self, test_session, sample_record):
        service = IngestionService(None, PipelineConfig())
        record = service.ingest_single(test_session, sample_record)
        test_session.commit()

        assert record.processing_time_ms >= 0
        assert record.processed_at is not None

    def test_ingest_batch_balanced(self, test_session, sample_records):
        service = IngestionService(None, PipelineConfig())
        run, latencies = service.ingest_batch(test_session, sample_records, "balanced")
        test_session.commit()

        assert run.records_ingested == 100
        assert run.records_failed == 0
        assert run.status == "completed"
        assert run.avg_latency_ms is not None
        assert len(latencies) == 100

    def test_ingest_batch_high_throughput(self, test_session, sample_records):
        service = IngestionService(None, PipelineConfig())
        run, latencies = service.ingest_batch(
            test_session, sample_records, "high_throughput"
        )
        test_session.commit()

        assert run.records_ingested == 100
        assert run.pipeline_config == "high_throughput"

    def test_ingest_batch_low_latency(self, test_session, sample_records):
        service = IngestionService(None, PipelineConfig())
        run, latencies = service.ingest_batch(
            test_session, sample_records, "low_latency"
        )
        test_session.commit()

        assert run.records_ingested == 100

    def test_ingest_batch_all_configs(self, test_session):
        """Test all 6 pipeline configurations."""
        configs = [
            "balanced",
            "high_throughput",
            "low_latency",
            "batch_optimized",
            "realtime",
            "analytical",
        ]
        service = IngestionService(None, PipelineConfig())

        for config_name in configs:
            records = [
                {
                    "source_id": f"test-{config_name}-{i}",
                    "source_type": "api",
                    "category": "test",
                    "payload": {"config": config_name, "i": i},
                    "priority": 1,
                    "size_bytes": 100,
                }
                for i in range(20)
            ]
            run, latencies = service.ingest_batch(test_session, records, config_name)
            test_session.commit()

            assert run.records_ingested == 20, f"Failed for config: {config_name}"
            assert run.pipeline_config == config_name

    def test_query_records_no_filter(self, test_session, sample_records):
        service = IngestionService(None, PipelineConfig())
        service.ingest_batch(test_session, sample_records, "balanced")
        test_session.commit()

        results = service.query_records(test_session, {}, limit=50)
        assert len(results) == 50

    def test_query_records_by_source_type(self, test_session):
        service = IngestionService(None, PipelineConfig())
        records = [
            {
                "source_id": f"s{i}",
                "source_type": "api" if i < 5 else "webhook",
                "category": "test",
                "payload": {},
                "priority": 0,
                "size_bytes": 0,
            }
            for i in range(10)
        ]
        service.ingest_batch(test_session, records, "balanced")
        test_session.commit()

        results = service.query_records(test_session, {"source_type": "api"})
        assert len(results) == 5

    def test_query_records_by_status(self, test_session, sample_records):
        service = IngestionService(None, PipelineConfig())
        service.ingest_batch(test_session, sample_records, "balanced")
        test_session.commit()

        results = service.query_records(test_session, {"status": "completed"})
        assert len(results) == 100

    def test_query_records_by_category(self, test_session):
        service = IngestionService(None, PipelineConfig())
        records = [
            {
                "source_id": f"s{i}",
                "source_type": "api",
                "category": "cat_a" if i < 3 else "cat_b",
                "payload": {},
                "priority": 0,
                "size_bytes": 0,
            }
            for i in range(6)
        ]
        service.ingest_batch(test_session, records, "balanced")
        test_session.commit()

        results = service.query_records(test_session, {"category": "cat_a"})
        assert len(results) == 3

    def test_query_records_ordering(self, test_session):
        service = IngestionService(None, PipelineConfig())
        records = [
            {
                "source_id": f"s{i}",
                "source_type": "api",
                "category": "test",
                "payload": {},
                "priority": i,
                "size_bytes": 0,
            }
            for i in range(5)
        ]
        service.ingest_batch(test_session, records, "balanced")
        test_session.commit()

        results = service.query_records(
            test_session, {"order_by": "priority", "order_dir": "desc"}
        )
        priorities = [r.priority for r in results]
        assert priorities == sorted(priorities, reverse=True)

    def test_query_with_pagination(self, test_session, sample_records):
        service = IngestionService(None, PipelineConfig())
        service.ingest_batch(test_session, sample_records, "balanced")
        test_session.commit()

        page1 = service.query_records(test_session, {}, limit=10, offset=0)
        page2 = service.query_records(test_session, {}, limit=10, offset=10)

        assert len(page1) == 10
        assert len(page2) == 10
        assert page1[0].id != page2[0].id

    def test_get_record_count(self, test_session, sample_records):
        service = IngestionService(None, PipelineConfig())
        service.ingest_batch(test_session, sample_records, "balanced")
        test_session.commit()

        total = service.get_record_count(test_session)
        assert total == 100

        completed = service.get_record_count(test_session, status="completed")
        assert completed == 100

    def test_get_stats(self, test_session, sample_records):
        service = IngestionService(None, PipelineConfig())
        service.ingest_batch(test_session, sample_records, "balanced")
        test_session.commit()

        stats = service.get_stats(test_session)
        assert stats["total_records"] == 100
        assert "records_by_status" in stats
        assert stats["records_by_status"]["completed"] == 100
