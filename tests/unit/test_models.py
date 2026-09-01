"""Unit tests for data models."""

from app.models.record import IngestionRecord, PipelineRun, QueryAnalysis


class TestIngestionRecord:
    def test_create_record(self, test_session):
        record = IngestionRecord(
            source_id="test-001",
            source_type="api",
            category="transactions",
            payload={"amount": 100},
            status="pending",
        )
        test_session.add(record)
        test_session.commit()

        assert record.id is not None
        assert record.source_id == "test-001"
        assert record.status == "pending"
        assert record.processed is False

    def test_record_defaults(self, test_session):
        record = IngestionRecord(
            source_id="test-002",
            source_type="webhook",
            category="events",
        )
        test_session.add(record)
        test_session.commit()

        assert record.priority == 0
        assert record.size_bytes == 0
        assert record.retry_count == 0
        assert record.processed is False

    def test_record_to_dict(self, test_session):
        record = IngestionRecord(
            source_id="test-003",
            source_type="api",
            category="logs",
            priority=5,
            region="us-east-1",
        )
        test_session.add(record)
        test_session.commit()

        d = record.to_dict()
        assert d["source_id"] == "test-003"
        assert d["priority"] == 5
        assert d["region"] == "us-east-1"
        assert "id" in d

    def test_multiple_records(self, test_session):
        for i in range(10):
            record = IngestionRecord(
                source_id=f"test-{i}",
                source_type="api",
                category="transactions",
            )
            test_session.add(record)
        test_session.commit()

        count = test_session.query(IngestionRecord).count()
        assert count == 10

    def test_record_with_tags(self, test_session):
        record = IngestionRecord(
            source_id="test-tags",
            source_type="api",
            category="events",
            tags=["important", "audit"],
        )
        test_session.add(record)
        test_session.commit()

        fetched = (
            test_session.query(IngestionRecord).filter_by(source_id="test-tags").first()
        )
        assert fetched.tags == ["important", "audit"]

    def test_record_with_payload(self, test_session):
        payload = {"user_id": "u123", "action": "purchase", "amount": 49.99}
        record = IngestionRecord(
            source_id="test-payload",
            source_type="webhook",
            category="transactions",
            payload=payload,
        )
        test_session.add(record)
        test_session.commit()

        fetched = (
            test_session.query(IngestionRecord)
            .filter_by(source_id="test-payload")
            .first()
        )
        assert fetched.payload["amount"] == 49.99

    def test_filter_by_status(self, test_session):
        for status in ["pending", "completed", "failed"]:
            for _ in range(5):
                test_session.add(
                    IngestionRecord(
                        source_id=f"test-{status}",
                        source_type="api",
                        category="test",
                        status=status,
                    )
                )
        test_session.commit()

        completed = (
            test_session.query(IngestionRecord).filter_by(status="completed").count()
        )
        assert completed == 5

    def test_filter_by_source_type(self, test_session):
        for st in ["api", "webhook", "api", "stream"]:
            test_session.add(
                IngestionRecord(
                    source_id=f"test-{st}",
                    source_type=st,
                    category="test",
                )
            )
        test_session.commit()

        api_count = (
            test_session.query(IngestionRecord).filter_by(source_type="api").count()
        )
        assert api_count == 2

    def test_order_by_priority(self, test_session):
        for p in [3, 1, 5, 2, 4]:
            test_session.add(
                IngestionRecord(
                    source_id=f"test-p{p}",
                    source_type="api",
                    category="test",
                    priority=p,
                )
            )
        test_session.commit()

        records = (
            test_session.query(IngestionRecord)
            .order_by(IngestionRecord.priority.desc())
            .all()
        )
        priorities = [r.priority for r in records]
        assert priorities == [5, 4, 3, 2, 1]


class TestPipelineRun:
    def test_create_pipeline_run(self, test_session):
        run = PipelineRun(
            pipeline_config="balanced",
            records_ingested=1000,
            records_failed=5,
            avg_latency_ms=12.5,
        )
        test_session.add(run)
        test_session.commit()

        assert run.id is not None
        assert run.pipeline_config == "balanced"
        assert run.records_ingested == 1000

    def test_pipeline_run_to_dict(self, test_session):
        run = PipelineRun(
            pipeline_config="high_throughput",
            records_ingested=5000,
            status="completed",
        )
        test_session.add(run)
        test_session.commit()

        d = run.to_dict()
        assert d["pipeline_config"] == "high_throughput"
        assert d["records_ingested"] == 5000

    def test_multiple_pipeline_runs(self, test_session):
        configs = ["balanced", "high_throughput", "low_latency"]
        for c in configs:
            test_session.add(PipelineRun(pipeline_config=c))
        test_session.commit()

        count = test_session.query(PipelineRun).count()
        assert count == 3


class TestQueryAnalysis:
    def test_create_analysis(self, test_session):
        analysis = QueryAnalysis(
            query_name="test_query",
            query_text="SELECT * FROM test",
            pipeline_config="balanced",
            execution_time_ms=5.2,
            scan_type="IndexScan",
            index_used="ix_test",
        )
        test_session.add(analysis)
        test_session.commit()

        assert analysis.id is not None
        assert analysis.scan_type == "IndexScan"

    def test_analysis_to_dict(self, test_session):
        analysis = QueryAnalysis(
            query_name="benchmark",
            query_text="SELECT 1",
            pipeline_config="low_latency",
            execution_time_ms=1.5,
        )
        test_session.add(analysis)
        test_session.commit()

        d = analysis.to_dict()
        assert d["query_name"] == "benchmark"
        assert d["execution_time_ms"] == 1.5
