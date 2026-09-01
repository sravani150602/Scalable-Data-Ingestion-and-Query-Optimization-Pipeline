"""Integration tests for FastAPI endpoints."""

import uuid


class TestHealthEndpoint:
    def test_health_check(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] in ("healthy", "degraded")
        assert "version" in data

    def test_root(self, client):
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "service" in data
        assert "docs" in data


class TestRecordIngestion:
    def test_create_single_record(self, client):
        payload = {
            "source_id": "test-api-001",
            "source_type": "api",
            "category": "transactions",
            "payload": {"amount": 100},
            "priority": 5,
            "size_bytes": 512,
            "region": "us-east-1",
            "tags": ["test"],
        }
        response = client.post("/api/v1/records/", json=payload)
        assert response.status_code == 201
        data = response.json()
        assert data["source_id"] == "test-api-001"
        assert data["status"] == "completed"
        assert data["processed"] is True

    def test_create_record_minimal(self, client):
        payload = {
            "source_id": "min-001",
            "source_type": "webhook",
            "category": "events",
        }
        response = client.post("/api/v1/records/", json=payload)
        assert response.status_code == 201

    def test_create_record_invalid_priority(self, client):
        payload = {
            "source_id": "bad-001",
            "source_type": "api",
            "category": "test",
            "priority": 99,
        }
        response = client.post("/api/v1/records/", json=payload)
        assert response.status_code == 422

    def test_create_record_missing_required(self, client):
        response = client.post("/api/v1/records/", json={"source_type": "api"})
        assert response.status_code == 422

    def test_batch_ingestion(self, client):
        records = [
            {
                "source_id": f"batch-{i}",
                "source_type": "api",
                "category": "test",
                "payload": {"i": i},
                "priority": i % 5,
                "size_bytes": 100,
            }
            for i in range(50)
        ]
        response = client.post(
            "/api/v1/records/batch",
            json={
                "records": records,
                "pipeline_config": "balanced",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_records"] == 50
        assert data["ingested"] == 50
        assert data["failed"] == 0
        assert data["pipeline_config"] == "balanced"
        assert "pipeline_run_id" in data

    def test_batch_ingestion_high_throughput(self, client):
        records = [
            {
                "source_id": f"ht-{i}",
                "source_type": "stream",
                "category": "analytics",
                "payload": {},
                "priority": 0,
                "size_bytes": 200,
            }
            for i in range(30)
        ]
        response = client.post(
            "/api/v1/records/batch",
            json={
                "records": records,
                "pipeline_config": "high_throughput",
            },
        )
        assert response.status_code == 200
        assert response.json()["ingested"] == 30

    def test_batch_invalid_config(self, client):
        records = [{"source_id": "x", "source_type": "api", "category": "test"}]
        response = client.post(
            "/api/v1/records/batch",
            json={
                "records": records,
                "pipeline_config": "invalid",
            },
        )
        assert response.status_code == 422

    def test_batch_empty_rejected(self, client):
        response = client.post(
            "/api/v1/records/batch",
            json={
                "records": [],
                "pipeline_config": "balanced",
            },
        )
        assert response.status_code == 422


class TestRecordQuerying:
    def _seed_records(self, client, count=20):
        records = [
            {
                "source_id": f"q-{i}",
                "source_type": "api" if i < 10 else "webhook",
                "category": "transactions" if i < 15 else "logs",
                "payload": {},
                "priority": i % 5,
                "size_bytes": 100,
                "region": "us-east-1" if i < 12 else "eu-west-1",
            }
            for i in range(count)
        ]
        client.post(
            "/api/v1/records/batch",
            json={
                "records": records,
                "pipeline_config": "balanced",
            },
        )

    def test_list_all_records(self, client):
        self._seed_records(client)
        response = client.get("/api/v1/records/")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 20

    def test_filter_by_source_type(self, client):
        self._seed_records(client)
        response = client.get("/api/v1/records/?source_type=api")
        assert response.status_code == 200
        data = response.json()
        assert all(r["source_type"] == "api" for r in data)
        assert len(data) == 10

    def test_filter_by_category(self, client):
        self._seed_records(client)
        response = client.get("/api/v1/records/?category=logs")
        assert response.status_code == 200
        data = response.json()
        assert all(r["category"] == "logs" for r in data)

    def test_filter_by_region(self, client):
        self._seed_records(client)
        response = client.get("/api/v1/records/?region=us-east-1")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 12

    def test_filter_by_status(self, client):
        self._seed_records(client)
        response = client.get("/api/v1/records/?status=completed")
        assert response.status_code == 200
        assert len(response.json()) == 20

    def test_pagination(self, client):
        self._seed_records(client)
        page1 = client.get("/api/v1/records/?limit=5&offset=0").json()
        page2 = client.get("/api/v1/records/?limit=5&offset=5").json()
        assert len(page1) == 5
        assert len(page2) == 5
        assert page1[0]["id"] != page2[0]["id"]

    def test_order_by_priority(self, client):
        self._seed_records(client)
        response = client.get("/api/v1/records/?order_by=priority&order_dir=desc")
        assert response.status_code == 200
        data = response.json()
        priorities = [r["priority"] for r in data]
        assert priorities == sorted(priorities, reverse=True)

    def test_get_specific_record(self, client):
        # Create a record first
        payload = {
            "source_id": "specific-001",
            "source_type": "api",
            "category": "test",
        }
        create_resp = client.post("/api/v1/records/", json=payload)
        record_id = create_resp.json()["id"]

        response = client.get(f"/api/v1/records/{record_id}")
        assert response.status_code == 200
        assert response.json()["source_id"] == "specific-001"

    def test_get_nonexistent_record(self, client):
        fake_id = str(uuid.uuid4())
        response = client.get(f"/api/v1/records/{fake_id}")
        assert response.status_code == 404

    def test_stats_summary(self, client):
        self._seed_records(client)
        response = client.get("/api/v1/records/stats/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["total_records"] == 20
        assert "records_by_status" in data


class TestResponseTiming:
    def test_response_has_timing_header(self, client):
        payload = {"source_id": "timing-001", "source_type": "api", "category": "test"}
        response = client.post("/api/v1/records/", json=payload)
        assert "x-response-time" in response.headers

    def test_single_record_latency(self, client):
        """Verify single record operations are fast."""
        import time

        payload = {"source_id": "lat-001", "source_type": "api", "category": "test"}
        start = time.time()
        response = client.post("/api/v1/records/", json=payload)
        duration_ms = (time.time() - start) * 1000

        assert response.status_code == 201
        assert duration_ms < 500  # Should be well under 500ms locally
