"""Unit tests for configuration."""

from app.config import AppConfig, DatabaseConfig, PipelineConfig, PipelineMode


class TestDatabaseConfig:
    def test_defaults(self):
        config = DatabaseConfig()
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "ingestion_pipeline"

    def test_url_format(self):
        config = DatabaseConfig(
            host="db.example.com",
            port=5432,
            database="mydb",
            user="admin",
            password="secret",
        )
        assert config.url == "postgresql://admin:secret@db.example.com:5432/mydb"

    def test_async_url_format(self):
        config = DatabaseConfig(
            host="db.example.com",
            port=5432,
            database="mydb",
            user="admin",
            password="secret",
        )
        assert (
            config.async_url
            == "postgresql+asyncpg://admin:secret@db.example.com:5432/mydb"
        )


class TestPipelineConfig:
    def test_balanced_config(self):
        config = PipelineConfig.get_config("balanced")
        assert config.mode == PipelineMode.BALANCED
        assert config.batch_size == 1000

    def test_high_throughput_config(self):
        config = PipelineConfig.get_config("high_throughput")
        assert config.batch_size == 5000
        assert config.max_concurrent_inserts == 20

    def test_low_latency_config(self):
        config = PipelineConfig.get_config("low_latency")
        assert config.batch_size == 100
        assert config.enable_composite_indexes is True

    def test_batch_optimized_config(self):
        config = PipelineConfig.get_config("batch_optimized")
        assert config.batch_size == 10000
        assert config.maintenance_work_mem_mb == 512

    def test_realtime_config(self):
        config = PipelineConfig.get_config("realtime")
        assert config.batch_size == 50
        assert config.statement_timeout_ms == 5000

    def test_analytical_config(self):
        config = PipelineConfig.get_config("analytical")
        assert config.index_strategy == "covering"
        assert config.work_mem_mb == 256

    def test_invalid_config_returns_balanced(self):
        config = PipelineConfig.get_config("nonexistent")
        assert config.mode == PipelineMode.BALANCED

    def test_all_six_configs_exist(self):
        configs = [
            "balanced",
            "high_throughput",
            "low_latency",
            "batch_optimized",
            "realtime",
            "analytical",
        ]
        for name in configs:
            config = PipelineConfig.get_config(name)
            assert config is not None
            assert config.batch_size > 0


class TestAppConfig:
    def test_defaults(self):
        config = AppConfig()
        assert config.app_name == "Data Ingestion Pipeline"
        assert config.version == "1.0.0"
        assert config.port == 8000
