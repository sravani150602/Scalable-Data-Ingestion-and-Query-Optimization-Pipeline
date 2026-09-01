"""Application configuration with support for multiple pipeline configurations."""

import os
from dataclasses import dataclass, field
from enum import Enum


class PipelineMode(Enum):
    """Pipeline configuration modes for different optimization strategies."""

    BALANCED = "balanced"
    HIGH_THROUGHPUT = "high_throughput"
    LOW_LATENCY = "low_latency"
    BATCH_OPTIMIZED = "batch_optimized"
    REALTIME = "realtime"
    ANALYTICAL = "analytical"


@dataclass
class DatabaseConfig:
    """PostgreSQL connection configuration."""

    host: str = os.environ.get("DB_HOST", "localhost")
    port: int = int(os.environ.get("DB_PORT", "5432"))
    database: str = os.environ.get("DB_NAME", "ingestion_pipeline")
    user: str = os.environ.get("DB_USER", "postgres")
    password: str = os.environ.get("DB_PASSWORD", "postgres")
    min_pool_size: int = int(os.environ.get("DB_MIN_POOL", "5"))
    max_pool_size: int = int(os.environ.get("DB_MAX_POOL", "20"))

    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def async_url(self) -> str:
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class PipelineConfig:
    """Configuration for a specific pipeline mode."""

    mode: PipelineMode = PipelineMode.BALANCED
    batch_size: int = 1000
    max_concurrent_inserts: int = 10
    enable_composite_indexes: bool = True
    enable_query_plan_analysis: bool = True
    index_strategy: str = "composite"  # composite, partial, covering, btree
    vacuum_after_bulk_insert: bool = True
    statement_timeout_ms: int = 30000
    work_mem_mb: int = 64
    maintenance_work_mem_mb: int = 256

    @classmethod
    def get_config(cls, mode: str) -> "PipelineConfig":
        """Get a predefined pipeline configuration."""
        configs = {
            "balanced": cls(
                mode=PipelineMode.BALANCED,
                batch_size=1000,
                max_concurrent_inserts=10,
            ),
            "high_throughput": cls(
                mode=PipelineMode.HIGH_THROUGHPUT,
                batch_size=5000,
                max_concurrent_inserts=20,
                vacuum_after_bulk_insert=True,
            ),
            "low_latency": cls(
                mode=PipelineMode.LOW_LATENCY,
                batch_size=100,
                max_concurrent_inserts=5,
                enable_composite_indexes=True,
                work_mem_mb=128,
            ),
            "batch_optimized": cls(
                mode=PipelineMode.BATCH_OPTIMIZED,
                batch_size=10000,
                max_concurrent_inserts=25,
                vacuum_after_bulk_insert=True,
                maintenance_work_mem_mb=512,
            ),
            "realtime": cls(
                mode=PipelineMode.REALTIME,
                batch_size=50,
                max_concurrent_inserts=3,
                enable_composite_indexes=True,
                statement_timeout_ms=5000,
            ),
            "analytical": cls(
                mode=PipelineMode.ANALYTICAL,
                batch_size=2000,
                max_concurrent_inserts=8,
                index_strategy="covering",
                enable_query_plan_analysis=True,
                work_mem_mb=256,
            ),
        }
        return configs.get(mode, configs["balanced"])


@dataclass
class AppConfig:
    """Top-level application configuration."""

    app_name: str = "Data Ingestion Pipeline"
    version: str = "1.0.0"
    debug: bool = os.environ.get("DEBUG", "false").lower() == "true"
    host: str = os.environ.get("APP_HOST", "0.0.0.0")
    port: int = int(os.environ.get("APP_PORT", "8000"))
    log_level: str = os.environ.get("LOG_LEVEL", "INFO")
    db: DatabaseConfig = field(default_factory=DatabaseConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)
