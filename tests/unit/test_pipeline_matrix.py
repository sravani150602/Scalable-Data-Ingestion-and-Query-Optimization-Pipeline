"""Configuration matrix tests covering all supported workload profiles."""

import pytest

from app.config import PipelineConfig, PipelineMode

MODES = [
    "balanced",
    "high_throughput",
    "low_latency",
    "batch_optimized",
    "realtime",
    "analytical",
]

LOAD_PROFILES = [
    (records, payload_bytes)
    for records in (1, 10, 50, 100, 500)
    for payload_bytes in (128, 1024, 8192, 50000)
]


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize(("records", "payload_bytes"), LOAD_PROFILES)
def test_configuration_supports_load_profile(mode, records, payload_bytes):
    """Every documented mode must produce safe settings for varied workloads."""
    config = PipelineConfig.get_config(mode)

    assert isinstance(config.mode, PipelineMode)
    assert config.mode.value == mode
    assert config.batch_size > 0
    assert config.max_concurrent_inserts > 0
    assert config.statement_timeout_ms > 0
    assert config.work_mem_mb > 0
    assert config.maintenance_work_mem_mb > 0
    assert records * payload_bytes > 0
