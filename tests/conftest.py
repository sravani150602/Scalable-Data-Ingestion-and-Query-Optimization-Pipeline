"""Pytest configuration and shared fixtures."""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.config import PipelineConfig
from app.database import Base
from app.services.ingestion_service import IngestionService

# Use SQLite for unit tests (PostgreSQL for integration)
TEST_DB_URL = "sqlite:///./test.db"


@pytest.fixture(scope="function")
def test_engine():
    """Create a test database engine."""
    engine = create_engine(
        TEST_DB_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    yield engine
    Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="function")
def test_session(test_engine):
    """Create a test database session."""
    SessionLocal = sessionmaker(bind=test_engine, autocommit=False, autoflush=False)
    session = SessionLocal()
    yield session
    session.close()


@pytest.fixture
def ingestion_service(test_session):
    """Create an IngestionService with test session."""
    SessionLocal = sessionmaker(bind=test_session.get_bind())
    return IngestionService(SessionLocal, PipelineConfig())


@pytest.fixture
def sample_record():
    """Generate a sample record dict."""
    return {
        "source_id": "test-src-001",
        "source_type": "api",
        "category": "transactions",
        "subcategory": "payment",
        "payload": {"amount": 99.99, "currency": "USD"},
        "priority": 5,
        "size_bytes": 1024,
        "region": "us-east-1",
        "tags": ["production", "important"],
        "metadata_extra": {"version": "1.0"},
    }


@pytest.fixture
def sample_records():
    """Generate a list of sample records."""
    import random
    import uuid

    records = []
    source_types = ["api", "webhook", "file_upload", "stream", "batch_import"]
    categories = [
        "transactions",
        "user_events",
        "system_logs",
        "analytics",
        "notifications",
    ]
    regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]

    for i in range(100):
        records.append(
            {
                "source_id": f"src-{uuid.uuid4().hex[:8]}",
                "source_type": random.choice(source_types),
                "category": random.choice(categories),
                "subcategory": random.choice(["payment", "login", "error"]),
                "payload": {"value": random.uniform(0, 1000), "index": i},
                "priority": random.randint(0, 10),
                "size_bytes": random.randint(100, 10000),
                "region": random.choice(regions),
                "tags": ["test"],
                "metadata_extra": {},
            }
        )
    return records


@pytest.fixture
def test_app(test_engine):
    """Create a test FastAPI application."""
    from app.main import app
    from app.routes import analytics, records

    SessionLocal = sessionmaker(bind=test_engine, autocommit=False, autoflush=False)
    service = IngestionService(SessionLocal, PipelineConfig())

    def override_get_db():
        session = SessionLocal()
        try:
            yield session
        finally:
            session.close()

    def override_get_service():
        return service

    app.dependency_overrides[records.get_db] = override_get_db
    app.dependency_overrides[records.get_ingestion_service] = override_get_service
    app.dependency_overrides[analytics.get_db] = override_get_db

    yield app
    app.dependency_overrides.clear()


@pytest.fixture
def client(test_app):
    """Create a test HTTP client."""
    return TestClient(test_app)
