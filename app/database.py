"""Database connection management and session handling."""

import logging
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.pool import QueuePool

from app.config import DatabaseConfig, PipelineConfig

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    """SQLAlchemy declarative base."""

    pass


class DatabaseManager:
    """Manages database connections, sessions, and performance tuning."""

    def __init__(
        self, db_config: DatabaseConfig, pipeline_config: PipelineConfig = None
    ):
        self.config = db_config
        self.pipeline_config = pipeline_config or PipelineConfig()

        self.engine = create_engine(
            db_config.url,
            poolclass=QueuePool,
            pool_size=db_config.min_pool_size,
            max_overflow=db_config.max_pool_size - db_config.min_pool_size,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False,
        )

        # Set performance parameters on each new connection
        @event.listens_for(self.engine, "connect")
        def set_pg_params(dbapi_conn, connection_record):
            cursor = dbapi_conn.cursor()
            cursor.execute(f"SET work_mem = '{self.pipeline_config.work_mem_mb}MB'")
            cursor.execute(
                f"SET maintenance_work_mem = '{self.pipeline_config.maintenance_work_mem_mb}MB'"
            )
            cursor.execute(
                f"SET statement_timeout = '{self.pipeline_config.statement_timeout_ms}'"
            )
            cursor.close()

        self.SessionLocal = sessionmaker(
            bind=self.engine, autocommit=False, autoflush=False
        )

    def create_tables(self):
        """Create all database tables."""
        Base.metadata.create_all(bind=self.engine)
        logger.info("Database tables created")

    def drop_tables(self):
        """Drop all database tables."""
        Base.metadata.drop_all(bind=self.engine)
        logger.info("Database tables dropped")

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Get a database session with automatic cleanup."""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def execute_raw(self, query: str, params: dict = None):
        """Execute a raw SQL query."""
        with self.engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            conn.commit()
            return result

    def get_query_plan(self, query: str, params: dict = None) -> list:
        """Get the EXPLAIN ANALYZE output for a query."""
        explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"
        with self.engine.connect() as conn:
            result = conn.execute(text(explain_query), params or {})
            return result.fetchall()

    def vacuum_analyze(self, table_name: str):
        """Run VACUUM ANALYZE on a table."""
        with self.engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            conn.execute(text(f"VACUUM ANALYZE {table_name}"))
            logger.info(f"VACUUM ANALYZE completed for {table_name}")

    def get_table_stats(self, table_name: str) -> dict:
        """Get table statistics from pg_stat_user_tables."""
        query = """
        SELECT
            n_live_tup AS row_count,
            n_dead_tup AS dead_rows,
            seq_scan AS sequential_scans,
            idx_scan AS index_scans,
            n_tup_ins AS inserts,
            n_tup_upd AS updates,
            n_tup_del AS deletes,
            pg_size_pretty(pg_total_relation_size(:table)) AS total_size,
            pg_size_pretty(pg_indexes_size(:table)) AS index_size
        FROM pg_stat_user_tables
        WHERE relname = :table
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"table": table_name})
            row = result.fetchone()
            if row:
                return dict(row._mapping)
            return {}

    def get_index_usage(self, table_name: str) -> list:
        """Get index usage statistics."""
        query = """
        SELECT
            indexrelname AS index_name,
            idx_scan AS scans,
            idx_tup_read AS tuples_read,
            idx_tup_fetch AS tuples_fetched,
            pg_size_pretty(pg_relation_size(indexrelid)) AS size
        FROM pg_stat_user_indexes
        WHERE relname = :table
        ORDER BY idx_scan DESC
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"table": table_name})
            return [dict(row._mapping) for row in result.fetchall()]
