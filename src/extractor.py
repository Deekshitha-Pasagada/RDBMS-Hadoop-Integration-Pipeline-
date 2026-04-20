import logging
import hashlib
from datetime import datetime, timezone
from typing import Optional, Iterator
import pandas as pd
from sqlalchemy import create_engine, text, pool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("rdbms-extractor")


class RDBMSExtractor:
    """
    Extracts data from RDBMS systems (MySQL/PostgreSQL) for
    integration with the Hadoop ecosystem. Supports full table,
    incremental watermark-based, and custom SQL extraction with
    connection pooling and batch processing.
    """

    def __init__(self, config: dict):
        self.config = config
        connection_string = self._build_connection_string(config)
        self.engine = create_engine(
            connection_string,
            poolclass=pool.QueuePool,
            pool_size=config.get("pool_size", 5),
            max_overflow=config.get("max_overflow", 10),
            pool_pre_ping=True
        )
        logger.info(
            f"RDBMS Extractor initialized: "
            f"{config.get('type', 'unknown')} @ "
            f"{config.get('host', 'localhost')}"
        )

    def _build_connection_string(self, config: dict) -> str:
        db_type = config.get("type", "postgresql")
        host = config.get("host", "localhost")
        port = config.get("port", 5432)
        database = config.get("database", "db")
        username = config.get("username", "user")
        password = config.get("password", "pass")
        if db_type == "mysql":
            return (
                f"mysql+pymysql://{username}:{password}"
                f"@{host}:{port}/{database}"
            )
        return (
            f"postgresql://{username}:{password}"
            f"@{host}:{port}/{database}"
        )

    def extract_full(
        self,
        table_name: str,
        batch_size: int = 50000
    ) -> pd.DataFrame:
        """Full table extraction with batch processing."""
        logger.info(
            f"Full extract: {table_name} "
            f"(batch_size={batch_size})")
        chunks = []
        query = f"SELECT * FROM {table_name}"
        with self.engine.connect() as conn:
            for chunk in pd.read_sql(
                text(query), conn, chunksize=batch_size
            ):
                chunks.append(chunk)
                logger.info(
                    f"  Fetched chunk: {len(chunk)} rows")
        df = pd.concat(chunks, ignore_index=True)
        logger.info(
            f"Full extract complete: {len(df)} rows "
            f"from {table_name}")
        return df

    def extract_incremental(
        self,
        table_name: str,
        incremental_col: str,
        last_watermark: str,
        batch_size: int = 50000
    ) -> pd.DataFrame:
        """
        Incremental extraction using watermark column.
        Only fetches records updated since last_watermark.
        """
        logger.info(
            f"Incremental extract: {table_name} "
            f"since {last_watermark}")
        query = f"""
            SELECT * FROM {table_name}
            WHERE {incremental_col} > :watermark
            ORDER BY {incremental_col}
        """
        chunks = []
        with self.engine.connect() as conn:
            for chunk in pd.read_sql(
                text(query),
                conn,
                params={"watermark": last_watermark},
                chunksize=batch_size
            ):
                chunks.append(chunk)

        if not chunks:
            logger.info(
                f"No new records since {last_watermark}")
            return pd.DataFrame()

        df = pd.concat(chunks, ignore_index=True)
        logger.info(
            f"Incremental extract: {len(df)} new rows "
            f"from {table_name}")
        return df

    def extract_sql(self, query: str) -> pd.DataFrame:
        """Extract using custom SQL query."""
        logger.info(
            f"Custom SQL extract: {query[:60]}...")
        with self.engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        logger.info(f"SQL extract: {len(df)} rows")
        return df

    def get_watermark(
        self,
        table_name: str,
        watermark_col: str
    ) -> Optional[str]:
        """Get current max watermark value from source table."""
        query = (
            f"SELECT MAX({watermark_col}) as wm "
            f"FROM {table_name}"
        )
        with self.engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()
            watermark = result[0] if result else None
        logger.info(
            f"Current watermark {table_name}."
            f"{watermark_col}: {watermark}")
        return str(watermark) if watermark else None

    def get_row_count(self, table_name: str) -> int:
        """Get total row count for a table."""
        with self.engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM {table_name}")
            ).fetchone()
            return result[0] if result else 0