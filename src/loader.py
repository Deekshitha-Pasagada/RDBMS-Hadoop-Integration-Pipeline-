import logging
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import pandas as pd

logger = logging.getLogger("rdbms-loader")


class HadoopLoader:
    """
    Loads transformed data into Hadoop-compatible storage.
    Writes Parquet with Snappy compression, supports Hive
    partitioning, idempotent writes, and checkpoint tracking
    for pipeline fault tolerance and recovery.
    """

    def __init__(
        self,
        output_path: str,
        checkpoint_path: str = "data/checkpoints"
    ):
        self.output_path = Path(output_path)
        self.checkpoint_path = Path(checkpoint_path)
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.checkpoint_path.mkdir(parents=True, exist_ok=True)
        logger.info(
            f"Loader initialized: {output_path}")

    def load_parquet(
        self,
        df: pd.DataFrame,
        table_name: str,
        partition_col: Optional[str] = None,
        compression: str = "snappy"
    ) -> str:
        """
        Write DataFrame to Parquet format with optional
        Hive-compatible partitioning. Idempotent writes
        support pipeline recovery.
        """
        if df.empty:
            logger.info(
                f"Empty DataFrame — skipping {table_name}")
            return ""

        if partition_col and partition_col in df.columns:
            return self._write_partitioned(
                df, table_name, partition_col, compression)

        output_file = self.output_path / f"{table_name}.parquet"
        df.to_parquet(
            output_file,
            compression=compression,
            index=False
        )
        size_mb = output_file.stat().st_size / (1024 * 1024)
        logger.info(
            f"Loaded {len(df)} rows → {output_file} "
            f"({size_mb:.2f} MB, {compression})")
        return str(output_file)

    def _write_partitioned(
        self,
        df: pd.DataFrame,
        table_name: str,
        partition_col: str,
        compression: str
    ) -> str:
        """Write Hive-style partitioned Parquet."""
        table_path = self.output_path / table_name
        table_path.mkdir(parents=True, exist_ok=True)

        partitions = df[partition_col].unique()
        total_rows = 0

        for val in partitions:
            part_df = df[df[partition_col] == val]
            part_path = (
                table_path / f"{partition_col}={val}")
            part_path.mkdir(parents=True, exist_ok=True)
            output_file = part_path / "data.parquet"
            part_df.to_parquet(
                output_file,
                compression=compression,
                index=False
            )
            total_rows += len(part_df)

        logger.info(
            f"Partitioned load: {total_rows} rows, "
            f"{len(partitions)} partitions "
            f"by '{partition_col}'")
        return str(table_path)

    def save_checkpoint(
        self,
        table_name: str,
        watermark: str,
        rows_loaded: int
    ):
        """Save pipeline checkpoint for recovery."""
        checkpoint = {
            "table": table_name,
            "watermark": watermark,
            "rows_loaded": rows_loaded,
            "saved_at": datetime.now(
                timezone.utc).isoformat()
        }
        checkpoint_file = (
            self.checkpoint_path / f"{table_name}.json")
        with open(checkpoint_file, "w") as f:
            json.dump(checkpoint, f, indent=2)
        logger.info(
            f"Checkpoint saved: {table_name} "
            f"watermark={watermark}")

    def load_checkpoint(
        self, table_name: str
    ) -> Optional[dict]:
        """Load last checkpoint for incremental recovery."""
        checkpoint_file = (
            self.checkpoint_path / f"{table_name}.json")
        if not checkpoint_file.exists():
            return None
        with open(checkpoint_file) as f:
            checkpoint = json.load(f)
        logger.info(
            f"Loaded checkpoint: {table_name} "
            f"watermark={checkpoint.get('watermark')}")
        return checkpoint

    def validate(
        self,
        source_df: pd.DataFrame,
        output_path: str
    ) -> dict:
        """Validate loaded Parquet matches source."""
        try:
            loaded = pd.read_parquet(output_path)
            result = {
                "source_rows": len(source_df),
                "loaded_rows": len(loaded),
                "match": len(loaded) == len(source_df),
                "columns_match": (
                    set(source_df.columns)
                    == set(loaded.columns)
                )
            }
            logger.info(f"Validation: {result}")
            return result
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return {"match": False, "error": str(e)}