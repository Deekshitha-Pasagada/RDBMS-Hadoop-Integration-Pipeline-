import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional
import pandas as pd
import numpy as np

logger = logging.getLogger("rdbms-transformer")


class HadoopTransformer:
    """
    Transforms RDBMS data for Hadoop ecosystem compatibility.
    Handles schema normalization, type casting, data quality,
    Hive schema generation, and full data lineage tracking.
    """

    def __init__(self, config: dict):
        self.config = config
        self.batch_id = str(uuid.uuid4())[:8]

    def normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names for Hive compatibility."""
        original = list(df.columns)
        df.columns = [
            re.sub(r'[^a-z0-9_]', '_', col.lower().strip())
            for col in df.columns
        ]
        renamed = [
            f"{o}→{n}"
            for o, n in zip(original, df.columns)
            if o != n
        ]
        if renamed:
            logger.info(
                f"Schema normalized: {renamed}")
        return df

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Data quality — nulls, duplicates, whitespace."""
        before = len(df)
        df = df.drop_duplicates()
        dupes = before - len(df)
        if dupes:
            logger.info(f"Removed {dupes} duplicates")

        num_cols = df.select_dtypes(
            include=[np.number]).columns
        for col in num_cols:
            nulls = df[col].isna().sum()
            if nulls > 0:
                median = df[col].median()
                df[col] = df[col].fillna(median)
                logger.info(
                    f"Filled {nulls} nulls in "
                    f"'{col}' with median {median:.4f}")

        str_cols = df.select_dtypes(
            include=["object"]).columns
        for col in str_cols:
            df[col] = df[col].fillna("").str.strip()

        return df

    def cast_types(
        self,
        df: pd.DataFrame,
        schema: Dict[str, str]
    ) -> pd.DataFrame:
        """Cast columns to Hive/Parquet compatible types."""
        for col, dtype in schema.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                    logger.info(
                        f"Cast '{col}' → {dtype}")
                except Exception as e:
                    logger.warning(
                        f"Cast failed '{col}': {e}")
        return df

    def remove_outliers(
        self,
        df: pd.DataFrame,
        columns: List[str],
        z_threshold: float = 3.0
    ) -> pd.DataFrame:
        """Remove statistical outliers using Z-score."""
        for col in columns:
            if col in df.columns and len(df) > 0:
                mean = df[col].mean()
                std = df[col].std()
                if std > 0:
                    z = (df[col] - mean) / std
                    before = len(df)
                    df = df[z.abs() <= z_threshold]
                    removed = before - len(df)
                    if removed > 0:
                        logger.info(
                            f"Removed {removed} outliers "
                            f"from '{col}'")
        return df

    def add_lineage(
        self,
        df: pd.DataFrame,
        source_table: str,
        extract_mode: str = "full"
    ) -> pd.DataFrame:
        """Add full data lineage metadata columns."""
        df["_source_table"] = source_table
        df["_source_system"] = self.config.get(
            "type", "rdbms")
        df["_extract_mode"] = extract_mode
        df["_batch_id"] = self.batch_id
        df["_ingestion_timestamp"] = datetime.now(
            timezone.utc).isoformat()
        df["_pipeline_version"] = self.config.get(
            "version", "1.0.0")
        return df

    def generate_hive_schema(
        self,
        df: pd.DataFrame,
        table_name: str,
        partition_col: Optional[str] = None
    ) -> str:
        """Generate Hive CREATE TABLE DDL from DataFrame."""
        type_map = {
            "int64": "BIGINT",
            "int32": "INT",
            "float64": "DOUBLE",
            "float32": "FLOAT",
            "object": "STRING",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP"
        }
        cols = []
        for col, dtype in df.dtypes.items():
            if col == partition_col:
                continue
            if col.startswith("_"):
                continue
            hive_type = type_map.get(
                str(dtype), "STRING")
            cols.append(f"  `{col}` {hive_type}")

        ddl = f"CREATE EXTERNAL TABLE IF NOT EXISTS "
        ddl += f"`{table_name}` (\n"
        ddl += ",\n".join(cols)
        ddl += "\n)\n"
        ddl += "STORED AS PARQUET\n"
        ddl += "TBLPROPERTIES ('parquet.compress'='SNAPPY')"
        if partition_col:
            part_type = type_map.get(
                str(df[partition_col].dtype), "STRING")
            ddl += f"\nPARTITIONED BY "
            ddl += f"(`{partition_col}` {part_type})"
        return ddl

    def transform(
        self,
        df: pd.DataFrame,
        source_table: str,
        table_config: dict
    ) -> pd.DataFrame:
        """Full transformation pipeline."""
        if df.empty:
            logger.info(
                f"Empty DataFrame for {source_table}, "
                f"skipping transform")
            return df

        df = self.normalize_schema(df)
        df = self.clean(df)

        schema = table_config.get("schema")
        if schema:
            df = self.cast_types(df, schema)

        outlier_cols = table_config.get(
            "outlier_columns", [])
        if outlier_cols:
            df = self.remove_outliers(df, outlier_cols)

        extract_mode = table_config.get(
            "extract_mode", "full")
        df = self.add_lineage(df, source_table, extract_mode)

        logger.info(
            f"Transform complete: {source_table} → "
            f"{df.shape[0]} rows, {df.shape[1]} cols")
        return df