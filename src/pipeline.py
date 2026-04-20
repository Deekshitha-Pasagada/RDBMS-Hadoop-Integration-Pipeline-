import logging
import time
import yaml
from pathlib import Path
from extractor import RDBMSExtractor
from transformer import HadoopTransformer
from loader import HadoopLoader

logger = logging.getLogger("rdbms-pipeline")


class RDBMSHadoopPipeline:
    """
    End-to-end integration pipeline from RDBMS to Hadoop.
    Orchestrates extraction, transformation, and loading
    with checkpointing, lineage tracking, and benchmarking.
    """

    def __init__(
        self,
        config_path: str = "config/pipeline_config.yaml"
    ):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        source_config = {
            **self.config["source"],
            "version": self.config["pipeline"]["version"]
        }
        self.extractor = RDBMSExtractor(source_config)
        self.transformer = HadoopTransformer(source_config)
        self.loader = HadoopLoader(
            output_path=self.config["target"]["output_path"],
            checkpoint_path=self.config.get(
                "checkpoints", {}).get(
                    "path", "data/checkpoints")
        )
        logger.info(
            f"Pipeline initialized: "
            f"{self.config['pipeline']['name']}")

    def run_table(self, table_name: str) -> dict:
        """Run full ETL pipeline for a single table."""
        start = time.time()
        table_config = self.config.get(
            "tables", {}).get(table_name, {})
        extract_mode = table_config.get(
            "extract_mode", "full")

        logger.info(
            f"Starting pipeline: {table_name} "
            f"mode={extract_mode}")

        # EXTRACT
        t0 = time.time()
        if extract_mode == "incremental":
            checkpoint = self.loader.load_checkpoint(
                table_name)
            last_watermark = (
                checkpoint.get("watermark")
                if checkpoint else "1970-01-01 00:00:00"
            )
            incremental_col = table_config.get(
                "incremental_col",
                self.config["pipeline"].get(
                    "incremental_col", "updated_at")
            )
            df = self.extractor.extract_incremental(
                table_name,
                incremental_col,
                last_watermark,
                self.config["pipeline"].get(
                    "batch_size", 50000)
            )
        else:
            df = self.extractor.extract_full(
                table_name,
                self.config["pipeline"].get(
                    "batch_size", 50000)
            )
        extract_time = time.time() - t0

        if df.empty:
            logger.info(
                f"No data to load for {table_name}")
            return {
                "table": table_name,
                "rows": 0,
                "status": "no_new_data"
            }

        # TRANSFORM
        t1 = time.time()
        df = self.transformer.transform(
            df, table_name, table_config)
        transform_time = time.time() - t1

        # LOAD
        t2 = time.time()
        output_path = self.loader.load_parquet(
            df,
            table_name,
            partition_col=table_config.get("partition_col"),
            compression=self.config["target"].get(
                "compression", "snappy")
        )
        load_time = time.time() - t2

        # CHECKPOINT
        if extract_mode == "incremental":
            incremental_col = table_config.get(
                "incremental_col", "updated_at")
            if incremental_col in df.columns:
                watermark = str(
                    df[incremental_col].max())
                self.loader.save_checkpoint(
                    table_name, watermark, len(df))

        # HIVE DDL
        hive_ddl = self.transformer.generate_hive_schema(
            df,
            table_name,
            table_config.get("partition_col")
        )

        total_time = time.time() - start
        report = {
            "table": table_name,
            "rows": len(df),
            "columns": len(df.columns),
            "extract_mode": extract_mode,
            "output_path": output_path,
            "extract_s": round(extract_time, 2),
            "transform_s": round(transform_time, 2),
            "load_s": round(load_time, 2),
            "total_s": round(total_time, 2),
            "throughput_rows_per_s": round(
                len(df) / total_time, 0
            ) if total_time > 0 else 0,
            "hive_ddl": hive_ddl,
            "status": "success"
        }
        logger.info(
            f"Pipeline complete: {table_name} "
            f"{len(df)} rows in {total_time:.2f}s "
            f"({report['throughput_rows_per_s']} rows/s)")
        return report

    def run_all(self) -> list:
        """Run pipeline for all configured tables."""
        tables = list(
            self.config.get("tables", {}).keys())
        logger.info(
            f"Running pipeline for {len(tables)} tables")
        reports = []
        for table in tables:
            report = self.run_table(table)
            reports.append(report)
        return reports


if __name__ == "__main__":
    pipeline = RDBMSHadoopPipeline()
    reports = pipeline.run_all()
    print("\n" + "="*60)
    print("PIPELINE BENCHMARK REPORT")
    print("="*60)
    for r in reports:
        print(f"\nTable: {r['table']}")
        print(f"  Rows:       {r.get('rows', 0):,}")
        print(f"  Extract:    {r.get('extract_s', 0)}s")
        print(f"  Transform:  {r.get('transform_s', 0)}s")
        print(f"  Load:       {r.get('load_s', 0)}s")
        print(f"  Total:      {r.get('total_s', 0)}s")
        print(
            f"  Throughput: "
            f"{r.get('throughput_rows_per_s', 0):,} rows/s")
        print(f"  Status:     {r.get('status')}")
    print("="*60)