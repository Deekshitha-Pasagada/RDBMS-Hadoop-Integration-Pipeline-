import pytest
import pandas as pd
import numpy as np
import sys
import os
import tempfile

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), '..', 'src')
)
from transformer import HadoopTransformer
from loader import HadoopLoader


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "Customer ID": [1, 2, 2, 3, 4, 5],
        "Age": [25.0, 30.0, 30.0, np.nan, 200.0, 28.0],
        "Name": ["Alice", "Bob", "Bob", None, "Dave", "Eve"],
        "Amount": [100.0, 200.0, 200.0, 150.0, 99999.0, 180.0],
        "Country": ["US", "UK", "UK", "US", "UK", "US"],
        "updated_at": pd.date_range(
            "2024-01-01", periods=6, freq="D")
    })


@pytest.fixture
def transformer():
    return HadoopTransformer({
        "type": "postgresql",
        "version": "1.0.0"
    })


@pytest.fixture
def table_config():
    return {
        "extract_mode": "incremental",
        "schema": {
            "customer_id": "float64",
            "age": "float64"
        },
        "outlier_columns": ["amount"],
        "partition_col": "country"
    }


def test_normalize_schema(transformer, sample_df):
    df = transformer.normalize_schema(sample_df)
    assert "customer_id" in df.columns
    assert "Customer ID" not in df.columns


def test_remove_duplicates(transformer, sample_df):
    df = transformer.normalize_schema(sample_df)
    df = transformer.clean(df)
    assert len(df) == 5


def test_fill_nulls(transformer, sample_df):
    df = transformer.normalize_schema(sample_df)
    df = transformer.clean(df)
    assert df["age"].isna().sum() == 0
    assert df["name"].isna().sum() == 0


def test_remove_outliers(transformer, sample_df):
    df = transformer.normalize_schema(sample_df)
    df = transformer.clean(df)
    df = transformer.remove_outliers(df, ["amount"])
    assert 99999.0 not in df["amount"].values


def test_add_lineage(transformer, sample_df):
    df = transformer.normalize_schema(sample_df)
    df = transformer.add_lineage(df, "customers", "full")
    assert "_source_table" in df.columns
    assert "_batch_id" in df.columns
    assert "_ingestion_timestamp" in df.columns
    assert df["_source_table"].iloc[0] == "customers"


def test_generate_hive_ddl(transformer, sample_df):
    df = transformer.normalize_schema(sample_df)
    ddl = transformer.generate_hive_schema(
        df, "customers", partition_col="country")
    assert "CREATE EXTERNAL TABLE" in ddl
    assert "STORED AS PARQUET" in ddl
    assert "SNAPPY" in ddl
    assert "PARTITIONED BY" in ddl


def test_full_transform(transformer, sample_df, table_config):
    df = transformer.transform(
        sample_df, "customers", table_config)
    assert "_source_table" in df.columns
    assert len(df) > 0


def test_load_parquet(sample_df):
    with tempfile.TemporaryDirectory() as tmpdir:
        loader = HadoopLoader(
            output_path=tmpdir,
            checkpoint_path=tmpdir
        )
        path = loader.load_parquet(sample_df, "customers")
        assert os.path.exists(path)
        loaded = pd.read_parquet(path)
        assert len(loaded) == len(sample_df)


def test_partitioned_load(sample_df):
    with tempfile.TemporaryDirectory() as tmpdir:
        loader = HadoopLoader(
            output_path=tmpdir,
            checkpoint_path=tmpdir
        )
        path = loader.load_parquet(
            sample_df, "customers",
            partition_col="Country")
        assert os.path.exists(path)


def test_checkpoint_save_load():
    with tempfile.TemporaryDirectory() as tmpdir:
        loader = HadoopLoader(
            output_path=tmpdir,
            checkpoint_path=tmpdir
        )
        loader.save_checkpoint(
            "customers", "2024-01-15 00:00:00", 5000)
        checkpoint = loader.load_checkpoint("customers")
        assert checkpoint is not None
        assert checkpoint["watermark"] == \
               "2024-01-15 00:00:00"
        assert checkpoint["rows_loaded"] == 5000


def test_validation(sample_df):
    with tempfile.TemporaryDirectory() as tmpdir:
        loader = HadoopLoader(
            output_path=tmpdir,
            checkpoint_path=tmpdir
        )
        path = loader.load_parquet(sample_df, "test")
        result = loader.validate(sample_df, path)
        assert result["match"] is True