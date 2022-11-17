import gzip
import json
from pathlib import Path

import pandas as pd
import numpy as np

from data_science_pipeline.utils.bq import (
    df_as_jsonl_file_without_null
)


class TestDfAsJsonlFileWithoutNull:
    def test_should_remove_null_value(self, temp_dir: Path):
        jsonl_file = temp_dir / 'data.jsonl'
        df = pd.DataFrame([{'key1': 'value1', 'key2': None}])
        with df_as_jsonl_file_without_null(df, gzip_enabled=False) as jsonl_file:
            result = [
                json.loads(line)
                for line in Path(jsonl_file).read_text().splitlines()
            ]
        assert result == [{'key1': 'value1'}]

    def test_should_remove_np_nan_value(self, temp_dir: Path):
        jsonl_file = temp_dir / 'data.jsonl'
        df = pd.DataFrame([{'key1': 'value1', 'key2': np.nan}])
        with df_as_jsonl_file_without_null(df, gzip_enabled=False) as jsonl_file:
            result = [
                json.loads(line)
                for line in Path(jsonl_file).read_text().splitlines()
            ]
        assert result == [{'key1': 'value1'}]

    def test_should_remove_null_value_from_nested_field(self, temp_dir: Path):
        jsonl_file = temp_dir / 'data.jsonl'
        df = pd.DataFrame([{'parent': {'key1': 'value1', 'key2': None}}])
        with df_as_jsonl_file_without_null(df, gzip_enabled=False) as jsonl_file:
            result = [
                json.loads(line)
                for line in Path(jsonl_file).read_text().splitlines()
            ]
        assert result == [{'parent': {'key1': 'value1'}}]

    def test_should_not_fail_with_list_values_field(self, temp_dir: Path):
        jsonl_file = temp_dir / 'data.jsonl'
        df = pd.DataFrame([{'key1': ['value1', 'value2'], 'key2': None}])
        with df_as_jsonl_file_without_null(df, gzip_enabled=False) as jsonl_file:
            result = [
                json.loads(line)
                for line in Path(jsonl_file).read_text().splitlines()
            ]
        assert result == [{'key1': ['value1', 'value2']}]

    def test_should_use_gzip_compression_by_default(self):
        df = pd.DataFrame([{'key1': 'value1', 'key2': None}])
        with df_as_jsonl_file_without_null(df) as jsonl_file:
            assert jsonl_file.endswith('.gz')
            result = [
                json.loads(line)
                for line in (
                    gzip.decompress(Path(jsonl_file).read_bytes())
                    .decode()
                    .splitlines()
                )
            ]
        assert result == [{'key1': 'value1'}]
