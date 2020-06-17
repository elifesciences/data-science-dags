import json
from pathlib import Path

import pandas as pd

from data_science_pipeline.utils.bq import (
    to_jsonl_without_null
)


class TestToJsonlWithoutNull:
    def test_should_remove_null_value(self, temp_dir: Path):
        jsonl_file = temp_dir / 'data.jsonl'
        df = pd.DataFrame([{'key1': 'value1', 'key2': None}])
        to_jsonl_without_null(df, jsonl_file)
        result = [json.loads(line) for line in jsonl_file.read_text().splitlines()]
        assert result == [{'key1': 'value1'}]

    def test_should_remove_null_value_from_nested_field(self, temp_dir: Path):
        jsonl_file = temp_dir / 'data.jsonl'
        df = pd.DataFrame([{'parent': {'key1': 'value1', 'key2': None}}])
        to_jsonl_without_null(df, jsonl_file)
        result = [json.loads(line) for line in jsonl_file.read_text().splitlines()]
        assert result == [{'parent': {'key1': 'value1'}}]
