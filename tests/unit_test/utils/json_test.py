import json
from pathlib import Path

import numpy as np

from data_science_pipeline.utils.json import (
    get_json_compatible_value,
    get_recursive_json_compatible_value,
    remove_key_with_null_value,
    write_jsonl_to_file
)


class TestGetJsonCompatibleValue:
    def test_should_convert_numpy_array_to_list(self):
        assert get_json_compatible_value(np.asarray([1, 2, 3])) == [1, 2, 3]


class TestGetRecursiveJsonCompatibleValue:
    def test_should_return_passed_in_none_value(self):
        assert get_recursive_json_compatible_value(None) is None

    def test_should_return_passed_in_string_value(self):
        assert get_recursive_json_compatible_value('123') == '123'

    def test_should_return_passed_in_int_value(self):
        assert get_recursive_json_compatible_value(123) == 123

    def test_should_convert_numpy_array_to_list(self):
        assert get_recursive_json_compatible_value(np.asarray([1, 2, 3])) == [1, 2, 3]

    def test_should_convert_numpy_array_within_dict(self):
        assert get_recursive_json_compatible_value({
            'key1': np.asarray([1, 2, 3])
        }) == {
            'key1': [1, 2, 3]
        }

    def test_should_convert_numpy_array_within_list(self):
        assert get_recursive_json_compatible_value([
            np.asarray([1, 2, 3])
        ]) == [
            [1, 2, 3]
        ]


class TestRemoveKeyWithNullValue:
    def test_should_remove_none_from_dict(self):
        assert remove_key_with_null_value({
            'key1': None,
            'other': 'value'
        }) == {'other': 'value'}

    def test_should_remove_empty_string_from_dict(self):
        assert remove_key_with_null_value({
            'key1': '',
            'other': 'value'
        }) == {'other': 'value'}

    def test_should_remove_not_remove_false_from_dict(self):
        assert remove_key_with_null_value({
            'key1': False,
            'other': 'value'
        }) == {'key1': False, 'other': 'value'}

    def test_should_remove_empty_list_from_dict(self):
        assert remove_key_with_null_value({
            'key1': [],
            'other': 'value'
        }) == {'other': 'value'}

    def test_should_remove_np_nan_from_dict(self):
        assert remove_key_with_null_value({
            'key1': np.nan,
            'other': 'value'
        }) == {'other': 'value'}

    def test_should_not_fail_with_np_array(self):
        record = {
            'key1': np.asarray([1, 2, 3]),
            'other': 'value'
        }
        assert remove_key_with_null_value(record.copy()) == record

    def test_should_remove_none_from_dict_within_list(self):
        assert remove_key_with_null_value([{
            'key1': None,
            'other': 'value'
        }]) == [{'other': 'value'}]

    def test_should_remove_none_from_dict_within_dict(self):
        assert remove_key_with_null_value({
            'parent': {
                'key1': None,
                'other': 'value'
            }
        }) == {
            'parent': {'other': 'value'}
        }

    def test_should_not_modify_passed_in_value(self):
        record = {
            'key1': None,
            'other': 'value'
        }
        record_copy = record.copy()
        remove_key_with_null_value(record)
        assert record == record_copy


class TestWriteJsonlToFile:
    def test_should_be_able_to_write_numpy_array(self, temp_dir: Path):
        temp_file = temp_dir / 'file.json'
        write_jsonl_to_file(
            json_list=[{'key1': np.asarray([1, 2, 3])}],
            full_temp_file_location=temp_file
        )
        assert json.loads(temp_file.read_text()) == {
            'key1': [1, 2, 3]
        }
