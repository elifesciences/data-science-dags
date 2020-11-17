import numpy as np

from data_science_pipeline.utils.json import remove_key_with_null_value


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
