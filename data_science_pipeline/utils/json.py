import json
import os
from contextlib import contextmanager
from tempfile import TemporaryDirectory
from typing import ContextManager, Iterable

import pandas as pd
import numpy as np

from data_science_pipeline.utils.io import open_with_auto_compression


def get_json_compatible_value(value):
    """
    Returns a value that can be JSON serialized.
    This is more or less identical to JSONEncoder.default,
    but less dependent on the actual serialization.
    """
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


def get_recursive_json_compatible_value(value):
    if isinstance(value, dict):
        return {
            key: get_recursive_json_compatible_value(item_value)
            for key, item_value in value.items()
        }
    if isinstance(value, list):
        return [
            get_recursive_json_compatible_value(item_value)
            for item_value in value
        ]
    return get_json_compatible_value(value)


# copied from:
# https://github.com/elifesciences/data-hub-ejp-xml-pipeline/blob/develop/ejp_xml_pipeline/transform_json.py
# modified to handle numpy and pandas types
def remove_key_with_null_value(record):
    if isinstance(record, dict):
        for key in list(record):
            value = record.get(key)
            if (
                (value is None or np.isscalar(value))
                and (
                    pd.isnull(value)
                    or (not value and not isinstance(value, bool))
                )
            ):
                record.pop(key, None)
            elif isinstance(value, (dict, list)):
                remove_key_with_null_value(value)

    elif isinstance(record, list):
        for value in record:
            if isinstance(value, (dict, list)):
                remove_key_with_null_value(value)

    return record


def write_jsonl_to_file(
        json_list: Iterable[dict],
        full_temp_file_location: str,
        write_mode: str = 'w'):
    with open_with_auto_compression(full_temp_file_location, write_mode) as write_file:
        for record in json_list:
            try:
                write_file.write(json.dumps(get_recursive_json_compatible_value(record)))
            except TypeError as exc:
                raise TypeError('failed to convert %r due to %r' % (record, exc)) from exc
            write_file.write("\n")


@contextmanager
def json_list_as_jsonl_file(
        json_list: Iterable[dict],
        gzip_enabled: bool = True,
        jsonl_file: str = None) -> ContextManager[str]:
    if jsonl_file:
        write_jsonl_to_file(json_list, jsonl_file)
        yield jsonl_file
        return
    with TemporaryDirectory() as temp_dir:
        jsonl_file = os.path.join(temp_dir, 'data.jsonl')
        if gzip_enabled:
            jsonl_file += '.gz'
        write_jsonl_to_file(json_list, jsonl_file)
        yield jsonl_file
