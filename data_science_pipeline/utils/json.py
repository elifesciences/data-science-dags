import json
import os
from contextlib import contextmanager
from tempfile import TemporaryDirectory
from typing import ContextManager, Iterable

from data_science_pipeline.utils.io import open_with_auto_compression


# copied from:
# https://github.com/elifesciences/data-hub-ejp-xml-pipeline/blob/develop/ejp_xml_pipeline/transform_json.py
def remove_key_with_null_value(record):
    if isinstance(record, dict):
        for key in list(record):
            val = record.get(key)
            if not val and not isinstance(val, bool):
                record.pop(key, None)
            elif isinstance(val, (dict, list)):
                remove_key_with_null_value(val)

    elif isinstance(record, list):
        for val in record:
            if isinstance(val, (dict, list)):
                remove_key_with_null_value(val)

    return record


def write_jsonl_to_file(
        json_list: Iterable[dict],
        full_temp_file_location: str,
        write_mode: str = 'w'):
    with open_with_auto_compression(full_temp_file_location, write_mode) as write_file:
        for record in json_list:
            write_file.write(json.dumps(record))
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
