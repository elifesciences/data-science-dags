import logging
import json
import os
import time
from contextlib import contextmanager
from itertools import islice
from tempfile import TemporaryDirectory
from typing import Iterable, List, ContextManager, Tuple

import pandas as pd

import google.cloud.exceptions
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery import (
    LoadJobConfig,
    QueryJobConfig,
    Client,
    SourceFormat,
    WriteDisposition
)

from data_science_pipeline.utils.io import open_with_auto_compression
from data_science_pipeline.utils.bq_schema import (
    generate_schema_from_file,
    get_schemafield_list_from_json_list,
    create_or_extend_table_schema
)


LOGGER = logging.getLogger(__name__)


def is_bq_not_found_exception(exception: BaseException) -> bool:
    if not exception:
        return False
    return (
        isinstance(exception, google.cloud.exceptions.NotFound)
        or is_bq_not_found_exception(exception.__context__)
    )


def get_client(project_id: str) -> Client:
    return Client(project=project_id)


def get_validated_dataset_name_table_name(
        dataset_name: str,
        table_name: str) -> Tuple[str, str]:
    if not table_name:
        raise ValueError('table_name is required')
    if dataset_name is None:
        dataset_name, table_name = table_name.split('.', maxsplit=1)
    return dataset_name, table_name


def load_file_into_bq(
        filename: str,
        dataset_name: str = None,
        table_name: str = None,
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        write_mode=WriteDisposition.WRITE_APPEND,
        auto_detect_schema=True,
        schema: List[SchemaField] = None,
        rows_to_skip=0,
        project_id: str = None):
    dataset_name, table_name = get_validated_dataset_name_table_name(
        dataset_name, table_name
    )
    if os.path.isfile(filename) and os.path.getsize(filename) == 0:
        LOGGER.info("File %s is empty.", filename)
        return
    client = get_client(project_id=project_id)
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
    job_config = LoadJobConfig()
    job_config.source_format = source_format
    job_config.write_disposition = write_mode
    job_config.autodetect = auto_detect_schema
    job_config.schema = schema
    if source_format is SourceFormat.CSV:
        job_config.skip_leading_rows = rows_to_skip
    LOGGER.info('loading from %s', filename)
    with open_with_auto_compression(filename, "rb") as source_file:
        job = client.load_table_from_file(
            source_file, destination=table_ref, job_config=job_config
        )

        # Waits for table cloud_data_store to complete
        job.result()
        LOGGER.info(
            "Loaded %s rows into %s:%s.",
            job.output_rows,
            dataset_name,
            table_name
        )


def load_file_into_bq_with_auto_schema(
        jsonl_file: str,
        dataset_name: str = None,
        table_name: str = None,
        write_mode: str = WriteDisposition.WRITE_APPEND,
        project_id: str = None,
        **kwargs):
    if write_mode == WriteDisposition.WRITE_APPEND:
        dataset_name, table_name = get_validated_dataset_name_table_name(
            dataset_name, table_name
        )
        create_or_extend_table_schema(
            gcp_project=project_id,
            dataset_name=dataset_name,
            table_name=table_name,
            full_file_location=jsonl_file,
            quoted_values_are_strings=True
        )
        load_file_into_bq(
            jsonl_file,
            project_id=project_id,
            dataset_name=dataset_name,
            table_name=table_name,
            write_mode=write_mode,
            schema=None,
            auto_detect_schema=False,
            **kwargs
        )
        return
    schema = get_schemafield_list_from_json_list(generate_schema_from_file(jsonl_file))
    LOGGER.debug('schema: %s', schema)
    load_file_into_bq(
        jsonl_file,
        project_id=project_id,
        dataset_name=dataset_name,
        table_name=table_name,
        write_mode=write_mode,
        schema=schema,
        auto_detect_schema=False,
        **kwargs
    )


def load_file_and_replace_bq_table_with_auto_schema(*args, **kwargs):
    load_file_into_bq_with_auto_schema(
        *args,
        write_mode=WriteDisposition.WRITE_TRUNCATE,
        **kwargs
    )


def load_file_and_append_to_bq_table_with_auto_schema(*args, **kwargs):
    load_file_into_bq_with_auto_schema(
        *args,
        write_mode=WriteDisposition.WRITE_APPEND,
        **kwargs
    )


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
        gzip_enabled: bool = True) -> ContextManager[str]:
    with TemporaryDirectory() as temp_dir:
        jsonl_file = os.path.join(temp_dir, 'data.jsonl')
        if gzip_enabled:
            jsonl_file += '.gz'
        write_jsonl_to_file(json_list, jsonl_file)
        yield jsonl_file


def load_json_list_into_bq_with_auto_schema(json_list: Iterable[dict], **kwargs):
    with json_list_as_jsonl_file(json_list) as  jsonl_file:
        load_file_into_bq_with_auto_schema(jsonl_file, **kwargs)


def load_json_list_and_replace_bq_table_with_auto_schema(*args, **kwargs):
    load_json_list_into_bq_with_auto_schema(
        *args, write_mode=WriteDisposition.WRITE_TRUNCATE, **kwargs
    )


def load_json_list_and_append_to_bq_table_with_auto_schema(*args, **kwargs):
    load_json_list_into_bq_with_auto_schema(
        *args, write_mode=WriteDisposition.WRITE_APPEND, **kwargs
    )


def get_bq_write_disposition(if_exists: str) -> WriteDisposition:
    if if_exists == 'replace':
        return WriteDisposition.WRITE_TRUNCATE
    if if_exists == 'append':
        return WriteDisposition.WRITE_APPEND
    if if_exists == 'fail':
        return WriteDisposition.WRITE_EMPTY
    raise ValueError('unsupported if_exists: %s' % if_exists)


def to_jsonl_without_null(df: pd.DataFrame, jsonl_file: str):
    write_jsonl_to_file(
        remove_key_with_null_value(
            df.to_dict(orient='records')
        ),
        jsonl_file
    )


def to_gbq(
        df: pd.DataFrame,
        destination_table: str,
        if_exists: str = 'fail',
        project_id: str = None,
        temp_dir: str = None):
    """Similar to DataFrame.to_gpq but better handles schema detection of nested fields"""
    dataset_name, table_name = destination_table.split('.', maxsplit=1)
    with TemporaryDirectory() as _temp_dir:
        jsonl_file = os.path.join(temp_dir or _temp_dir, 'data.jsonl')
        to_jsonl_without_null(df, jsonl_file)
        load_file_into_bq_with_auto_schema(
            jsonl_file,
            dataset_name=dataset_name,
            table_name=table_name,
            write_mode=get_bq_write_disposition(if_exists),
            project_id=project_id
        )


def run_query_and_save_to_table(  # pylint: disable=too-many-arguments
        client: Client,
        query: str,
        destination_dataset: str,
        destination_table_name: str):
    LOGGER.debug(
        "running query and saving to, destination=%s.%s, query=%r",
        destination_dataset, destination_table_name, query
    )

    start = time.perf_counter()
    dataset_ref = client.dataset(destination_dataset)
    destination_table_ref = dataset_ref.table(destination_table_name)

    job_config = QueryJobConfig()
    job_config.destination = destination_table_ref
    job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE

    query_job = client.query(query, job_config=job_config)
    # getting the result will make sure that the query ran successfully
    result: bigquery.table.RowIterator = query_job.result()
    duration = time.perf_counter() - start
    LOGGER.info(
        'ran query and saved to: %s.%s, total rows: %s, took: %.3fs',
        destination_dataset,
        destination_table_name,
        result.total_rows,
        duration
    )
    if LOGGER.isEnabledFor(logging.DEBUG):
        sample_result = list(islice(result, 3))
        LOGGER.debug("sample_result: %s", sample_result)
