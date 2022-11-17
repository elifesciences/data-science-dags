import logging
import os
from pathlib import Path
import time
from contextlib import contextmanager
from itertools import islice
from typing import Iterable, Iterator, List, Optional, Tuple, cast

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
from data_science_pipeline.utils.json import (
    remove_key_with_null_value,
    json_list_as_jsonl_file
)
from data_science_pipeline.utils.bq_schema import (
    generate_schema_from_file,
    get_schemafield_list_from_json_list,
    create_or_extend_table_schema
)
from data_science_pipeline.utils.pandas import dataframe_chunk


LOGGER = logging.getLogger(__name__)


def with_limit_sql(sql: str, limit: Optional[int] = None) -> str:
    if not limit:
        return sql
    return sql + '\nLIMIT %d' % limit


def is_bq_not_found_exception(exception: Optional[BaseException] = None) -> bool:
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
        project_id: str,
        dataset_name: str,
        table_name: str,
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        write_mode=WriteDisposition.WRITE_APPEND,
        auto_detect_schema=True,
        schema: Optional[List[SchemaField]] = None,
        rows_to_skip=0):
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
    with open_with_auto_compression(cast(Path, filename), "rb") as source_file:
        job = client.load_table_from_file(
            source_file, destination=table_ref, job_config=job_config
        )

        # Waits for table cloud_data_store to complete
        job.result()
        LOGGER.info('File size is : %s', os.path.getsize(filename))
        LOGGER.info(
            "Loaded %s rows into %s:%s.",
            job.output_rows,
            dataset_name,
            table_name
        )


def load_file_into_bq_with_auto_schema(
        jsonl_file: str,
        project_id: str,
        dataset_name: str,
        table_name: str,
        write_mode: str = WriteDisposition.WRITE_APPEND,
        **kwargs):
    if write_mode == WriteDisposition.WRITE_APPEND:
        dataset_name, table_name = get_validated_dataset_name_table_name(
            dataset_name, table_name
        )
        LOGGER.info('creating or extending table %s.%s schema', dataset_name, table_name)
        create_or_extend_table_schema(
            gcp_project=project_id,
            dataset_name=dataset_name,
            table_name=table_name,
            full_file_location=jsonl_file,
            quoted_values_are_strings=True
        )
        LOGGER.info('loading the data to the table : %s.%s', dataset_name, table_name)
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


def load_json_list_into_bq_with_auto_schema(json_list: Iterable[dict], **kwargs):
    with json_list_as_jsonl_file(json_list) as jsonl_file:
        load_file_into_bq_with_auto_schema(str(jsonl_file), **kwargs)


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


def iter_json_without_null_from_df(df: pd.DataFrame, batch_size: int = 5000) -> Iterable[dict]:
    for df_batch in dataframe_chunk(df, batch_size):
        json_list = df_batch.to_dict(orient='records')
        LOGGER.info('[df.to_dict] length of json_list: %s', len(json_list))
        json_list = remove_key_with_null_value(json_list)
        yield from json_list


@contextmanager
def df_as_jsonl_file_without_null(df: pd.DataFrame, **kwargs) -> Iterator[str]:
    json_iterable = iter_json_without_null_from_df(df)
    with json_list_as_jsonl_file(json_iterable, **kwargs) as jsonl_file:
        yield str(jsonl_file)


def to_gbq(
        df: pd.DataFrame,
        destination_table: str,
        project_id: str,
        if_exists: str = 'fail',
        jsonl_file: Optional[str] = None):
    """Similar to DataFrame.to_gpq but better handles schema detection of nested fields"""
    dataset_name, table_name = destination_table.split('.', maxsplit=1)
    with df_as_jsonl_file_without_null(df, jsonl_file=jsonl_file) as _jsonl_file:
        LOGGER.info("Loading dataframe %s to BQ table %s ...", df, destination_table)
        load_file_into_bq_with_auto_schema(
            _jsonl_file,
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
