import logging
import os
import time
from itertools import islice
from tempfile import TemporaryDirectory
from typing import List

import pandas as pd

from bigquery_schema_generator.generate_schema import SchemaGenerator

from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery import (
    LoadJobConfig,
    QueryJobConfig,
    Client,
    SourceFormat,
    WriteDisposition
)


LOGGER = logging.getLogger(__name__)


def get_client(project_id: str) -> Client:
    return Client(project=project_id)


def generate_schema_from_file(full_temp_file_location):
    file_reader = open(full_temp_file_location)
    generator = SchemaGenerator(
        input_format="json",
        quoted_values_are_strings=True
    )
    schema_map, _ = generator.deduce_schema(
        file_reader
    )
    schema = generator.flatten_schema(schema_map)
    return schema


def get_schemafield_list_from_json_list(
        json_schema: List[dict]) -> List[SchemaField]:
    schema = [SchemaField.from_api_repr(x) for x in json_schema]
    return schema


def load_file_into_bq(
        filename: str,
        dataset_name: str,
        table_name: str,
        source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
        write_mode=WriteDisposition.WRITE_APPEND,
        auto_detect_schema=True,
        schema: List[SchemaField] = None,
        rows_to_skip=0,
        project_id: str = None):
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
    with open(filename, "rb") as source_file:
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


def get_bq_write_disposition(if_exists: str) -> WriteDisposition:
    if if_exists == 'replace':
        return WriteDisposition.WRITE_TRUNCATE
    if if_exists == 'append':
        return WriteDisposition.WRITE_APPEND
    if if_exists == 'fail':
        return WriteDisposition.WRITE_EMPTY
    raise ValueError('unsupported if_exists: %s' % if_exists)


def to_gbq(
        df: pd.DataFrame,
        destination_table: str,
        if_exists: str = 'fail',
        project_id: str = None):
    """Similar to DataFrame.to_gpq but better handles schema detection of nested fields"""
    dataset_name, table_name = destination_table.split('.', maxsplit=1)
    with TemporaryDirectory() as temp_dir:
        jsonl_file = os.path.join(temp_dir, 'data.jsonl')
        df.to_json(jsonl_file, orient='records', lines=True)
        schema = get_schemafield_list_from_json_list(generate_schema_from_file(jsonl_file))
        LOGGER.info('schema: %s', schema)
        load_file_into_bq(
            jsonl_file,
            dataset_name=dataset_name,
            table_name=table_name,
            write_mode=get_bq_write_disposition(if_exists),
            schema=schema,
            project_id=project_id
        )


def get_select_all_from_query(view_name: str, project: str,
                              dataset: str) -> str:
    return f"SELECT * FROM `{project}.{dataset}.{view_name}`"


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
