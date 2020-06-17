# mostly copied from:
# https://github.com/elifesciences/data-hub-core-airflow-dags/blob/develop/data_pipeline/utils/data_store/bq_data_service.py

import logging
from typing import List
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField
from google.cloud.exceptions import NotFound
from bigquery_schema_generator.generate_schema import SchemaGenerator

from data_science_pipeline.utils.io import open_with_auto_compression


LOGGER = logging.getLogger(__name__)


def create_table(
        project_name: str,
        dataset_name: str,
        table_name: str,
        json_schema: list
):
    client = bigquery.Client()
    table_id = compose_full_table_name(
        project_name, dataset_name, table_name
    )
    schema = get_schemafield_list_from_json_list(json_schema)
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, True)  # API request
    LOGGER.info(
        "Created table %s.%s.%s",
        table.project,
        table.dataset_id,
        table.table_id
    )


def create_table_if_not_exist(
        project_name: str,
        dataset_name: str,
        table_name: str,
        json_schema: list
):
    if not does_bigquery_table_exist(project_name, dataset_name, table_name):
        create_table(project_name, dataset_name, table_name, json_schema)


def compose_full_table_name(
        project_name: str, dataset_name: str, table_name: str
) -> str:
    return ".".join([project_name, dataset_name, table_name])


def does_bigquery_table_exist(
        project_name: str, dataset_name: str, table_name: str
) -> bool:
    table_id = compose_full_table_name(project_name, dataset_name, table_name)
    client = bigquery.Client()
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False


def get_schemafield_list_from_json_list(
        json_schema: List[dict]
) -> List[SchemaField]:
    schema = [SchemaField.from_api_repr(x) for x in json_schema]
    return schema


def get_table_schema_field_names(
        project_name: str,
        dataset_name: str,
        table_name: str
):
    client = bigquery.Client()

    dataset_ref = client.dataset(dataset_name, project=project_name)
    table_ref = dataset_ref.table(table_name)
    try:
        table = client.get_table(table_ref)  # API Request
        return [field.name for field in table.schema]
    except NotFound:
        return []


def extend_table_schema_with_nested_schema(
        project_name: str, dataset_name: str,
        table_name: str, new_fields: list
):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_name, project=project_name)
    table_ref = dataset_ref.table(table_name)
    table = client.get_table(table_ref)  # Make an API request.
    original_schema = table.schema
    original_schema_dict = [
        schema_field.to_api_repr()
        for schema_field in original_schema
    ]
    new_schema_dict = get_new_merged_schema(
        original_schema_dict, new_fields
    )
    new_schema = [
        SchemaField.from_api_repr(schema_field_dict)
        for schema_field_dict in new_schema_dict
    ]

    table.schema = new_schema
    client.update_table(table, ["schema"])  # Make an API request.


def get_new_merged_schema(
        existing_schema: list,
        update_schema: list,
):
    new_schema = []
    existing_schema_dict = {
        schema_object.get("name").lower(): schema_object
        for schema_object in existing_schema
    }
    update_schema_dict = {
        schema_object.get("name").lower(): schema_object
        for schema_object in update_schema
    }
    merged_dict = {
        **update_schema_dict,
        **existing_schema_dict
    }
    set_intersection = (
        set(existing_schema_dict.keys()).intersection(
            set(update_schema_dict.keys())
        )
    )

    fields_to_recurse = [
        obj_key
        for obj_key in set_intersection
        if existing_schema_dict.get(obj_key).get("fields") and
        isinstance(existing_schema_dict.get(obj_key).get("fields"), list)
    ]
    new_schema.extend(
        [
            merged_dict.get(key)
            for key, value in merged_dict.items()
            if key not in fields_to_recurse
        ]
    )
    for field_to_recurse in fields_to_recurse:
        field = existing_schema_dict.get(field_to_recurse).copy()
        field["fields"] = get_new_merged_schema(
            existing_schema_dict.get(field_to_recurse).get("fields", []),
            update_schema_dict.get(field_to_recurse).get("fields", []),
        )
        new_schema.append(
            field
        )

    return new_schema


def generate_schema_from_file(
        full_file_location: str,
        quoted_values_are_strings: str = True
):
    with open_with_auto_compression(full_file_location, 'r') as file_reader:
        generator = SchemaGenerator(
            input_format="json",
            quoted_values_are_strings=quoted_values_are_strings
        )
        schema_map, _ = generator.deduce_schema(
            file_reader
        )
        schema = generator.flatten_schema(schema_map)
    return schema


def create_or_extend_table_schema(
        gcp_project,
        dataset_name,
        table_name,
        full_file_location,
        quoted_values_are_strings: True
):
    schema = generate_schema_from_file(
        full_file_location,
        quoted_values_are_strings
    )

    if does_bigquery_table_exist(
            gcp_project,
            dataset_name,
            table_name,
    ):
        extend_table_schema_with_nested_schema(
            gcp_project,
            dataset_name,
            table_name,
            schema
        )
    else:
        create_table(
            gcp_project,
            dataset_name,
            table_name,
            schema
        )
