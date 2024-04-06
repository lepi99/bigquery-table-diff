
from google.api_core.exceptions import NotFound
from app.utils.build_queries import *
from flask import jsonify
import logging
logger = logging.getLogger(__name__)  # Logger named after the module
logger.setLevel(logging.DEBUG)

def input_json_validation(data):
    if not all(field in data for field in
               ["project_id", "dataset_id1", "dataset_id2", "table1", "table2", "key_columns",
                "compair_columns"]):
        error_data = {
            "type_error": "input config json",
            "message":"Missing required fields"  # You can keep the status code
        }
        logger.error(f"WARNING: issues with json input config")
        logger.error(
            f"WARNING:Missing required fields in the json input config")
        return error_data
    return ""

def table_existence_validation(client,dataset_id,table_name):
    try:
        client.get_table(f"{dataset_id}.{table_name}")
    except NotFound:
        error_data = {
            "type_error": "biguery table Not found",
            "message": f"Table '{dataset_id}.{table_name}' not found"
        }
        logger.warning(f"WARNING: biguery table Not found")
        logger.warning(
            f"WARNING:Table '{dataset_id}.{table_name}' not found")
        return error_data
    return ""


def missing_columns_validation(table,columns):
    schema_fields = {field.name for field in table.schema}
    missing_columns = set(columns) - schema_fields
    if missing_columns:
        error_data = {
            "type_error": "biguery table missing columns",
            "message": f"Missing columns in '{table.project}.{table.dataset_id}.{table.table_id.split('.')[-1]}': {missing_columns}"
        }
        logger.warning(f"WARNING: biguery table missing columns")
        logger.warning(
            f"WARNING:Missing columns in '{table.project}.{table.dataset_id}.{table.table_id.split('.')[-1]}': {missing_columns}")
        return error_data
    return ""


def unique_query_validation(client,table, columns):
    uniqueQ = build_unique_query(table, columns)

    # print(uniqueQ)
    query_job = client.query(uniqueQ)
    results = query_job.result()
    countD = 0
    for row in results:
        countD += 1
        # There are rows in the results
    if countD > 0:
        error_data = {
            "type_error": "biguery table with duplicates with given keys",
            "message": f"Duplicates columns in '{table.table_id}' with keys: {', '.join(f'{col}' for col in columns)}",
            "status_code": 402  # You can keep the status code
        }
        logger.warning(f"WARNING: biguery table with duplicates with given keys")
        logger.warning(
            f"WARNING:Duplicates columns in '{table.table_id}' with keys: {', '.join(f'{col}' for col in columns)}")
        return error_data
    return ""


def schema_difference_validation(table1,table2,data):
    schema_diffs = {}
    for field1 in table1.schema:
        if field1.name not in data["key_columns"] + data["compair_columns"]:

            field2 = None  # In case it's not found
            for field in table2.schema:
                if field.name == field1.name:
                    field2 = field
                    break

            if field2 is not None and field2.field_type != field1.field_type:
                schema_diffs[field1.name] = {
                    "type1": field1.field_type,
                    "type2": field2.field_type
                }

    if schema_diffs:
        logger.warning(f"WARNING: schema differences: {schema_diffs}")
        return jsonify({"error": f"schema differences: {schema_diffs}"})
    return ""


def validate_input(client,data):
    """
    Validates the input data.
    Returns a dictionary with validation results.
    """
    validation = {"errors": []}
    validation["data"]=data

    # 1. Basic Validation
    json_validation_errors=input_json_validation(data)
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print(json_validation_errors)
    if json_validation_errors != "":
        print("222222222222222222222222222222222222222222222")
        validation["errors"].append(json_validation_errors)
        return validation



    table_existence_validation()
    # 2. Table Existence Validation
    for client, dataset_id, table_name in [(client, data["dataset_id1"], data["table1"]),
                                           (client, data["dataset_id2"], data["table2"])]:
        table_existence_errors=table_existence_validation(client, dataset_id, table_name)

        if table_existence_errors != "":
            validation["errors"].append(table_existence_errors)

    if validation["errors"]:
        return validation

    table1 = client.get_table(f"{data['dataset_id1']}.{data['table1']}")
    table2 = client.get_table(f"{data['dataset_id2']}.{data['table2']}")
    # 3. Key and Comparison Column Validation
    for client, table, columns in [
        (client, table1, data["key_columns"] + data["compair_columns"]),
        (client, table2, data["key_columns"] + data["compair_columns"])]:

        missing_columns_errors=missing_columns_validation(table, columns)
        if missing_columns_errors!= "":
            validation["errors"].append( missing_columns_errors)

    if validation["errors"]:
        return validation

    for client, table, columns in [
        (client, table1, data["key_columns"]),
        (client, table2, data["key_columns"])]:

        unique_query_errors=unique_query_validation(client,table, columns)
        if unique_query_errors !="":
            validation["errors"].append(unique_query_errors)

    if validation["errors"]:
        return validation

    schema_difference_errors=schema_difference_validation(table1,table2,data)
    schema_diffs = {}
    for field1 in table1.schema:
        if field1.name not in data["key_columns"] + data["compair_columns"]:

            field2 = None  # In case it's not found
            for field in table2.schema:
                if field.name == field1.name:
                    field2 = field
                    break
            #field2 = table2.schema.get(field1.name)
            if field2 is not None and field2.field_type != field1.field_type:
                schema_diffs[field1.name] = {
                    "type1": field1.field_type,
                    "type2": field2.field_type
                }

    if schema_difference_errors != "":
        validation["errors"].append( jsonify({
                           "error": f"schema differences: {schema_diffs}"}))
        if validation["errors"]:
            return validation
    return validation