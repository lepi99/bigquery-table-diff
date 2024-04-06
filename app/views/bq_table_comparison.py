from flask import Blueprint,request
view_bqtb = Blueprint('bq_table_comparison', __name__)
from google.cloud import bigquery
from app.utils.validation import * # Import input validation functions
import logging
logger = logging.getLogger(__name__)  # Logger named after the module
logger.setLevel(logging.DEBUG)

@view_bqtb.route('/table-diff', methods=['POST'])
def compare_tables():
    # Retrieve and parse JSON data from the request
    data = request.get_json()
    # Create a BigQuery client for interaction
    client = bigquery.Client(project=data["project_id"])

    # Validate the provided input data
    validation=validate_input(client,data)
    if "errors" in validation and validation["errors"] != []:
        print(validation)
        return jsonify(validation), 400

    # Initialize empty results for column and row differences
    results = {
        "column_differences": {},
        "row_differences": []
    }

    # Construct a query to find rows deleted from table1 compared to table2
    query_deleted = f"""
            SELECT {", ".join(data['key_columns'])}  
            FROM `{data['project_id']}.{data['dataset_id1']}.{data['table1']}` 
            EXCEPT DISTINCT
            SELECT {", ".join(data['key_columns'])} 
            FROM `{data['project_id']}.{data['dataset_id2']}.{data['table2']}`;
            """

    logger.debug(f"get deleted rows query: {query_deleted}")
    deleted_row_keys = execute_and_fetch_keys(client, query_deleted)

    # Process deleted rows and store differences
    for row in deleted_row_keys:
        key_value = tuple(row)  # Create a tuple from all columns in the row
        results["row_differences"].append(("deleted", key_value,""))  # Assuming a single key column

    # Construct a query to find rows added from table1 compared to table2
    query_added = f"""
            SELECT {", ".join(data['key_columns'])} 
            FROM `{data['project_id']}.{data['dataset_id2']}.{data['table2']}` 
            EXCEPT DISTINCT
            SELECT {", ".join(data['key_columns'])} 
            FROM `{data['project_id']}.{data['dataset_id1']}.{data['table1']}`;
            """
    logger.debug(f"get added rows query: {query_added}")
    added_row_keys = execute_and_fetch_keys(client, query_added)

    # Process added rows and store differences
    for row in added_row_keys:
        key_value = tuple(row)  # Create a tuple from all columns in the row
        results["row_differences"].append(("added", key_value,""))

    # Construct a query to find rows with differences
    query_cell_differences=build_difference_query(data)

    diff_row_keys = execute_and_fetch_keys(client, query_cell_differences)
    for row in diff_row_keys:
        key = tuple(row[:len(data['key_columns'])])
        diff_data = {}
        offset = len(data['key_columns'])  # Offset to start of non-key columns
        for i, col_name in enumerate(data['compair_columns']):
            col_index = offset + i
            table1_value = row[col_index]
            table2_value = row[col_index + len(data['compair_columns'])]
            if table1_value != table2_value:
                diff_data[col_name] = {
                    'original_value': table1_value,
                    'new_value': table2_value
                }
        if diff_data:
            results["row_differences"].append(("difference", key, diff_data))
    return jsonify(results)