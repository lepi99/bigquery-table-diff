import logging
logger_queries = logging.getLogger(__name__)  # Logger named after the module
logger_queries.setLevel(logging.DEBUG)

def execute_and_fetch_keys(client,query_string):
    """
    Executes a query and returns rows object
    """
    query_job = client.query(query_string)
    rows  = query_job.result()

    return rows

def build_unique_query(table,columns):
    querys=f"""SELECT
    {', '.join(f'{col}' for col in columns)}
    FROM `{table}` AS table
    GROUP BY {', '.join(f'{col}' for col in columns)}
    HAVING COUNT(*) > 1;
    """
    logger_queries.debug(f"Unique row query for {table.table_id}: {querys}")
    return querys

def build_difference_query(data):
    columns = data["key_columns"] + data["compair_columns"]

    case_statements = [
        f"CASE WHEN table1.{col} <> table2.{col} THEN 'modified' ELSE NULL END AS {col}_status"
        for col in data["compair_columns"]
    ]

    where_conditions = [
        f"table1.{col} <> table2.{col}" for col in data["compair_columns"]
    ]

    query = f"""
        SELECT 
            {', '.join(f'table1.{col}' for col in columns)} ,
            {', '.join(f'table2.{col}' for col in data["compair_columns"])} ,
            {', '.join(case_statements)} 
        FROM `{data['project_id']}.{data['dataset_id1']}.{data['table1']}` AS table1
        INNER JOIN `{data['project_id']}.{data['dataset_id2']}.{data['table2']}` AS table2 
            ON {' AND '.join(f'table1.{col} = table2.{col}' for col in data['key_columns'])}
        WHERE {' OR '.join(where_conditions)} 
    """
    logger_queries.debug(f"Build difference query for table 1-{data['dataset_id1']}.{data['table1']} table 2-{data['dataset_id2']}.{data['table2']}: {query}")
    return query
