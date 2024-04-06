# BigQuery Table Comparison Flask API  

Python-based utility to automate the comparison of two BigQuery tables, exposed as a Flask API. Identifies mismatched data, schema discrepancies, added/deleted rows, and validates table existence and key uniqueness.


## Key Features

* Identifies mismatches in both data and schema.
* Detects added, deleted, and modified rows based on key columns.
* Supports comparison of specific columns.
* Provides a user-friendly API interface.

## Prerequisites

* Python 3.x 
* Flask
* Google Cloud BigQuery Python client library (`google-cloud-bigquery`)
* Access to a Google Cloud Project with BigQuery API enabled

## Installation - change to the correct python version

```bash
bash build_venv.sh 
```
## Usage

**1. Set Environment Variables:**

   * `GOOGLE_APPLICATION_CREDENTIALS`: Path to your Google Cloud service account credentials JSON file.

**2. Start the Flask application:**

   ```bash
   export FLASK_APP=app.py flask run
```
**3. Send a POST request to the `/table-diff` endpoint: (see run_example.py)**

```json
{
    "project_id": "your-project-id",
    "dataset_id1": "dataset1",
    "dataset_id2": "dataset2",
    "table1": "table1_name",
    "table2": "table2_name",
    "key_columns": ["key_column1", "key_column2"],
    "compare_columns": ["column1", "column2"]
}
```
## To Do
* add option to ouput to a bigquery table

