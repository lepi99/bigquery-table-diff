import requests

# Base URL for your BigQuery table diff Flask API
url = "https://ip:8080/api/v1/tables/table-diff"

# Dictionary to store multiple comparison profiles.
tables_profiles={
    "profile1":{
    "project_id": "project_id", # Project ID where the datasets and tables are located   
    "dataset_id1": "dataset_id1", # IDs of the datasets containing the tables to compare
    "dataset_id2": "dataset_id2",
    "table1": "table_name1", # Names of the tables to compare within the datasets
    "table2": "table_name2",
    "key_columns": ["keyColumn1","keyColumn2"], # Names of the columns to use as unique keys for comparison
    "compair_columns": ["column2","column3"] # Names of additional columns to compare for differences
    }
}

# Select the profile to run
profile="profile1"
print(f"running {profile}")

# Retrieve configuration for the selected profile
data = tables_profiles[profile]

# Send a POST request to the API endpoint
response = requests.post(url, json=data, verify=False)
print(response)
if response.status_code == 200:
    results = response.json()
    print(results)  # Print the differences returned by your API
else:
    response_data = response.json()
    message = response_data.get("errors")
    print(message)
    print(f"Request failed with status code: {response.status_code}")