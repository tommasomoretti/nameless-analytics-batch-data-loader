![Na logo beta](https://github.com/tommasomoretti/nameless-analytics/assets/29273232/7d4ded5e-4b79-46a2-b089-03997724fd10)

# Data loader
The Nameless Analytics data loader is a python script that manages and loads data from a CSV file.
- Use [this script](#from-a-local-machine-or-a-server-to-google-bigquery) to load data from a local machine or a server to Google BigQuery
- Use [this script](#from-a-cloud-function-to-google-bigquery) to load data from a Google Cloud Function to Google BigQuery

Here an example in Google Sheets for structuring the data correctly, the CSV template is exported from here:
- [Google Sheets template](https://docs.google.com/spreadsheets/d/1RxHfa4KQkciep-xiskgMLITrvxJAntoSnmGkmHYt7ls/)
- [CSV template](https://github.com/user-attachments/files/18222451/nameless-analytics.csv)

## Load data into main table
### From a local machine or a server to Google BigQuery

```py 
import os
import csv
import json
import time
from google.cloud import bigquery

# Configurations
CREDENTIALS_PATH = '/Users/tommasomoretti/Desktop/nameless_analytics/tom-moretti-1cbb44553fc3.json'
CSV_FILE_PATH = '/Users/tommasomoretti/Desktop/nameless_analytics/documentazione/code/nameless-analytics.csv'
PROJECT_ID = 'tom-moretti'
DATASET_ID = 'nameless_analytics'
TABLE_ID = 'events'

def prepare_structured_data(csv_file_path):
    structured_data = []
    try:
        with open(csv_file_path, 'r') as csvfile:
            print(f'🟢 File {CSV_FILE_PATH.split('/')[-1]} found.')
            print(f'👉🏻 Structuring payload...')
            reader = csv.DictReader(csvfile)

            for row in reader:
                event_data = [
                    {
                        "name": key.split(".")[1],
                        "value": convert_value(value)
                    }
                    for key, value in row.items() if key.startswith("event_data")
                ]

                consent_data = [
                    {
                        "name": key.split(".")[1],
                        "value": {
                            "string": None if value in ("granted", "denied") else value,
                            "bool": value == "granted"
                        }
                    }
                    for key, value in row.items() if key.startswith("consent_data")
                ]

                structured_data.append({
                    "event_date": row.get("event_date") or None,
                    "event_datetime": row.get("event_datetime") or None,
                    "event_timestamp": row.get("event_timestamp") or None,
                    "received_event_timestamp": round(time.time() * 1000),
                    "event_origin": "Batch",
                    "content_length": row.get("content_length") or None,
                    "client_id": row.get("client_id") or None,
                    "user_id": row.get("user_id") or None,
                    "session_id": row.get("session_id") or None,
                    "event_name": row.get("event_name") or None,
                    "event_data": event_data,
                    "consent_data": consent_data
                })
        print(f'🟢 Payload structured.')

    except FileNotFoundError:
        raise FileNotFoundError(f"🔴 File {csv_file_path} not found.")
    except Exception as e:
        raise Exception(f"🔴 Error during data preparation: {e}")

    return structured_data

def convert_value(value):
    if value.isdigit():
        return {"int": int(value), "float": None, "string": None, "json": None}
    try:
        return {"int": None, "float": float(value), "string": None, "json": None}
    except ValueError:
        parsed_json = try_parse_json(value)
        if parsed_json:
            return {"int": None, "float": None, "string": None, "json": json.dumps(parsed_json)}
    return {"int": None, "float": None, "string": value or None, "json": None}

def try_parse_json(value):
    if isinstance(value, str) and value.strip().startswith(('{', '[')):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return None
    return None

def upload_to_bigquery(data, project_id, dataset_id, table_id, credentials_path):
    try:
        client = bigquery.Client.from_service_account_json(credentials_path)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        errors = client.insert_rows_json(table_ref, data)
        if errors:
            raise Exception(f"🔴 Data insertion failed with errors: {errors}")

        print(f"🟢 Data successfully uploaded to table: {table_ref}")

    except FileNotFoundError:
        raise FileNotFoundError(f"🔴 Error during authentication: Credentials file {credentials_path} not found.")
    except Exception as e:
        raise Exception(f"🔴 Error during BigQuery upload: {e}")

if __name__ == "__main__":
    try:
        print('----- NAMELESS ANALYTICS -----')
        print('--------- DATA LOADER --------')
        print('')
        print(f'👉🏻 Reading data from {CSV_FILE_PATH}')

        structured_data = prepare_structured_data(CSV_FILE_PATH)       
        
        # print('👉🏻 Data to upload: ' +  json.dumps(structured_data, indent=2))
        print(f'👉🏻 Uploading data to {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}...')

        upload_to_bigquery(structured_data, PROJECT_ID, DATASET_ID, TABLE_ID, CREDENTIALS_PATH)
    except Exception as e:
        print(f"{e}")
```

### From a Google Cloud Function to Google BigQuery

```py 
import csv
import json
import time
from google.cloud import bigquery, storage


# Configurations
PROJECT_ID = 'tom-moretti'
DATASET_ID = 'nameless_analytics'
TABLE_ID = 'events'

def download_blob(bucket_name, source_blob_name):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        return blob.download_as_text()
    except Exception as e:
        raise Exception(f"🔴 Error during file download from Google Cloud Storage: {e}")

def prepare_structured_data(csv_content):
    structured_data = []
    try:
        print(f'👉🏻 Structuring payload...')
        reader = csv.DictReader(csv_content.splitlines())

        for row in reader:
            event_data = [
                {
                    "name": key.split(".")[1],
                    "value": convert_value(value)
                }
                for key, value in row.items() if key.startswith("event_data")
            ]

            consent_data = [
                {
                    "name": key.split(".")[1],
                    "value": {
                        "string": None if value in ("granted", "denied") else value,
                        "bool": value == "granted"
                    }
                }
                for key, value in row.items() if key.startswith("consent_data")
            ]

            structured_data.append({
                "event_date": row.get("event_date") or None,
                "event_datetime": row.get("event_datetime") or None,
                "event_timestamp": row.get("event_timestamp") or None,
                "received_event_timestamp": round(time.time() * 1000),
                "event_origin": "Batch",
                "content_length": row.get("content_length") or None,
                "client_id": row.get("client_id") or None,
                "user_id": row.get("user_id") or None,
                "session_id": row.get("session_id") or None,
                "event_name": row.get("event_name") or None,
                "event_data": event_data,
                "consent_data": consent_data
            })
        print(f'🟢 Payload structured.')

    except Exception as e:
        raise Exception(f"🔴 Error during data preparation: {e}")

    return structured_data

def convert_value(value):
    if value.isdigit():
        return {"int": int(value), "float": None, "string": None, "json": None}
    try:
        return {"int": None, "float": float(value), "string": None, "json": None}
    except ValueError:
        parsed_json = try_parse_json(value)
        if parsed_json:
            return {"int": None, "float": None, "string": None, "json": json.dumps(parsed_json)}
    return {"int": None, "float": None, "string": value or None, "json": None}

def try_parse_json(value):
    if isinstance(value, str) and value.strip().startswith(("{", "[")):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return None
    return None

def upload_to_bigquery(data, project_id, dataset_id, table_id):
    try:
        client = bigquery.Client()
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        errors = client.insert_rows_json(table_ref, data)
        if errors:
            raise Exception(f"🔴 Data insertion failed with errors: {errors}")

        print(f"🟢 Data successfully uploaded to table: {table_ref}")

    except Exception as e:
        raise Exception(f"🔴 Error during BigQuery upload: {e}")

def main(event, context):
    try:
        bucket_name = event["bucket"]
        file_name = event["name"]

        print('----- NAMELESS ANALYTICS -----')
        print('--------- DATA LOADER --------')
        print('')
        print(f'👉🏻 Reading data from gs://{bucket_name}/{file_name}')

        csv_content = download_blob(bucket_name, file_name)
        structured_data = prepare_structured_data(csv_content)

        # print(f'👉🏻 Data to upload: {json.dumps(structured_data, indent=2)}')
        print(f'👉🏻 Uploading data to {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}')

        upload_to_bigquery(structured_data, PROJECT_ID, DATASET_ID, TABLE_ID)

    except Exception as e:
        print(f"{e}")
```
