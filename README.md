![Na logo beta](https://github.com/tommasomoretti/nameless-analytics/assets/29273232/7d4ded5e-4b79-46a2-b089-03997724fd10)

---

# Data loader
The Nameless Analytics data loader is a python script that manages and loads data from a CSV file.
- Use [this script](#from-a-local-machine-or-a-server-to-google-bigquery) to load data from a local machine or a server to Google BigQuery
- Use [this script](#from-a-google-cloud-function-to-google-bigquery) to load data from a Google Cloud Function to Google BigQuery

Here an example in Google Sheets for structuring the data correctly, the CSV template is exported from here:
- [Google Sheets template](https://docs.google.com/spreadsheets/d/1RxHfa4KQkciep-xiskgMLITrvxJAntoSnmGkmHYt7ls/)
- [CSV template](https://github.com/user-attachments/files/18222451/nameless-analytics.csv)

## Load data into main table
### From a local machine or a remote server to Google BigQuery
Ensure that the Python Client for Google BigQuery is installed before executing the script. 

```terminal
pip install --upgrade google-cloud-bigquery
```

```py 
import os
import csv
import json
import time
from google.cloud import bigquery

# Configurations
CREDENTIALS_PATH = 'credentials-path' # Change it according to the credential path in your local machine or remote server 
CSV_FILE_PATH = 'csv-file-path' # Change it according to the csv path in your local machine or remote server 
PROJECT_ID = 'project-id' # Change it according to your project
DATASET_ID = 'dataset-id' # Change it according to Nameless Analytics dataset
TABLE_ID = 'table-id' # Change it according to Nameless Analytics main table

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
This version of the Nameless Analytics data loader, take the csv file from a Google Cloud Storage bucket and insert it in the Nameless Analytics main table. The Google Cloud Function will be triggered by a Google Cloud Storage event, running automatically whenever a file is uploaded to a specified bucket. 

Create a [new Google Cloud Storage bucket](https://console.cloud.google.com/storage/). 

Create a [new Google Cloud Funtion](https://console.cloud.google.com/functions/) (Gen 1) as described above.

<img width="1512" alt="Screenshot 2024-12-28 alle 14 47 06" src="https://github.com/user-attachments/assets/ed0df3b0-8821-46c2-8e66-22b2cf103789" />

In the next step, select Python 3.12 as runtime 

<img width="1512" alt="Screenshot 2024-12-28 alle 14 56 23" src="https://github.com/user-attachments/assets/2cee3899-7013-4071-ac7e-3e8701fb44ad" />

Edit the file named main.py with this script:

```py 
import csv
import json
import time
from google.cloud import bigquery, storage


# Configurations
PROJECT_ID = 'project-id' # Change it according to your project
DATASET_ID = 'dataset-id' # Change it according to Nameless Analytics dataset
TABLE_ID = 'table-id' # Change it according to Nameless Analytics main table

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

Edit the requirements.txt file with this lines:

```
google-cloud-bigquery>=3.11.4
google-cloud-storage>=2.13.2
```

Save the Google Cloud Function and load a valid csv file in the Google Cloud Storage bucket configured in the trigger section.

<img width="1512" alt="Screenshot 2024-12-28 alle 14 58 38" src="https://github.com/user-attachments/assets/04992ee5-6ca2-4fa9-9ee7-c5c028b08452" />

