# nameless-analytics-data-loader

Load event data from structured csv.
- [CSV template](https://github.com/user-attachments/files/18222451/nameless-analytics.csv)
- [Google Sheets template](https://docs.google.com/spreadsheets/d/1RxHfa4KQkciep-xiskgMLITrvxJAntoSnmGkmHYt7ls/) 

```py 
import csv
import json
import time
from google.cloud import bigquery

# Configuration
CREDENTIALS_PATH = '/Users/tommasomoretti/Desktop/nameless_analytics/tom-moretti-1cbb44553fc3.json'
CSV_FILE_PATH = '/Users/tommasomoretti/Desktop/nameless_analytics/nameless-analytics.csv'
PROJECT_ID = 'tom-moretti'
DATASET_ID = 'nameless_analytics'
TABLE_ID = 'events'

def prepare_structured_data(csv_file_path):
    structured_data = []
    try:
        with open(csv_file_path, 'r') as csvfile:
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

    except FileNotFoundError:
        raise FileNotFoundError(f"File {csv_file_path} not found.")
    except Exception as e:
        raise Exception(f"Error during data preparation: {e}")

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
            raise Exception(f"Upload failed with errors: {errors}")

        print(f"🟢 Data successfully uploaded to the table: {table_ref}")

    except FileNotFoundError:
        raise FileNotFoundError(f"Error: The credentials file {credentials_path} was not found.")
    except Exception as e:
        raise Exception(f"Error during upload to BigQuery: {e}")


if __name__ == "__main__":
    try:
        structured_data = prepare_structured_data(CSV_FILE_PATH)
        
        print('NAMELESS ANALYTICS')
        print(f'👉🏻 Read data from {CSV_FILE_PATH}')
        print('👉🏻 Data to upload: ' +  json.dumps(structured_data, indent=2))
        print(f'👉🏻 Upload data to {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}')

        upload_to_bigquery(structured_data, PROJECT_ID, DATASET_ID, TABLE_ID, CREDENTIALS_PATH)
    except Exception as e:
        print(f"🔴 {e}")
```
