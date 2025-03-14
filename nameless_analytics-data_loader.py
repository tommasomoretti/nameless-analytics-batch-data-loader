# NAMELESS ANALYTICS DATA LOADER 
# PYTHON CODE  

# Template for building CSV
# https://docs.google.com/spreadsheets/d/1RxHfa4KQkciep-xiskgMLITrvxJAntoSnmGkmHYt7ls/edit?gid=1495129113#gid=1495129113

import os
import csv
import json
import time
import traceback
from google.cloud import bigquery


# --------------------------------------------------------------------------------------------------------------


# Configurations
CSV_FILE_PATH = '/Users/tommasomoretti/Documents/Nameless Analytics/Data loader/data_loader-demo_events.csv'
CREDENTIALS_PATH = '/Users/tommasomoretti/Documents/Nameless Analytics/worker_service_account.json'

PROJECT_ID = 'tom-moretti'
DATASET_ID = 'nameless_analytics'
TABLE_ID = 'events'
LOG_TABLE_ID = 'data_loader_logs'

# Logging functions
def log_info(message):
    print(f"{message}")

def log_success(message):
    print(f"  🟢 {message}")

def log_error(message, exception=None):
    print(f"  🔴 {message}")
    if exception:
        print(traceback.format_exc())

# Structure CSV for BigQuery
def prepare_structured_data(csv_file_path):
    log_info(f"👉 Reading data from {csv_file_path}")
    structured_data = []
    try:
        with open(csv_file_path, 'r') as csvfile:
            log_success(f"File {csv_file_path.split('/')[-1]} found.")
            log_info("👉 Structuring payload...")

            reader = csv.DictReader(csvfile)
            for row in reader:
                event_data = [
                    {"name": key.split(".")[1], "value": convert_value(value)}
                    for key, value in row.items() if key.startswith("event_data")
                ]

                consent_data = [
                    {"name": key.split(".")[1], "value": {"string": None if value in ("granted", "denied") else value}}
                    for key, value in row.items() if key.startswith("consent_data")
                ]

                structured_data.append({
                    "event_date": row.get("event_date"),
                    "event_datetime": row.get("event_datetime"),
                    "event_timestamp": row.get("event_timestamp"),
                    "processing_event_timestamp": round(time.time() * 1000),
                    "event_origin": "Batch",
                    "job_id": None,
                    "content_length": row.get("content_length"),
                    "client_id": row.get("client_id"),
                    "user_id": row.get("user_id"),
                    "session_id": row.get("session_id"),
                    "event_name": row.get("event_name"),
                    "event_data": event_data,
                    "consent_data": consent_data
                })
        log_success("Payload structured successfully.")
    except FileNotFoundError:
        log_error(f"File {csv_file_path} not found.")
        raise
    except Exception as e:
        log_error(f"Error during data preparation: {e}")
        raise
    
    return structured_data


# Convert CSV values
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


# Parse JSON
def try_parse_json(value):
    if isinstance(value, str) and value.strip().startswith(('{', '[')):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return None
    return None


# --------------------------------------------------------------------------------------------------------------


# Upload data to BigQuery
def upload_to_bigquery(data, project_id, dataset_id, table_id, log_table_id, credentials_path):
    log_info(f"👉 Uploading data to {project_id}.{dataset_id}.{table_id}...")
    job_id = os.urandom(8).hex()

    upload_status = {
        "success": False,
        "message": "",
        "rows_inserted": 0,
        "execution_time": None
    }

    for item in data:
        item["job_id"] = job_id

    try:
        client = bigquery.Client.from_service_account_json(credentials_path)
        dataset_ref = client.dataset(dataset_id)
        try:
            client.get_dataset(dataset_ref)
        except Exception:
            error_msg = f"Dataset {dataset_id} not found in project {project_id}."
            upload_status["message"] = error_msg
            raise
        
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        try:
            client.get_table(table_ref)
        except Exception:
            error_msg = f"Table {table_id} not found in dataset {dataset_id}."
            upload_status["message"] = error_msg
            raise
        
        start_time = time.time()
        try:
            errors = client.insert_rows_json(table_ref, data)
            if errors:
                error_msg = f"BigQuery insert failed with errors: {json.dumps(errors, indent=2)}"
                upload_status["message"] = error_msg
                log_error(error_msg)
                raise Exception(error_msg)
        except Exception as e:
            error_msg = f"Error during data upload to {table_ref}: {str(e)}"
            upload_status["message"] = error_msg
            log_error(error_msg)
            raise
        
        execution_time = int((time.time() - start_time) * 1000)
        upload_status.update({"success": True, "message": f"Data successfully uploaded to {table_ref}", "rows_inserted": len(data), "execution_time": execution_time})
        log_success(upload_status["message"])
        log_data = {"date": time.strftime("%Y-%m-%d"), "datetime": time.strftime("%Y-%m-%dT%H:%M:%S"), "timestamp": round(time.time() * 1000), "job_id": job_id, "status": "Success" if upload_status["success"] else "Failure", "message": upload_status["message"], "execution_time_micros": upload_status["execution_time"], "rows_inserted": upload_status["rows_inserted"]}
        log_operation(project_id, dataset_id, log_table_id, log_data, credentials_path)
    except Exception as e:
        upload_status["message"] = str(e)
        log_error(f'Error uploading payload to BigQuery {e}')
        raise


# --------------------------------------------------------------------------------------------------------------


# Log function execution in BigQuery
def log_operation(project_id, dataset_id, log_table_id, log_data, credentials_path):
    log_info("👉 Writing job logs...")
    try:
        client = bigquery.Client.from_service_account_json(credentials_path)
        log_table_ref = f"{project_id}.{dataset_id}.{log_table_id}"
        client.insert_rows_json(log_table_ref, [log_data])
        log_success(f"Log successfully written to {log_table_ref}")
    except Exception as e:
        log_error(e)
        raise


# --------------------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    try:
        log_info("----- NAMELESS ANALYTICS -----")
        log_info("--------- DATA LOADER --------")
        log_info("Function execution start: 🤞")
        structured_data = prepare_structured_data(CSV_FILE_PATH)
        upload_to_bigquery(structured_data, PROJECT_ID, DATASET_ID, TABLE_ID, LOG_TABLE_ID, CREDENTIALS_PATH)
        log_info("Function execution end: 👍")
    except Exception as e:
        log_info("Function execution end: 🖕")