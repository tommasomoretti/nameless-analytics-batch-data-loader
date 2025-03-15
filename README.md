![Na logo beta](https://github.com/tommasomoretti/nameless-analytics/assets/29273232/7d4ded5e-4b79-46a2-b089-03997724fd10)

---

# Data loader
The Nameless Analytics Data Loader is a python script that manages and loads data from a CSV file.

For an overview of how Nameless Analytics works [start from here](https://github.com/tommasomoretti/nameless-analytics).

Start from here:
- [Preparing data](#preparing-data)
- [load data from a local machine or a server](#from-a-local-machine-or-a-remote-server-to-google-bigquery) to Google BigQuery
- [load data from a Google Cloud Function](#from-a-google-cloud-function-to-google-bigquery) to Google BigQuery



## Preparing data
Here an [example in Google Sheets](https://docs.google.com/spreadsheets/d/1RxHfa4KQkciep-xiskgMLITrvxJAntoSnmGkmHYt7ls/) for structuring the data correctly, the CSV template example file is exported from here.



## From a local machine or a remote server to Google BigQuery
This version of the Nameless Analytics Data Loader, take a csv file from a folder and inserts it in Nameless Analytics main table. 

Ensure that the Python Client for Google BigQuery is installed before executing the script. 

```terminal
pip install --upgrade google-cloud-bigquery
```


## From a Google Cloud Function to Google BigQuery
This version of the Nameless Analytics Data Loader, take a csv file from a Google Cloud Storage bucket and inserts it in Nameless Analytics main table. The Google Cloud Function will be triggered by a Google Cloud Storage event, running automatically whenever a file is uploaded to a specified bucket. 

Create a [new Google Cloud Storage bucket](https://console.cloud.google.com/storage/). 

Create a [new Google Cloud Funtion](https://console.cloud.google.com/functions/) (Gen 1) as described above.

<img width="1512" alt="Screenshot 2024-12-28 alle 14 47 06" src="https://github.com/user-attachments/assets/ed0df3b0-8821-46c2-8e66-22b2cf103789" />

In the next step, select Python 3.12 as runtime 

<img width="1512" alt="Screenshot 2024-12-28 alle 14 56 23" src="https://github.com/user-attachments/assets/2cee3899-7013-4071-ac7e-3e8701fb44ad" />

Edit the file named main.py with this script:

Edit the requirements.txt file with this lines:

```
google-cloud-bigquery>=3.11.4
google-cloud-storage>=2.13.2
```

Save the Google Cloud Function and load a valid csv file in the Google Cloud Storage bucket configured in the trigger section.

<img width="1512" alt="Screenshot 2024-12-28 alle 14 58 38" src="https://github.com/user-attachments/assets/04992ee5-6ca2-4fa9-9ee7-c5c028b08452" />

