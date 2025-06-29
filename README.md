<picture>
  <source srcset="https://github.com/user-attachments/assets/6af1ff70-3abe-4890-a952-900a18589590" media="(prefers-color-scheme: dark)">
  <img src="https://github.com/user-attachments/assets/9d9a4e42-cd46-452e-9ea8-2c03e0289006">
</picture>

---

# Batch data loader
The Nameless Analytics Batch data loader is a python script that manages and loads data to [Nameless Analytics Event raw table](https://github.com/tommasomoretti/nameless-analytics-tables/#events-raw-table) from a CSV file.

For an overview of how Nameless Analytics works [start from here](https://github.com/tommasomoretti/nameless-analytics/).

Table of contents:
- [Preparing data](#preparing-data)
- [Load data from local machine or remote server](#load-data-from-local-machine-or-remote-server)
- [load data from Google Cloud Function](#load-data-from-google-cloud-function) to Google BigQuery



## Preparing data
Here an [example in Google Sheets](https://docs.google.com/spreadsheets/d/1RxHfa4KQkciep-xiskgMLITrvxJAntoSnmGkmHYt7ls/) for structuring the data correctly, the CSV template example file is exported from here.



## Load data from a local machine or a remote server to Google BigQuery
This version of the Nameless Analytics Data Loader, take a csv file from a folder and inserts it in Nameless Analytics main table. 

Please note: Python client for Google BigQuery must be installed before execute the script. 

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

---

Reach me at: [Email](mailto:hello@tommasomoretti.com) | [Website](https://tommasomoretti.com/?utm_source=github.com&utm_medium=referral&utm_campaign=nameless_analytics) | [Twitter](https://twitter.com/tommoretti88) | [Linkedin](https://www.linkedin.com/in/tommasomoretti/)
