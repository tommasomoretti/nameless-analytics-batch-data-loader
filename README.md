<img src="https://github.com/user-attachments/assets/93640f49-d8fb-45cf-925e-6b7075f83927#gh-light-mode-only" alt="Light Mode" />
<img src="https://github.com/user-attachments/assets/71380a65-3419-41f4-ba29-2b74c7e6a66b#gh-dark-mode-only" alt="Dark Mode" />

---

# Batch data loader
The Nameless Analytics Batch data loader is a python script that manages and loads data to [Nameless Analytics Event raw table](https://github.com/tommasomoretti/nameless-analytics-tables/#events-raw-table) from a CSV file.

For an overview of how Nameless Analytics works [start from here](https://github.com/tommasomoretti/nameless-analytics/).

Table of contents:
- [Preparing data](#preparing-data)
- [Load data from local machine or remote server](#load-data-from-local-machine-or-remote-server)
- [load data from Google Cloud Function](#load-data-from-google-cloud-function)



## Preparing data
Here an [example in Google Sheets](https://docs.google.com/spreadsheets/d/1RxHfa4KQkciep-xiskgMLITrvxJAntoSnmGkmHYt7ls/) for structuring the data correctly, the CSV template example file is exported from here.



## Load data from local machine or remote server
This version of the Nameless Analytics Data Loader, take a csv file from a folder and inserts it in Nameless Analytics main table. 

Please note: Python client for Google BigQuery must be installed before execute the script. 

```terminal
pip install --upgrade google-cloud-bigquery
```


## Load data from Google Cloud Function
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
