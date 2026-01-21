import csv
import apache_beam as beam
from datetime import datetime
from google.cloud import bigquery
from apache_beam.options.pipeline_options import (
    PipelineOptions, 
    GoogleCloudOptions, 
    SetupOptions
)
from apache_beam.io.gcp.bigquery import WriteToBigQuery

client = bigquery.Client()

PROJECT_ID = client.project
DATASET = "users"
TABLE = "customers"
INPUT_FILE = "../bigquery/datasets/customers-1000.csv"


client = bigquery.Client()
project_id = client.project

# Pipeline options
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = project_id
google_cloud_options.job_name = "csv-to-bq-job"
google_cloud_options.region = "us-central1"
google_cloud_options.temp_location = ""
google_cloud_options.staging_location = ""

options.view_as(SetupOptions).save_main_session = True


table_schema = {
    "fields": [
        {"name": "Index", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "Customer_Id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "First_Name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Last_Name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Company", "type": "STRING", "mode": "NULLABLE"},
        {"name": "City", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Country", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Phone_1", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Phone_2", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Email", "type": "STRING", "mode": "REQUIRED"},
        {"name": "Subscription_Date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "Website", "type": "STRING", "mode": "NULLABLE"},
    ]
}

# Parse CSV
def parse_csv(line):
    reader = csv.DictReader([line])
    for row in reader:
        subscription_date = row.get("Subscription_Date")
        if subscription_date:
            try:
                row["Subscription_Date"] = datetime.strptime(subscription_date, "%Y-%m-%d").date().isoformat()
            except Exception:
                row["Subscription_Date"] = None
        return row
    

# Pipeline definition
with beam.Pipeline(options=options) as p:
    (
        p
        | "Read CSV" >> beam.io.ReadFromText(
            "gs://YOUR_BUCKET/datasets/customers-1000.csv", 
            skip_header_lines=1
        )
        | "Parse CSV" >> beam.Map(parse_csv)
        | "Write to BigQuery" >> WriteToBigQuery(
            table=f"{project_id}.users.customers",
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )
    )

print("Pipeline finished successfully!")


