import csv
import apache_beam as beam
from datetime import datetime
from google.cloud import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery

client = bigquery.Client()

PROJECT_ID = client.project
DATASET = "users"
TABLE = "customers_new"
INPUT_FILE = "../bigquery/datasets/customers-1000.csv"

SCHEMA = {
    "fields": [
        {"name": "Index", "type": "INTEGER"},
        {"name": "Customer_Id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "First_Name", "type": "STRING"},
        {"name": "Last_Name", "type": "STRING"},
        {"name": "Company", "type": "STRING"},
        {"name": "City", "type": "STRING"},
        {"name": "Country", "type": "STRING"},
        {"name": "Phone_1", "type": "STRING"},
        {"name": "Phone_2", "type": "STRING"},
        {"name": "Email", "type": "STRING", "mode": "REQUIRED"},
        {"name": "Subscription_Date", "type": "DATE"},
        {"name": "Website", "type": "STRING"},
    ]
}

# Parse CSV line → dict
def parse_csv(line):
    reader = csv.DictReader([line], fieldnames=[
        "Index", "Customer_Id", "First_Name", "Last_Name", "Company",
        "City", "Country", "Phone_1", "Phone_2", "Email",
        "Subscription_Date", "Website"
    ])

    for row in reader:
        # Convert types
        row["Index"] = int(row["Index"]) if row["Index"] else None

        if row["Subscription_Date"]:
            try:
                row["Subscription_Date"] = datetime.strptime(
                    row["Subscription_Date"], "%Y-%m-%d"
                ).date().isoformat()
            except Exception:
                row["Subscription_Date"] = None

        return row

"""
"options" define how and where the pipeline runs
    - DirectRunner → runs locally
    - DataflowRunner → runs on Google Cloud Dataflow
"""
options = PipelineOptions(
    runner="DirectRunner",
    project=PROJECT_ID,
    save_main_session=True,
)

# p is like, A data processing graph where define steps (transforms)
with beam.Pipeline(options=options) as p:
    (
        p
        | "Read CSV (Local)" >> beam.io.ReadFromText(
            INPUT_FILE, skip_header_lines=1
        )
        | "Parse CSV" >> beam.Map(parse_csv)
        | "Write to BigQuery" >> WriteToBigQuery(
            table=f"{PROJECT_ID}:{DATASET}.{TABLE}",
            schema=SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, # Create table if missing
            method="STREAMING_INSERTS",
        )
    )

print("Local CSV loaded into BigQuery successfully")
# Access Denied: BigQuery BigQuery: Streaming insert is not allowed in the free tier
