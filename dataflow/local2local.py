import csv
from datetime import datetime
import apache_beam as beam

INPUT_FILE = "../bigquery/datasets/customers-1000.csv"


# CSV parsing function
def parse_csv(line):
    reader = csv.DictReader([line], fieldnames=[
        "Index", "Customer_Id", "First_Name", "Last_Name", "Company",
        "City", "Country", "Phone_1", "Phone_2", "Email",
        "Subscription_Date", "Website"
    ])
    for row in reader:
        # Convert date field
        sub_date = row.get("Subscription_Date")
        if sub_date:
            try:
                row["Subscription_Date"] = datetime.strptime(sub_date, "%Y-%m-%d").date().isoformat()
            except:
                row["Subscription_Date"] = ""
        return row

# Convert dict to CSV line
def dict_to_csv(row):
    return ",".join([row.get(col, "") for col in [
        "Index", "Customer_Id", "First_Name", "Last_Name", "Company",
        "City", "Country", "Phone_1", "Phone_2", "Email",
        "Subscription_Date", "Website"
    ]])

# Pipeline
with beam.Pipeline() as p:
    (
        p
        | "Read CSV" >> beam.io.ReadFromText(INPUT_FILE, skip_header_lines=1)
        | "Parse CSV" >> beam.Map(parse_csv)
        | "Write CSV" >> beam.io.WriteToText(
            "output/customers_processed",
            file_name_suffix=".csv",
            header="Index,Customer_Id,First_Name,Last_Name,Company,City,Country,Phone_1,Phone_2,Email,Subscription_Date,Website"
        )
    )

print("Pipeline finished! Output is in 'output/' folder.")
