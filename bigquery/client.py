from google.cloud import bigquery

# Initialize client (uses GOOGLE_APPLICATION_CREDENTIALS)
client = bigquery.Client()


project_id = client.project
dataset_id = "{}.users".format(project_id)
table_id = "customers" # main_table_id
staging_table_id = f"{dataset_id}.staging_load"


""" To create a dataset """
dataset = bigquery.Dataset(dataset_id)
dataset.location = "US"
dataset = client.create_dataset(dataset, timeout=30)
print("Created dataset- ", client.project, dataset.dataset_id)



table_ref = f"{dataset_id}.{table_id}"

""" Create an empty table without a schema definition """
table = bigquery.Table(table_ref)
table = client.create_table(table)

print(
    f"Created empty table {table.project}.{table.dataset_id}.{table.table_id}"
)




""" Create an empty table with a schema definition """
# schema = [
#     bigquery.SchemaField("full_name", "STRING", mode="REQUIRED"),
#     bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
# ]

# table = bigquery.Table(table_ref, schema=schema)
# table = client.create_table(table)

# print(
#     f"Created table {table.project}.{table.dataset_id}.{table.table_id}"
# )


""" To add schema after created a table """
schema = [
    # bigquery.SchemaField("Id", "INTEGER", mode="REQUIRED"),  # auto-increment style
    bigquery.SchemaField("Index", "INTEGER"),
    bigquery.SchemaField("Customer_Id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("First_Name", "STRING"),
    bigquery.SchemaField("Last_Name", "STRING"),
    bigquery.SchemaField("Company", "STRING"),
    bigquery.SchemaField("City", "STRING"),
    bigquery.SchemaField("Country", "STRING"),
    bigquery.SchemaField("Phone_1", "STRING"),
    bigquery.SchemaField("Phone_2", "STRING"),
    bigquery.SchemaField("Email", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Subscription_Date", "DATE"),
    bigquery.SchemaField("Website", "STRING"),
]
# To get information about tables
table = client.get_table(table_ref) # To get information about tables
table.schema = schema
table = client.update_table(table, ["schema"])

"""
table now has a schema with an auto-increment Id, 
the simplest way to add CSV records is to load the CSV into a temporary staging table and 
then insert into your main table using ROW_NUMBER() to populate the Id.
"""
# Load CSV into a staging table
job_config = bigquery.LoadJobConfig(
    source_format = bigquery.SourceFormat.CSV,
    skip_leading_rows = 1,
    # autodetect=True
    write_disposition="WRITE_APPEND"
)

with open("datasets/customers-1000.csv", "rb") as source_file:
    load_job = client.load_table_from_file(
        source_file,
        table_ref,
        job_config=job_config
    )
load_job.result()


# # Insert into main table with auto-increment Id
# query = f"""
# INSERT INTO `{table_id}` (
#     Id, Customer_Id, First_Name, Last_Name, Company,
#     City, Country, Phone_1, Phone_2, Email, Subscription_Date, Website
# )
# WITH max_id AS (
#     SELECT COALESCE(MAX(Id), 0) AS last_id FROM `{table_id}`
# )
# SELECT 
#     ROW_NUMBER() OVER() + last_id AS Id,
#     Customer_Id,
#     First_Name,
#     Last_Name,
#     Company,
#     City,
#     Country,
#     Phone_1,
#     Phone_2,
#     Email,
#     SAFE.PARSE_DATE('%Y-%m-%d', Subscription_Date) AS Subscription_Date,
#     Website
# FROM `{staging_table_id}`, max_id
# """

# query_job = client.query(query)
# query_job.result()
# print(f"CSV data inserted into main table {table_id} with auto-increment Id")



# Not required to add auto increament key so direct load into main table