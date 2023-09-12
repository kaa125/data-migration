from google.cloud import bigquery
import google.api_core
import psycopg2
import pandas as pd
import time
import pytz
from sqlalchemy import create_engine, exc
import time
import os
import json
import sys

# Reads in the maximum value of replication key (usually id or updated_at) present in the tables in the BQ Warehouse
def read_max(table_id, replication_key):
    try:
        if "on" in replication_key.lower() or "at" in replication_key.lower():
            # This is to compensate for the time zone difference in which time is recorded in Postgres and BQ
            query = "Select TIMESTAMP_ADD(max("+ replication_key +"), INTERVAL 5 HOUR) FROM " + table_id + ";"
        elif "id" in replication_key.lower():
            query = "SELECT max(" + replication_key + ") FROM " + table_id + ";"
        print("read_max::query:", query)
        query_job = client.query(query)
        for row in query_job:
            max = row[0]
        print("read_max::result:", max)

        # Handling case when table exists, but no data
        if max == None and "id" in replication_key.lower():
            print("read_max::No data in table. Returning default value: 0")
            return 0
        elif max == None and ("on" in replication_key.lower() or "at" in replication_key.lower()):
            print("read_max::No data in table. Returning default value: 2020-01-01")
            return "2020-01-01 00:00:00.000"

        return max
    # Handling case when table does not exist
    except google.api_core.exceptions.NotFound as e:
        print("read_max::exception:", e)
        print("read_max::Table does not exist. Creating table...")
        if "id" in replication_key.lower():
            return 0
        elif "on" in replication_key.lower() or "at" in replication_key.lower():
            return "2020-01-01 00:00:00.000"

# Fetch the relevant data from the Amazon RDS Database
def fetch_data(AWSTable, BQTable, replication_key, min, limit):
    offset = 0
    incoming_data = True
    retries = 0
    where_clause = ""
    order_by = "id"
    if "on" in replication_key.lower() or "at" in replication_key.lower():
        min = str(min).split("+")[0]
        where_clause = replication_key + " > '" + str(min) + "'"
    elif "id" in replication_key.lower():
        where_clause = replication_key + " > " + str(min)

    while incoming_data:
        query = "SELECT * FROM " + AWSTable + " WHERE " + where_clause + \
                " ORDER BY " + "id" +  " ASC LIMIT " + str(limit) + " OFFSET " + str(offset) + ";"
        print("fetch_data::query:", query)
        # ERROR PRONE LINE -- TALK TO DBA to enable hot_standby = 1 OR ..

        try:
            print("fetch_data::Fetching " + str(limit) + " records from " + AWSTable + ". Row:" + str(offset+1) + " to Row: " + str(offset+limit) )
            start = time.time()
            records = pd.read_sql(query, conn)
            end = time.time()
            print("fetch_data::Time taken for fetching " + str(records.shape[0]) + " records: ", end - start, "s")
            if records.empty:
                incoming_data = False
            if incoming_data:
                load_data(BQTable, records)
            offset += limit
            retries = 0

        except exc.OperationalError as e:
            print("fetch_data::exception", e)
            if retries == 6:
                print("fetch_data::Max retries attempted. Terminating program")
                conn.close()
                sys.exit()
            retries+=1
            print("fetch_data::Retrying ...")

        except Exception as e:
            print("fetch_data::exception",e)
            conn.close()
            sys.exit()

# Loads the data in the Google BQ Warehouse
def load_data(table_id, data):
    print("load_data::Writing records to table", table_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        # schema=[
        #     bigquery.SchemaField("__deal_id", "INT64"),
        #     bigquery.SchemaField("commission_value", "INT64"),
        #     bigquery.SchemaField("created_at", "TIMESTAMP"),
        #     bigquery.SchemaField("delta_return", "INT64"),
        #     bigquery.SchemaField("discount", "FLOAT64"),
        #     bigquery.SchemaField("id", "INT64"),
        #     bigquery.SchemaField("mrp", "FLOAT64"),
        #     bigquery.SchemaField("order_id", "INT64"),
        #     bigquery.SchemaField("paid_seller_amount", "FLOAT64"),
        #     bigquery.SchemaField("pickable_qty", "INT64"),
        #     bigquery.SchemaField("picked_qty", "INT64"),
        #     bigquery.SchemaField("pickup_at", "TIMESTAMP"),
        #     bigquery.SchemaField("price", "FLOAT64"),
        #     bigquery.SchemaField("qty", "INT64"),
        #     bigquery.SchemaField("reason", "STRING"),
        #     bigquery.SchemaField("reason_id", "INT64"),
        #     bigquery.SchemaField("received_date", "TIMESTAMP"),
        #     bigquery.SchemaField("return_qty", "INT64"),
        #     bigquery.SchemaField("seller_id", "INT64"),
        #     bigquery.SchemaField("seller_status", "INT64"),
        #     bigquery.SchemaField("sku_id", "INT64"),
        #     bigquery.SchemaField("status", "INT64"),
        #     bigquery.SchemaField("tag", "INT64")
        # ],
    )
    try:
        start = time.time()
        job = client.load_table_from_dataframe(
            data, table_id, job_config=job_config
        )
        job.result()
        end = time.time()
        print("load_data::Time taken for writing " + str(data.shape[0]) + " records: ", end - start, "s")

    except Exception as e:
        print("load_data::exception", e)
        print("load_data::Could not establish connection with Google BigQuery. Terminating program")
        conn.close()
        sys.exit()

# Load configurations from JSON file
with open('configurations.json') as config_file:
    config = json.load(config_file)

# Set Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["GOOGLE_APPLICATION_CREDENTIALS"]

# Initialize BigQuery client
client = bigquery.Client()

# Create a database engine for PostgreSQL
engine = create_engine(config["connection_string"])

# Connect to the PostgreSQL database
conn = engine.connect()

# Load schema information from JSON file
with open('schema.json') as schema_file:
    schema = json.load(schema_file)

# Iterate through tables in the schema
for table in schema['tables']:
    print("Reading max replication key from table: ", table["googleBQ"])
    max = read_max(table["googleBQ"], table["replicationKey"])
    print("Fetching new data from ", table["amazonRDS"])
    fetch_data(table["amazonRDS"], table["googleBQ"], table["replicationKey"], max, table["limit"])

# Close the PostgreSQL connection
conn.close()
