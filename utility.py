from google.oauth2 import service_account
from pymongo import MongoClient
from google.api_core.exceptions import GoogleAPICallError 
from google.cloud import bigquery

def get_gcp_credentials(creds_fp):
    try:
        # Retrieve GCP credentials from the provided file path
        credentials = service_account.Credentials.from_service_account_file(
        creds_fp,)
        print("GCP credentials retrieved successfully")
    except:
        print("Could not retrieve GCP credentials")
        credentials = None
    return credentials

def connect_mongodb_client():
    try:
        # Connect to the MongoDB instance
        client = MongoClient("")
        print("Connected to MongoDB instance")
    except:
        print("MongoDB instance could not be connected")
        client = None
    return client

def upload_to_bq(bq_client, table_id, df, job_config):
    print(f"Starting load job: {table_id}")

    job = bq_client.load_table_from_dataframe(
    df, table_id, job_config=job_config
    )  # Make an API request.
    
    job.result()  # Wait for the job to complete.
    
    print("Job succeeded")
    print("Fetching results...")
    
    table = bq_client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
    return "Data load complete"

def get_schema_results():
    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            bigquery.SchemaField("result_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("created_at", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("survey_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("updated_at", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("user_id", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("survey_reward", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("question_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("option_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("selected_option", bigquery.enums.SqlTypeNames.STRING),
        ],
        write_disposition="WRITE_TRUNCATE",
    )
    return job_config

def get_schema_retailerusers():
    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            bigquery.SchemaField("user_id", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("survey_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("city_id", bigquery.enums.SqlTypeNames.INTEGER)
        ],
        write_disposition="WRITE_TRUNCATE",
    )
    return job_config

def get_schema_surveys():
    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            bigquery.SchemaField("survey_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("survey_name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("cities_survey_offered", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("survey_created", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("survey_updated", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("survey_started", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("survey_ended", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("survey_status", bigquery.enums.SqlTypeNames.BOOL),
            bigquery.SchemaField("reward_amount", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("survey_url", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("question_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("question_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("question_text", bigquery.enums.SqlTypeNames.STRING),
            
            bigquery.SchemaField("option_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("option_text", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("follow_up_question", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("has_follow_up", bigquery.enums.SqlTypeNames.BOOL),
        ],
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition, it replaces the table with the loaded data.
        write_disposition="WRITE_TRUNCATE",
    )
    return job_config
