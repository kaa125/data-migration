import utility
import transformations
from google.cloud import bigquery
import pandas as pd
import time

# Main Logic
def main():
    # Construct a BigQuery client object.
    # Load GCP credentials from a JSON file.
    gcp_credentials = utility.get_gcp_credentials("./creds/your-gcp-credentials.json")
    bq_client = bigquery.Client(credentials=gcp_credentials, project=gcp_credentials.project_id)
    dataset_id = "your-project-id.your-dataset-id."
    
    # MongoDB client
    mongo_client = utility.connect_mongodb_client()
    db = mongo_client["YourMongoDBDatabaseName"]

    print("Retrieving data and applying transformations...")

    df_retailerusers = transformations.get_retailerusers_df(db)
    print("Retailer users dataframe retrieved")

    df_surveys = transformations.get_surveys_df(db)
    print("Surveys dataframe retrieved")

    df_results = transformations.get_results_df(db)
    print("Results dataframe retrieved")

    # Close the MongoDB connection
    mongo_client.close()

    # Upload jobs

    # Retailer users
    job_config_retailerusers = utility.get_schema_retailerusers()
    table_id = dataset_id + "retailerusers"
    utility.upload_to_bq(bq_client, table_id, df_retailerusers, job_config_retailerusers)

    # Surveys
    job_config_surveys = utility.get_schema_surveys()
    table_id = dataset_id + "surveys"
    utility.upload_to_bq(bq_client, table_id, df_surveys, job_config_surveys)

    # Results
    job_config_results = utility.get_schema_results()
    table_id = dataset_id + "results"
    utility.upload_to_bq(bq_client, table_id, df_results, job_config_results)

if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time() - start
    print("Execution time:", end)
