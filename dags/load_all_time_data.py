import os
import json
import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime, from_timestamp

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define DAG parameters
@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["cricket-data-extraction"],
)
def load_all_time_data():

    extract_to = os.path.normpath("data")
    json_folder_path = os.path.abspath(os.path.join(extract_to, "all_data"))
    conn_id = "postgres_localhost"

    @task()
    def check_postgres_connection():
        """Checks if the Postgres connection is valid, otherwise raises an exception."""
        try:
            hook = PostgresHook(postgres_conn_id=conn_id)
            conn = hook.get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            logger.info("Postgres connection is successful!")
        except Exception as e:
            raise Exception(f"Failed to connect to Postgres: {str(e)}")

    @task()
    def create_table():
        """Creates the required table if it doesn't exist."""
        hook = PostgresHook(postgres_conn_id=conn_id)
        create_table_sql = """
        CREATE SCHEMA IF NOT EXISTS ods;
        DROP TABLE IF EXISTS ods.all_match_data;
        CREATE TABLE IF NOT EXISTS ods.all_match_data (
            id SERIAL PRIMARY KEY,
            filename VARCHAR(255),
            filedate TIMESTAMP, 
            json_data JSONB
        );
        """
        hook.run(create_table_sql)
        logger.info("Table ods.all_match_data is ready.")

    @task()
    def process_and_ingest():
        """Reads, processes, and inserts JSON files into PostgreSQL in batches."""
        try:
            files = os.listdir(json_folder_path)
            if not files:
                logger.warning("No files found for processing.")
                return

            hook = PostgresHook(postgres_conn_id=conn_id)
            batch_size = 5000
            batch = []

            for filename in files:
                # Skip non-JSON files
                if not filename.endswith(".json"):
                    logger.warning(f"Skipping non-JSON file: {filename}")
                    continue  
                
                file_path = os.path.join(json_folder_path, filename)

                # Get last modified date of the file
                filedate = from_timestamp(os.path.getmtime(file_path))

                with open(file_path, "r") as file:
                    data = json.load(file)  # `data` is a dictionary

                batch.append((filename, filedate, json.dumps(data)))  # Ensure JSON is a string

                # Insert when batch is full
                if len(batch) >= batch_size:
                    hook.insert_rows("ods.all_match_data", batch, target_fields=["filename", "filedate", "json_data"])
                    logger.info(f"Inserted {len(batch)} records into PostgreSQL.")
                    batch = []  # Clear batch

            # Insert remaining records
            if batch:
                hook.insert_rows("ods.all_match_data", batch, target_fields=["filename", "filedate", "json_data"])
                logger.info(f"Inserted final {len(batch)} records into PostgreSQL.")

        except (json.JSONDecodeError, KeyError, Exception) as e:
            logger.error(f"Error processing file {filename}, {e}")


    # Task dependencies
    check_postgres_connection() >> create_table() >> process_and_ingest()

# Instantiate the DAG
load_all_time_data()
