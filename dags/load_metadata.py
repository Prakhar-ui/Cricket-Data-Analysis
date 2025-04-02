import os
import re
import json
import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define DAG parameters
@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "Astro", "retries": 3},
    tags=["cricket-data-extraction"],
)
def load_metadata():

    extract_to = os.path.normpath("data")
    json_folder_path = os.path.abspath(os.path.join(extract_to, "all_json"))
    conn_id = "postgres_localhost"

    @task()
    def check_postgres_connection():
        """Checks if the Postgres connection is valid, otherwise raises an exception."""
        try:
            hook = PostgresHook(postgres_conn_id=conn_id, database = "admin")
            conn = hook.get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            logger.info("Postgres connection is successful!")
        except Exception as e:
            raise Exception(f"Failed to connect to Postgres: {str(e)}")

    @task()
    def create_database_artifacts_if_not_exists():
        """Creates the required table if it doesn't exist."""
        hook = PostgresHook(postgres_conn_id=conn_id, database = "admin")
        create_table_sql = """
        CREATE SCHEMA IF NOT EXISTS AUDIT;
        DROP TABLE IF EXISTS AUDIT.MATCH_LINEAGE;
        CREATE TABLE IF NOT EXISTS AUDIT.MATCH_LINEAGE (
            id SERIAL PRIMARY KEY,
            filename VARCHAR,
            filedate DATE, 
            season_year VARCHAR,
            season_month VARCHAR,
            gender VARCHAR,
            team_type VARCHAR,
            match_type VARCHAR,
            team1 VARCHAR,
            team2 VARCHAR
        );
        """
        hook.run(create_table_sql)
        logger.info("Table ADMIN.AUDIT.MATCH_LINEAGE is ready.")

    @task()
    def extract_from_readme():
        """Reads, processes, and extracts match data from README.txt"""
        file_path = os.path.join(json_folder_path, "README.txt")
        
        try:
            with open(file_path, "r") as file:
                lines = file.readlines()

            # Regex pattern to match date
            pattern = r"\b\d{4}-\d{2}-\d{2}\b"
            
            # Extract data efficiently using list comprehension
            matching_lines = [line for line in lines if re.search(pattern, line)]
            
            extracted_list = []
            for line in matching_lines:
                extracted_data = line.split(" - ")
                year_and_month = extracted_data[0].split("-")[:-1]  # remove day
                teams = extracted_data[-1].strip().replace('\n','').split(" vs ")
                extracted_data.remove(extracted_data[-1]) #removing combined teams before adding individual team
                extracted_data.extend(year_and_month)
                extracted_data.extend(teams)
                extracted_list.append(extracted_data)


            logger.info(f"Extracted {len(extracted_list)} match records.")
            return extracted_list  

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            return []

    @task()
    def load_into_postgres(batch):
        """Loads extracted match data into PostgreSQL"""
        if not batch:
            logger.warning("No data to insert.")
            return

        try:
            hook = PostgresHook(postgres_conn_id=conn_id, database = "admin")
            hook.insert_rows(
                "AUDIT.MATCH_LINEAGE",
                batch,
                target_fields=[
                    "filedate",
                    "team_type",
                    "match_type",
                    "gender",
                    "filename",
                    "season_year",
                    "season_month",
                    "team1",
                    "team2",
                ],
            )
            logger.info(f"Inserted {len(batch)} records into PostgreSQL.")

        except Exception as e:
            logger.error(f"Error inserting data into PostgreSQL: {e}")

    # Task dependencies
    check_postgres_connection() >> create_database_artifacts_if_not_exists() >> load_into_postgres(extract_from_readme())

# Instantiate the DAG
load_metadata()
