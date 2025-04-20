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
def load_landing_zone_all_time():

    extract_to = os.path.normpath("data")
    json_folder_path = os.path.abspath(os.path.join(extract_to, "all_data"))
    conn_id = "postgres_localhost"

    @task()
    def check_postgres_connection():
        """Checks if the Postgres connection is valid, otherwise raises an exception."""
        try:
            hook = PostgresHook(postgres_conn_id=conn_id, database = 'admin')
            conn = hook.get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM ADMIN.AUDIT.MATCH_LINEAGE LIMIT 1")
            cursor.close()
            conn.close()
            logger.info("Postgres connection to Postgres Table ADMIN.AUDIT.MATCH_LINEAGE is successful!")
        except Exception as e:
            raise Exception(f"Failed to connect to Postgres Table ADMIN.AUDIT.MATCH_LINEAGE: {str(e)}")

    @task()
    def get_metadata():
        """Creates the required table if it doesn't exist."""
        hook = PostgresHook(postgres_conn_id=conn_id, database = 'admin')
        get_schemas_sql = """SELECT filename, filedate, match_type
        FROM admin.audit.match_lineage;"""

        metadata_records = hook.get_records(get_schemas_sql)
        
        if not metadata_records:
            logger.error("No records found.")
            return
        
        return metadata_records

    @task()
    def create_database_artifacts_if_not_exists(metadata_records):
        """Creates the required table if it doesn't exist."""
        hook = PostgresHook(postgres_conn_id=conn_id, database = 'landing_zone')

        schemas = [records[2] for records in metadata_records]
        schemas = list(set(schemas))
        
        for schema in schemas:
            create_database_artifacts_sql = f"""
            CREATE SCHEMA IF NOT EXISTS {schema};
            CREATE TABLE IF NOT EXISTS {schema}.all_match_data (
                id SERIAL PRIMARY KEY,
                filename VARCHAR(255),
                filedate TIMESTAMP, 
                json_data JSONB
            );
            """

            hook.run(create_database_artifacts_sql)

            logger.info(f"Table {schema}.all_match_data is ready.")

    @task()
    def ingest_into_landing_zone(metadata_records):
        """Reads, processes, and inserts JSON files into PostgreSQL in batches."""
        try:
            files = os.listdir(json_folder_path)
            if not files:
                logger.warning("No files found for processing.")
                return
            
            sorted_arr = sorted(metadata_records, key=lambda x: (x[2], x[1]))

            hook = PostgresHook(postgres_conn_id=conn_id, database = 'landing_zone')
            batch = {}

            for records in metadata_records:
                filename = records[0]
                filedate = records[1]
                schema = records[2]

                filename = f"{filename}.json"

                if schema not in batch:
                    batch[schema] = []

                file_path = os.path.join(json_folder_path, filename)

                with open(file_path, "r") as file:
                    json_data = json.load(file)  # `data` is a dictionary

                batch[schema].append((filename, filedate, json.dumps(json_data)))  # Ensure JSON is a string

            for schema, records in batch.items():
                if not records:
                    logger.warning(f"No records found for schema {schema}.")
                    continue
                # Insert records into PostgreSQL in batches
                hook.insert_rows(f"{schema}.all_match_data", records, target_fields=["filename", "filedate", "json_data"])

                logger.info(f"Inserted {len(batch)} records into {schema}.")
                    

        except (json.JSONDecodeError, KeyError, Exception) as e:
            logger.error(f"Error processing file {filename}, {e}")


    # Task dependencies
    # Task instances
    check_conn = check_postgres_connection()
    metadata = get_metadata()
    create_artifacts = create_database_artifacts_if_not_exists(metadata)
    ingest = ingest_into_landing_zone(metadata)

    # Set dependencies
    check_conn >> metadata >> create_artifacts >> ingest


# Instantiate the DAG
load_landing_zone_all_time()
