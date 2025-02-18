import os
import zipfile
import logging
import requests
from io import BytesIO
from airflow.decorators import dag, task
from pendulum import datetime

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
def get_all_cricket_data_ONE_TIME():
        
    @task()  
    def download_json_data_all_time():
        """
        Downloads and extracts JSON data from the provided URL.
        """
        data_url = "https://cricsheet.org/downloads/all_json.zip"
        extract_to = os.path.normpath("json_files")
        zip_save_path = os.path.join(extract_to, "all_json.zip")
        os.makedirs(extract_to, exist_ok=True)

        try:
                     
            response = requests.get(data_url, stream=True)                
            response.raise_for_status()
            
            with open(zip_save_path, "wb") as f:
                f.write(response.content)
                    
            # Check if the file exists before proceeding
            if not os.path.exists(zip_save_path):
                logger.error(f"ZIP file was not saved at {zip_save_path}.")
                return  
            
            # Extract ZIP contents
            extract_path = os.path.join(extract_to, "all_data")
            os.makedirs(extract_path, exist_ok=True)
            
            with zipfile.ZipFile(zip_save_path, "r") as zip_ref:
                zip_ref.extractall(extract_path)

            logger.info(f"Extracted {data_url} to {extract_path}")
                          
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download {data_url}: {e}")
        except zipfile.BadZipFile:
            logger.error(f"Error: {data_url} is not a valid ZIP archive.")
                    
    download_json_data_all_time()

# Instantiate the DAG
get_all_cricket_data_ONE_TIME()
