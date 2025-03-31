import os
import shutil
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
def download_all_time_data():

    data_url = "https://cricsheet.org/downloads/all_json.zip"
    extract_to = os.path.normpath("data")
    zip_save_path = os.path.join(extract_to, "all_json.zip")
    json_folder_path = os.path.abspath(os.path.join(extract_to, "all_data"))


    @task()
    def check_all_data():
        logger.info(f"Checking if ZIP file already exists at {zip_save_path}...")
        existing_file_size = 0
        if os.path.exists(zip_save_path):
                # Get the size of the existing file
                existing_file_size = os.path.getsize(zip_save_path)
                logger.info(f"Found existing ZIP file with size: {existing_file_size} bytes.")
                
                # Get the size of the remote file
                response = requests.head(data_url)
                response.raise_for_status()
                remote_file_size = int(response.headers.get('Content-Length', 0))
                logger.info(f"Remote ZIP file size: {remote_file_size} bytes.")
                
                # If sizes match, check if extraction is complete
                if existing_file_size == remote_file_size:
                    logger.info("Existing ZIP file is up-to-date. Checking for extracted files...")
                    
                    # Check if JSON files are already extracted
                    if os.path.exists(json_folder_path) and any(filename.endswith('.json') for filename in os.listdir(json_folder_path)):
                        logger.info("JSON files are already extracted.")
                        return True
                    else:
                        logger.info("JSON files are not extracted or missing. Proceeding to download and extract.")
                else:
                    logger.info("ZIP file size mismatch. Proceeding to download and extract.")
        else:
            logger.info(f"ZIP file does not exist. Proceeding to download and extract.")
        
        shutil.rmtree(extract_to)
        os.makedirs(extract_to)
        return False


        
    @task()  
    def download_all_data(file_exists):
        """
        Downloads and extracts JSON data from the provided URL.
        """
        
        try:
            # Check if the ZIP file already exists
            if (file_exists):
                logger.info(f"Correct files exist skipping download!")
                return

            # Proceed to download if file does not exist or sizes do not match
            logger.info("Downloading ZIP file...")
            response = requests.get(data_url, stream=True)                
            response.raise_for_status()
            
            with open(zip_save_path, "wb") as f:
                f.write(response.content)
                
            # Check if the file exists before proceeding
            if not os.path.exists(zip_save_path):
                logger.error(f"ZIP file was not saved at {zip_save_path}.")
                return  
            
            logger.info(f"ZIP file saved at {zip_save_path}.")
            return
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download {data_url}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")

    
            
    @task()  
    def extract_all_data(file_exists):
        """
        Downloads and extracts JSON data from the provided URL.
        """
        
        try:
            # Check if the ZIP file already exists
            if (file_exists):
                logger.info(f"Correct files exist skipping extract!")
                return
            
            # Extract ZIP contents
            os.makedirs(json_folder_path, exist_ok=True)
            
            with zipfile.ZipFile(zip_save_path, "r") as zip_ref:
                zip_ref.extractall(json_folder_path)

            logger.info(f"Extracted {data_url} to {json_folder_path}")
            return 

        except zipfile.BadZipFile:
            logger.error(f"Error: {data_url} is not a valid ZIP archive.")
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
    

    # Task dependencies
    file_exists = check_all_data()
    download_all_data(file_exists) >> extract_all_data(file_exists)
    

# Instantiate the DAG
download_all_time_data()