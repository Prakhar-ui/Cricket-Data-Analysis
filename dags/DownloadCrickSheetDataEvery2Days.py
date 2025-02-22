import os
import zipfile
import logging
import requests
from io import BytesIO
import glob
from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.hooks.base import BaseHook

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define DAG parameters
@dag(
    start_date=datetime(2025, 1, 1),
    schedule="0 0 */2 * *",
    catchup=False,
    default_args={"owner": "Astro", "retries": 3},
    tags=["cricket-data-extraction"],
)
def get_recent_cricket_data_every_2_days():
    
    @task()
    def get_download_url(endpoint: str) -> str:
        """Retrieve the base download URL from Airflow connection."""
        cricksheet_conn = BaseHook.get_connection("CricSheetDownloadLink")
        if not cricksheet_conn or not cricksheet_conn.host:
            raise ValueError("Missing or invalid CricSheetDownloadLink connection.")
        
        return f"{cricksheet_conn.host}/{endpoint}"
    
    @task()
    def download_file_from_cricksheet_and_save_in_local(download_url: str, zip_filename: str, container_name: str) -> str:
        """Download ZIP file content from the given URL."""
        try:
            response = requests.get(download_url, timeout=30)  # Add timeout for reliability
            response.raise_for_status()
            logger.info(f"Downloaded file successfully from {download_url}")
            
            os.makedirs(container_name, exist_ok=True)
            zip_path = os.path.join(container_name, zip_filename)
            
            with open(zip_path, "wb") as f:
                f.write(response.content)
                    
            # Check if the file exists before proceeding
            if not os.path.exists(zip_path):
                logger.error(f"ZIP file was not saved at {zip_path}.")
                return  
            
            # Extract ZIP contents
            extract_folder = os.path.join(container_name, zip_filename.replace(".zip", ""))
            os.makedirs(extract_folder, exist_ok=True)
            
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(extract_folder)
        

            logger.info(f"Extracted {download_url} to {extract_folder}")
            return extract_folder
                       
        except Exception as e:
            logger.error(f"Error downloading file: {e}")
            raise
        
    @task()
    def delete_all_files_from_blob(container_name: str):
        """Deletes all JSON files from the 'latest_data/' in Azure Blob Storage."""
        try:
            hook = WasbHook(wasb_conn_id="blobStorage-cricketdataanalysis")
            blobs = hook.get_blobs_list_recursive(container_name)  

            
            # Filter blobs to only include files inside 'latest_data/' and exclude the folder name itself
            files = [blob for blob in blobs if blob.startswith("latest_data/") and (blob.endswith(".json") or blob.endswith(".txt")) ]

            logger.info(f"Found {len(files)} files to delete.")

            deleted_count = 0
            # Delete each JSON file
            for blob in files:
                hook.delete_file(container_name, blob)
                logger.info(f"Deleted file: {blob}")
                deleted_count += 1
                
            logger.info(f"Deleted {deleted_count} files!")

        except Exception as e:
            logger.error(f"Error while deleting files: {e}", exc_info=True)
            raise
    @task()    
    def upload_to_blob(folder_path: str, container_name: str):
        """Uploads extracted files to Azure Blob Storage."""
        if not folder_path or not os.path.exists(folder_path):
            raise ValueError(f"Invalid folder path: {folder_path}") 
            
        hook = WasbHook(wasb_conn_id="blobStorage-cricketdataanalysis")
        file_paths = glob.glob(os.path.join(folder_path, "*"))
        
        uploaded_count = 0
        blob_folder = "latest_data/"
        
        logger.info(f"Uploading {len(file_paths)} files to {container_name}")
        
        for file_path in file_paths:
            try:
                blob_name = blob_folder + os.path.basename(file_path)
                hook.load_file(file_path, container_name=container_name, blob_name=blob_name, overwrite=True)
                logger.info(f"Uploaded {blob_name} to container {container_name}")
                uploaded_count += 1
            except Exception as e:
                logger.error(f"Failed to upload {file_path} to {container_name}: {e}")

        logger.info(f"Uploaded {uploaded_count} files to {container_name}")
        
        
  
    ZIP_FILENAME = "recently_added_2_json.zip"
    CONTAINER_NAME = "cricksheetdata"

    # Define task dependencies properly
    download_url_task = get_download_url(ZIP_FILENAME)
    folder_path_task = download_file_from_cricksheet_and_save_in_local(download_url_task, ZIP_FILENAME, CONTAINER_NAME)
    delete_files_task = delete_all_files_from_blob(CONTAINER_NAME)
    uploaded_files_task = upload_to_blob(folder_path_task, CONTAINER_NAME)

    # Ensure the proper order of execution
    download_url_task >> folder_path_task >> delete_files_task >> uploaded_files_task

# Instantiate the DAG
get_recent_cricket_data_every_2_days()