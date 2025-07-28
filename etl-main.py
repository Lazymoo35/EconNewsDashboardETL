import os
import argparse
import logging
from datetime import datetime

from etl.extract import get_econNews_last24hours
from etl.transform import enrich_econNews_data, modify_econNews_data
from etl.load import upload_to_bigquery

from dotenv import load_dotenv
load_dotenv()

from prefect_gcp import GcpCredentials
from prefect import flow


# === Logging Setup ===
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"etl_log_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

@flow(name="ETL Pipeline", log_prints=True) # Above pipeline in etl-main.py

def run_pipeline(mode:str="append") -> None:
    """
    Run the ETL pipeline with configurable mode.
    
    Args:
        mode (str, optional): "replace", "fail", or "append". Defaults to "append".
    """

    logging.info(f"Starting ETL pipeline - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # ==1. Extract ==
    logging.info("Extracting data...")
    raw = get_econNews_last24hours()
    if raw.empty:
        logging.warning("No data fetched. Exiting pipeline.")
        return
    
    # ==2. Transform ==
    logging.info("Transforming data...")
    logging.info(f"-- Data Enrichment --")
    enriched = enrich_econNews_data(raw)
    logging.info(f"-- Data Modification --")
    modified = modify_econNews_data(enriched)

    # ==3. Load ==
    PROJECT_ID = os.getenv("PROJECT_ID")
    TABLE_ID = os.getenv("TABLE_ID")
    
    logging.info("Loading data to BigQuery...")
    upload_to_bigquery(modified, TABLE_ID, PROJECT_ID, mode)

    logging.info(f"ETL pipeline completed - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return

if __name__ == "__main__":
    run_pipeline()