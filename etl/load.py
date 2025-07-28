"""
load.py

Module to load cleaned econnews data into Google BigQuery using service account authentication.

Author: Reyner T. Purwanto
Created: 2025-28-07

Dependencies:
- pandas
- pandas-gbq
- google-auth
"""

import os
import pandas as pd
from pandas_gbq import to_gbq
from google.oauth2 import service_account
import logging
from prefect_gcp import GcpCredentials

logging.basicConfig(level=logging.INFO)

def upload_to_bigquery(
    df: pd.DataFrame,
    table_id: str,
    project_id: str,
    if_exists: str = "append"
):
    """
    Upload a pandas DataFrame to Google BigQuery using service account credentials.

    Parameters:
        df (pd.DataFrame): Modified and enriched data to upload.
        table_id (str): Full table path in BigQuery (e.g., dataset.table_name).
        project_id (str): Google Cloud project ID.
        credentials_path (str): Path to the service account JSON key file.
        if_exists (str): What to do if table exists. Options: 'fail', 'replace', 'append'.
                         Default is 'append'.

    Returns:
        None
    """
    try:
        credentials = GcpCredentials.load("gcp-credentials-econnews").get_credentials_from_service_account()
        logging.info(f"Authenticating using service account")

        logging.info(f"Uploading to BigQuery table: {table_id} (mode: {if_exists})")
        to_gbq(
            dataframe=df,
            destination_table=table_id,
            project_id=project_id,
            if_exists=if_exists,
            credentials=credentials
        )
        logging.info("Upload completed successfully.")
    except Exception as e:
        logging.error(f"Failed to upload to BigQuery: {e}")