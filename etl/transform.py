"""
transform.py

Module to clean and enrich econNews data fetched from marketaux API.

Author: Reyner T. Purwanto
Created: 2025-28-07
"""

import pandas as pd
from datetime import datetime, timedelta

def enrich_econNews_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich raw econNews data:
    - Add a revised column, and delete the old column

    Parameters:
        df (pd.DataFrame): Raw DataFrame from extract module

    Returns:
        pd.DataFrame: Cleaned DataFrame with new revised column
    """

    if df.empty:
        return df
    
    df["list_of_keywords"] = df["keywords"].str.split(", ")
    df = df.drop(columns=["keywords"])
    return df

def modify_econNews_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw econNews data:
    - Standardize data types

    Parameters:
        df (pd.DataFrame): Raw DataFrame from extract module

    Returns:
        pd.DataFrame: Cleaned DataFrame with selected columns
    """

    if df.empty:
        return df

    df["title"] = df["title"].astype(str)
    df["url"] = df["url"].astype(str)
    df["image_url"] = df["image_url"].astype(str)
    df["published_at"] = pd.to_datetime(df["published_at"], errors='coerce').dt.strftime("%Y-%m-%d")
    df["source"] = df["source"].astype(str)
    df["language"] = df["language"].astype(str)
    df["symbol"] = df["symbol"].astype(str)
    df["name"] = df["name"].astype(str)
    df["type"] = df["type"].astype("category")
    df["industry"] = df["industry"].astype("category")
    return df