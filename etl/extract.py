import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import time
import logging
import os
from dotenv import load_dotenv

load_dotenv()

def parse_econNewsData(data: dict) -> pd.DataFrame:
    """
    Parse a JSON response from marketaux API into a pandas DataFrame.

    Parameters:
        data (dict): JSON object returned by marketaux API.

    Returns:
        pd.DataFrame: Flattened economic news data with relevant fields.
    """
    article_collection = []

    for article in data:
        symbol = "unknown"
        name = "unknown"
        type = "undetermined"
        industry = "undetermined"

        if article.get("entities"):
            entity = article["entities"][0]
            symbol = entity.get("symbol", "unknown")
            name = entity.get("name", "unknown")
            type = entity.get("type", "undetermined")
            industry = entity.get("industry", "undetermined")

        article_collection.append({
            "title": article.get("title", ""),
            "keywords": article.get("keywords", ""),
            "url": article.get("url", ""),
            "image_url": article.get("image_url", ""),
            "published_at": article.get("published_at", ""),
            "source": article.get("source", ""),
            "language": article.get("language", ""),
            "symbol": symbol,
            "name": name,
            "type": type,
            "industry": industry
        })
    return pd.DataFrame(article_collection)

def get_econNews_last24hours() -> pd.DataFrame:
    """
    Fetch economic news from the past 24 hours.

    Returns:
        pd.DataFrame: Economic news with en and id language from the past 24 hours.
    """
    BASE_URL = "https://api.marketaux.com/v1/news/all"
    API_TOKEN = os.getenv("API_TOKEN")

    published_after_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    params = {
    "language": "en,id",
    "must_have_entities": "true",
    "published_after": published_after_date,
    "api_token": API_TOKEN
    }

    all_articles = []

    try:
        for page in range(1, 101):
            params["page"] = page
            response = requests.get(BASE_URL, params=params)
    
            print(f"Page {page} | Status code: {response.status_code}")
            logging.info(f"Fetched page {page} successfully.")

            if response.status_code != 200:
                print(f"Failed on page {page}")
                continue
            result = response.json()
            page_articles = result.get("data", [])
            all_articles.extend(page_articles)
            time.sleep(1)
        return parse_econNewsData(all_articles)
    except Exception as e:
        logging.error(f"Error fetching econNews data: {e}")
        return pd.DataFrame()