import os
from datetime import datetime, timezone
import logging

import yfinance as yf
import pandas as pd
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "tickers.yaml")


def load_tickers(config_path: str = CONFIG_PATH) -> list:
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return [item["symbol"] for item in config["tickers"]]


def fetch_stock_data(tickers: list, start_date: str, end_date: str) -> pd.DataFrame:
    all_data = []

    for ticker in tickers:
        logger.info(f"fetching {ticker} from {start_date} to {end_date}")
        try:
            df = yf.download(
                ticker, start=start_date, end=end_date, auto_adjust=True, progress=False
            )

            if df.empty:
                logger.warning(f"No data returned for {ticker}, skipping.")
                continue

            df = df.reset_index()
            df.columns = [
                c[0].lower() if isinstance(c, tuple) else c.lower() for c in df.columns
            ]
            df["symbol"] = ticker
            df["ingested_at"] = datetime.now(timezone.utc).isoformat()

            all_data.append(df)
            logger.info(f"Fetched {len(df)} rows for {ticker}")

        except Exception as e:
            logger.error(f"Failed to fetch {ticker}: {e}")
            continue

    if not all_data:
        raise ValueError("No data fetched for any ticker. ")

    combined = pd.concat(all_data, ignore_index=True)
    logger.info(f"Total rows fetched: {len(combined)}")
    return combined
