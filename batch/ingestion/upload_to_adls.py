import pandas as pd
from io import BytesIO
from datetime import datetime
import logging
import os
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "raw")

def get_service_client(account_name: str) -> DataLakeServiceClient:
    """
    Build a DataLakeServiceClient authenticated via DefaultAzureCredential. 
    DefaultAzureCredential resolves identity through a fixed provider chain
    (environment variables, managed identity, Azure CLI login, etc.), so the
    same code path authenticates locally via `az login` and, without changes,
    via managed identity when deployed to Azure. No credentials are stored
    in code or configuration files.
    """
    account_url = f"https://{account_name}.dfs.core.windows.net"
    credential  = DefaultAzureCredential()
    return DataLakeServiceClient(account_url=account_url, credential=credential)

def upload_df_to_adls(
        df: pd.DataFrame,
        account_name: str,
        container:str,
        date: str
) -> str:
    # Container is already named "raw", so the "raw/" prefix used in the
    # original S3 key is dropped here to avoid redundant nesting.
    file_path = f"stock_prices/dt={date}/stock_prices.parquet"

    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    data = parquet_buffer.getvalue()
    
    service_client = get_service_client(account_name)
    file_system_client = service_client.get_file_system_client(file_system=container)
    file_client = file_system_client.get_file_client(file_path)

    # ADLS Gen2 writes are a three-step file-system operation
    # (create -> append -> flush), unlike S3's single put_object call,
    # reflecting its file-system semantics vs. S3's object-store semantics.
    file_client.create_file()
    file_client.append_data(data=data, offset=0, length=len(data))
    file_client.flush_data(len(data))

    adls_path = f"abfss://{container}@{account_name}.dfs.core.windows.net/{file_path}"
    logger.info(f"Upload {len(df)} rows to {adls_path}")
    return adls_path

if __name__ == "__main__":
    from fetch_stock_data import fetch_stock_data, SCOTTISH_TICKERS
    from datetime import timedelta

    end = datetime.today().strftime("%Y-%m-%d")
    start = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")

    df = fetch_stock_data(
        tickers = SCOTTISH_TICKERS,
        start_date = start,
        end_date = end,
    )

    adls_path = upload_df_to_adls(
        df=df,
        account_name=STORAGE_ACCOUNT_NAME,
        container=CONTAINER_NAME,
        date=end,
    )
    print(f"\nUpload to {adls_path}")