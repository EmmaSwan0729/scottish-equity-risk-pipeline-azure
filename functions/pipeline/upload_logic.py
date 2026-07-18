from io import BytesIO
import logging

import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_service_client(account_name: str) -> DataLakeServiceClient:
    account_url = f"https://{account_name}.dfs.core.windows.net"
    credential = DefaultAzureCredential()
    return DataLakeServiceClient(account_url=account_url, credential=credential)


def upload_df_to_adls(
    df: pd.DataFrame, account_name: str, container: str, date: str
) -> str:
    file_path = f"stock_prices/dt={date}/stock_prices.parquet"

    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    data = parquet_buffer.getvalue()

    service_client = get_service_client(account_name)
    file_system_client = service_client.get_file_system_client(file_system=container)
    file_client = file_system_client.get_file_client(file_path)

    file_client.create_file()
    file_client.append_data(data=data, offset=0, length=len(data))
    file_client.flush_data(len(data))

    adls_path = f"abfss://{container}@{account_name}.dfs.core.windows.net/{file_path}"
    logger.info(f"Uploaded {len(df)} rows to {adls_path}")
    return adls_path
