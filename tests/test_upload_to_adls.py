from unittest.mock import patch, MagicMock
import pandas as pd
from upload_to_adls import upload_df_to_adls

def test_upload_df_to_adls_builds_correct_path():
    test_df = pd.DataFrame({"symbol": ["SHEL.L"], "close": [100.0]})

    with patch("upload_to_adls.get_service_client") as mock_get_client:
        result = upload_df_to_adls(
            df = test_df,
            account_name="teststorage",
            container="raw",
            date="2026-07-15"
        )

    assert "stock_prices/dt=2026-07-15/stock_prices.parquet" in result

def test_upload_df_to_adls_calls_get_file_client_with_correct_path():
    test_df = pd.DataFrame({"symbol": ["SHEL.L"], "close": [100.0]})

    with patch("upload_to_adls.get_service_client") as mock_get_client:
        upload_df_to_adls(
            df = test_df,
            account_name = "teststorage",
            container = "raw",
            date = "2026-07-15"
        )
    mock_get_client.return_value.get_file_system_client.return_value.get_file_client.assert_called_with("stock_prices/dt=2026-07-15/stock_prices.parquet")