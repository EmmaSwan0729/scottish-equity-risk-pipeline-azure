from unittest.mock import patch
import pandas as pd
from fetch_stock_data import load_tickers, fetch_stock_data

def test_load_tickers_returns_symbol_list(tmp_path):
    # Arrange: write a temporary yaml file with known content
    config_content = """
    tickers:
        - symbol: SHEL.L
          name: Shell
        - symbol: BP.L
          name: BP        
    """
    config_file = tmp_path / "tickers.yaml"
    config_file.write_text(config_content)

    # Act: call the function under test with the temp file's path
    result = load_tickers(config_path=str(config_file))

    # Assert: only the symbols should be extracted, in order
    assert result == ["SHEL.L", "BP.L"]

def test_fetch_stock_data_adds_symbol_and_lowercases_columns():
    test_df = pd.DataFrame({
        "Open": [100.0],
        "Close": [105.0],
    })
    
    with patch("fetch_stock_data.yf.download", return_value=test_df):
        result = fetch_stock_data(
            tickers = ["SHEL.L"],
            start_date = "2024-01-01",
            end_date = "2024-01-02",
        )
    assert "symbol" in result.columns
    assert result["symbol"].iloc[0] == "SHEL.L"

    assert "open" in result.columns
    assert "close" in result.columns