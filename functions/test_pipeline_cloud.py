import os
import requests

BASE_URL = "https://func-scottish-equity-risk-bzfzejb0a9hhanbu.uksouth-01.azurewebsites.net"
FUNCTION_KEY = os.getenv("TEST_FUNCTION_KEY", "")

FETCH_URL = f"{BASE_URL}/api/fetch_stock_data?code={FUNCTION_KEY}"
UPLOAD_URL = f"{BASE_URL}/api/upload_to_adls?code={FUNCTION_KEY}"

fetch_payload = {"start_date": "2026-06-01", "end_date": "2026-07-17"}
fetch_resp = requests.post(FETCH_URL, json=fetch_payload)
fetch_resp.raise_for_status()
fetch_result = fetch_resp.json()

print(f"Fetched {fetch_result['row_count']} rows")

upload_payload = {
    "data": fetch_result["data"],
    "date": "2026-07-17",
}
upload_resp = requests.post(UPLOAD_URL, json=upload_payload)
print(upload_resp.status_code)
print(upload_resp.text)