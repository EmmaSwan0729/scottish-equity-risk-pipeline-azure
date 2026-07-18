import requests
# import json

FETCH_URL = "http://localhost:7071/api/fetch_stock_data"
UPLOAD_URL = "http://localhost:7071/api/upload_to_adls"

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
