import json
import os
import logging

import azure.functions as func
import pandas as pd

from pipeline.fetch_logic import load_tickers, fetch_stock_data
from pipeline.upload_logic import upload_df_to_adls

app = func.FunctionApp()


@app.route(route="fetch_stock_data", auth_level=func.AuthLevel.FUNCTION)
def fetch_stock_data_handler(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            "Request body must be valid JSON with start_date and end_date.",
            status_code=400,
        )

    start_date = req_body.get("start_date")
    end_date = req_body.get("end_date")

    if not start_date or not end_date:
        return func.HttpResponse(
            "Missing required fields: start_date, end_date.",
            status_code=400,
        )

    try:
        tickers = load_tickers()
        df = fetch_stock_data(tickers=tickers, start_date=start_date, end_date=end_date)
    except Exception as e:
        logging.error(f"fetch_stock_data failed: {e}")
        return func.HttpResponse(f"fetch_stock_data failed: {e}", status_code=500)

    records = json.loads(df.to_json(orient="records", date_format="iso"))
    response_body = json.dumps({"data": records, "row_count": len(df)})

    return func.HttpResponse(
        response_body, status_code=200, mimetype="application/json"
    )


@app.route(route="upload_to_adls", auth_level=func.AuthLevel.FUNCTION)
def upload_to_adls_handler(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            "Request body must be valid JSON with data and date.",
            status_code=400,
        )

    data = req_body.get("data")
    date = req_body.get("date")

    if isinstance(data, str):
        data = json.loads(data)

    if data is None or not date:
        return func.HttpResponse(
            "Missing required fields: data, date.",
            status_code=400,
        )

    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    container = os.getenv("AZURE_CONTAINER_NAME", "raw")

    if not account_name:
        return func.HttpResponse(
            "AZURE_STORAGE_ACCOUNT_NAME app setting is not configured.",
            status_code=500,
        )

    try:
        df = pd.DataFrame(data)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        adls_path = upload_df_to_adls(
            df=df, account_name=account_name, container=container, date=date
        )
    except Exception as e:
        logging.error(f"upload_to_adls_failed: {e}")
        return func.HttpResponse(f"upload_to_adls failed: {e}", status_code=500)

    response_body = json.dumps({"adls_path": adls_path, "row_count": len(df)})
    return func.HttpResponse(
        response_body, status_code=200, mimetype="application/json"
    )
