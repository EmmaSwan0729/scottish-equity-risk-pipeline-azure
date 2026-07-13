# Databricks notebook source
"""
databricks_streaming_consumer.py
 
Azure Databricks Structured Streaming consumer for the Scottish Equity Risk
Pipeline. Azure equivalent of the AWS/Kafka version's spark_streaming.py.
 
Risk-detection logic (HIGH_VOLATILITY / PRICE_DROP / PRICE_ANOMALY) and the
10-second micro-batch trigger are unchanged from the AWS version. Transport
and sink layers are re-implemented for Azure:
 
    Kafka topic "stock_prices"   -> Event Hubs "stock-prices"
                                     (native azure-eventhubs-spark connector)
    Per-alert Snowflake INSERT   -> batched Delta append to ADLS Gen2
    Local checkpoint (/tmp/...)  -> ADLS Gen2 checkpoint (abfss://...)
 
Runs as a Databricks notebook, where `spark` and `dbutils` are provided by
the runtime. Organised into four cells (marked below) matching the
notebook's cell structure.
"""

# COMMAND ----------

# 1 — Config
from pyspark.sql.functions import (
    col, from_json, stddev, avg, max as spark_max, min as spark_min
)
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, TimestampType
import uuid
from datetime import datetime, timezone

# Event Hubs connection string, retrieved from a Databricks secret scope
# backed by Azure Key Vault. Not hardcoded or stored in source control.
EVENT_HUB_CONNECTION_STRING  = dbutils.secrets.get(scope="scottish-equity-risk", key="eventhub-conn-str")

# ADLS paths for the alerts Delta table and its streaming checkpoint.
# "curated" is a separate container from "raw": raw holds ingested source
# data, curated holds derived/computed output.
STORAGE_ACCOUNT = "stscotequityriskuk"
ALERTS_PATH = f"abfss://curated@{STORAGE_ACCOUNT}.dfs.core.windows.net/alerts/"
CHECKPOINT_PATH = f"abfss://curated@{STORAGE_ACCOUNT}.dfs.core.windows.net/_checkpoints/equity_risk_alerts/"

# --- Alert thresholds — unchanged from the AWS version ---
VOLATILITY_THRESHOLD = 0.02
DROP_THRESHOLD = 0.03
ZSCORE_THRESHOLD = 2.0

# Schema for the JSON payload inside each Event Hubs message body, matching
# the message shape produced by eventhub_producer.py.
message_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("timestamp", LongType(),True),
])


# COMMAND ----------

# 2 — Read stream from Event Hubs

from pyspark.sql import SparkSession
spark_session = SparkSession.getActiveSession() or spark 

# The connection string must be encrypted via EventHubsUtils before being
# passed to the connector; required by azure-eventhubs-spark, not a
# general Spark pattern.
ehConf = {
    "eventhubs.connectionString": spark_session._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        EVENT_HUB_CONNECTION_STRING
    ),
    # Single downstream consumer for now. A second independent consumer
    # (e.g. a separate dashboard) should be given its own consumer group
    # rather than sharing $Default, to avoid offset contention.
    "eventhubs.consumerGroup": "$Default",
}

raw_stream = (
    spark.readStream
    .format("eventhubs")
    .options(**ehConf)
    .load()
)

# Event Hubs delivers the payload as raw bytes in a "body" column
# (Kafka's equivalent is "value").
parsed = (
    raw_stream
    .select(from_json(col("body").cast("string"), message_schema).alias("data"))
    .select("data.symbol", "data.price", "data.timestamp")
    .filter(col("symbol").isNotNull())
)


# COMMAND ----------

# 3 — process_batch: risk detection + batched Delta write

def process_batch(batch_df, batch_id):
    """Compute risk metrics for a micro-batch, collect any triggered
    alerts, and append them to the Delta alerts table in a single write."""
    if batch_df.isEmpty():
        return
    
    metrics = (
        batch_df.groupBy("symbol")
        .agg(
            stddev("price").alias("volatility"),
            avg("price").alias("avg_price"),
            spark_max("price").alias("max_price"),
            spark_min("price").alias("min_price"),
        )
    )

    rows = metrics.collect()

    # Alerts are collected here and written once per batch, rather than
    # opening a connection per alert (as the AWS version's
    # write_to_snowflake() did).
    alerts = []

    for row in rows:
        symbol = row["symbol"]
        volatility = row["volatility"] or 0.0
        avg_price = row["avg_price"] or 0.0
        max_price = row["max_price"] or 0.0

        latest = (
            batch_df.filter(col("symbol") == symbol)
            .orderBy(col("timestamp").desc())
            .first()
        )
        current_price = latest["price"] if latest else avg_price

         # Volatility alert — rolling volatility exceeds threshold.
        volatility_pct = volatility / avg_price if avg_price > 0 else 0
        if volatility_pct > VOLATILITY_THRESHOLD:
            alerts.append((
                str(uuid.uuid4()), symbol, "HIGH_VOLATILITY",
                float(volatility_pct), VOLATILITY_THRESHOLD, float(current_price), 
                datetime.now(timezone.utc)
            ))
        # Price drop alert — price falls more than threshold from window high.
        drop_pct = (max_price - current_price) / max_price if max_price > 0 else 0
        if drop_pct > DROP_THRESHOLD:
            alerts.append((
                str(uuid.uuid4()), symbol, "PRICE_DROP",
                float(drop_pct), DROP_THRESHOLD, float(current_price), 
                datetime.now(timezone.utc)
            ))
        # Price anomaly alert — price deviates more than threshold std devs from mean.
        if volatility > 0:
            zscore = abs(current_price - avg_price) / volatility
            if zscore > ZSCORE_THRESHOLD:
                alerts.append((
                str(uuid.uuid4()), symbol, "PRICE_ANOMALY",
                float(zscore), ZSCORE_THRESHOLD, float(current_price), 
                datetime.now(timezone.utc)
            ))
    # Single write for this whole micro-batch, regardless of how many
    # alerts (0 to 3 x 8 symbols) were triggered — this is the batching
    # behaviour we discussed.
    if alerts:
        alerts_schema = StructType([
            StructField("alert_id", StringType(), False),
            StructField("symbol", StringType(), False),
            StructField("alert_type", StringType(), False),
            StructField("metric_value", FloatType(), False),
            StructField("threshold_value", FloatType(), False),
            StructField("price_at_alert", FloatType(), False),
            StructField("triggered_at", TimestampType(), False), # cast below
        ])
        alerts_df = spark.createDataFrame(alerts, schema=alerts_schema)
        alerts_df.write.format("delta").mode("append").save(ALERTS_PATH)

        for a in alerts:
            print(f"[ALERT] {a[1]} | {a[2]} | value={a[3]:.4f} | threshold={a[4]}")
        

# COMMAND ----------

# 4 — Start the streaming query
query = (
    parsed.writeStream
    .foreachBatch(process_batch)
    .trigger(processingTime="10 seconds")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

# query.awaitTermination() is intentionally omitted: in a notebook it would
# block the cell indefinitely. The query runs in the background; stop it
# from a separate cell with query.stop() when finished.