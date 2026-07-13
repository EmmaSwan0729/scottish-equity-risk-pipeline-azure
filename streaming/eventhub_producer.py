"""
eventhub_producer.py

Azure Event Hubs producer: Simulate a real-time stock price feed for 8 Scottish
equities and publish to Azure Event Hubs (stock-prices).

This is the Azure equivalent of kafka_producer.py from the AWS/Kafka version of
this pipeline. The price simulation logic (GBM random walk) is unchanged —
only the transport layer (Kafka -> Event Hubs) and authentication (none ->
DefaultAzureCredential/RBAC) have been replaced.

Price simulation logic
    - Fetches the latest closing price from yfinance as the base price
    - Applies a small random walk (Geometric Brownian Motion) on each tick
    - This mimics realistic intraday price fluctuation

Partitioning strategy
    Mirrors the Kafka version's key=symbol behavior: each symbol is sent with
    its own partition_key, so all messages for a given symbol always land on
    the same Event Hubs partition (preserves per-symbol ordering guarantees).

Authentication
    Uses DefaultAzureCredential (RBAC), consistent with upload_to_adls.py.
    No connection strings or keys in code. Requires the "Azure Event Hubs
    Data Sender" role assigned at the namespace scope.

Usage:
    python eventhub_producer.py
"""

import json
import yaml
import time
import logging
from datetime import datetime, timezone

import numpy as np
import yfinance as yf
from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.exceptions import EventHubError
from azure.identity import DefaultAzureCredential

# Logging setup
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s "
)
logger = logging.getLogger(__name__)

# Config
FULLY_QUALIFIED_NAMESPACE = "ehns-scottish-equity-risk.servicebus.windows.net"
EVENT_HUB_NAME = "stock-prices"


def load_tickers(config_path: str = "config/tickers.yaml") -> list:
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return [item["symbol"] for item in config["tickers"]]


# 8 Scottish equities tracked in this pipeline
SYMBOLS = load_tickers()

# Price simulation parameters
TICK_INTERVAL_SEC = 2  # Seconds  between each batch of price updates
VOLATILITY = 0.005  # Per-tick volatity (0.5% std dev -realistic for intraday)

# STEP 1 Fetch base prices from yfinance (unchaged from AWS version)


def fetch_base_prices(symbols: list[str]) -> dict[str, float]:
    """
    Download the most recent closing price for each symbol using yfinance.
    These serve as the starting point for our simulated price walk.

    Returns:
    dict mapping symbol -> base price, e.g. {"NWG.L": 3.12, ...}
    """
    logger.info("Fetching base prices from yfinance...")
    base_prices = {}

    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(
                period="5d"
            )  # Last 5 days in case today has no data yet

            if hist.empty:
                logger.warning(
                    f"No data returned for {symbol}, using fallback proce 100.0"
                )
                base_prices[symbol] = 100.0
            else:
                last_close = float(hist["Close"].iloc[-1])
                base_prices[symbol] = last_close
                logger.info(f" {symbol}: base price = {last_close: .4f} GBX")

        except Exception as e:
            logger.error(f"Failed to fetch price for  {symbol}: {e}")
            base_prices[symbol] = 100.0  # Fall back to avoid crashing

    logger.info(f"Base prices loaded for {len(base_prices)} symbols")
    return base_prices


# STEP 2 Simulate price movement (unchanged from AWS version)


def simulate_next_price(current_price: float, volatility: float = VOLATILITY) -> float:
    """
    Generate the next simulated price using Geometric Brownian Motion (GBM).
    GBM is the standard model for stock price simulation:
        P(t+1) = P(t) * exp(random_shock)
    where random_shock ~ Normal(0, volatility)

    Args:
        current_price: the price from the previous tick
        volatility: per-tick std deviation of log returns

    Returns:
        float: the new simulated price
    """
    shock = np.random.normal(loc=0, scale=volatility)
    return round(current_price * np.exp(shock), 4)


def simulate_volume() -> int:
    """
    Generate a random trade volume for the tick.
    Based on a log-normal distribution - volumes are always positive and
    occasionally spike (mimicking real market bursts).

    Returns:
        int: simulated volume in number of shares
    """
    return int(np.random.lognormal(mean=9.5, sigma=1.2))  # typical range: -1k - 200k


# STEP 3: Build the message payload (unchanged from AWS version)


def build_message(symbol: str, price: float, prev_price: float, volume: int) -> dict:
    """
     Construct the JSON message payload for one price tick.

    Schema:
        symbol      (str)   : ticker symbol, e.g. "NWG.L"
        price       (float) : current simulated price in GBX (pence)
        prev_price  (float) : price from the previous tick
        change_pct  (float) : percentage change vs prev tick
        volume      (int)   : simulated trade volume
        timestamp   (str)   : ISO 8601 UTC timestamp
        source      (str)   : always "simulated" to distinguish from real data
    """
    change_pct = (
        round((price - prev_price) / prev_price * 100, 4) if prev_price else 0.0
    )
    return {
        "symbol": symbol,
        "price": price,
        "prev_price": prev_price,
        "change_pct": change_pct,
        "volume": volume,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "simulated",
    }


# STEP 4: Create Event Hubs Producer Client
# (Replaces create_producer() / KafkaProducer from the AWS version)


def create_producer() -> EventHubProducerClient:
    """
    Initialise and return an EventHubProducerClient instance.

    Authenticates via DefaultAzureCredential (RBAC) instead of a connection
    string / SAS key — consistent with the auth pattern used in
    upload_to_adls.py. Requires the caller's identity to hold the
    "Azure Event Hubs Data Sender" role at the namespace scope.
    """
    logger.info(f"Connecting to Event Hubs namespace {FULLY_QUALIFIED_NAMESPACE}...")
    credential = DefaultAzureCredential()
    producer = EventHubProducerClient(
        fully_qualified_namespace=FULLY_QUALIFIED_NAMESPACE,
        eventhub_name=EVENT_HUB_NAME,
        credential=credential,
    )
    logger.info(f"Event Hubs Producer connected successfully.")
    return producer


# STEP 5 Main producer loop


def run_producer():
    """
     Main loop: continuously simulate and publish stock prices.

    Flow per tick:
        1. For each symbol, simulate the next price
        2. Build the JSON message
        3. Wrap it in a partition_key-bound batch (partition_key = symbol,
           mirrors the Kafka version's key=symbol partitioning) and send
        4. Update the current price state
        5. Sleep for TICK_INTERVAL_SEC seconds
        6. Repeat indefinitely (Ctrl+C to stop)

    Note on send semantics: unlike the Kafka version's async send() with
    callbacks, EventHubProducerClient.send_batch() is synchronous — it
    blocks until the batch is acknowledged or raises an EventHubError.
    """
    # Load base prices once at startup
    current_prices = fetch_base_prices(SYMBOLS)

    # Create Event Hubs producer()
    producer = create_producer()

    tick = 0
    logger.info(
        f"Starting price feed - publishing to Event Hub '{EVENT_HUB_NAME}' every {TICK_INTERVAL_SEC}s"
    )
    logger.info("Press Ctrl + C to stop. \n")

    try:
        while True:
            tick += 1
            logger.info(f"-- Tick {tick} -----------")

            for symbol in SYMBOLS:
                prev_price = current_prices[symbol]
                new_price = simulate_next_price(prev_price)
                volume = simulate_volume()

                message = build_message(
                    symbol=symbol, price=new_price, prev_price=prev_price, volume=volume
                )

                # Each symbol gets its own batch, bound to a partition_key.
                # This ensures all messages for a given symbol always land on
                # the same partition — the Event Hubs equivalent of Kafka's
                # key=symbol partitioning. A single batch can only carry one
                # partition_key, so we can't combine symbols into one batch
                # here without losing that guarantee.
                event_batch = producer.create_batch(partition_key=symbol)
                event_batch.add(EventData(json.dumps(message)))
                producer.send_batch(event_batch)

                # Log the price update
                direction = (
                    "↑"
                    if new_price > prev_price
                    else "↓"
                    if new_price < prev_price
                    else "-"
                )
                logger.info(
                    f" {symbol:<8} {direction} {new_price:>10.4f} GBX"
                    f"({message['change_pct']:+.4f}%) vol={volume:,}"
                )

                # Update state for next tick
                current_prices[symbol] = new_price

            logger.info(
                f"All {len(SYMBOLS)} messages sent. Sleeping for {TICK_INTERVAL_SEC}s...\n"
            )
            time.sleep(TICK_INTERVAL_SEC)

    except KeyboardInterrupt:
        logger.info("\nProducer stopped by user (KeyboardInterrupt)")

    except EventHubError as e:
        logger.error(f"Event Hubs error: {e}")

    finally:
        producer.close()
        logger.info("Event Hubs Producer closed")


# Entry point
if __name__ == "__main__":
    run_producer()
