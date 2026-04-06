"""CoinGecko cryptocurrency price ingester.

Tracks crypto tokens used in cartel financial flows.
Source: https://api.coingecko.com/api/v3/ (free, no key, 30 req/min)
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.crypto.coingecko")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
COINGECKO_API = "https://api.coingecko.com/api/v3"

# Tokens relevant to cartel/illicit finance tracking
TRACKED_TOKENS = {
    "bitcoin": "BTC",
    "monero": "XMR",       # Primary privacy coin
    "tether": "USDT",      # Stablecoin for laundering
    "usd-coin": "USDC",
    "zcash": "ZEC",         # Privacy coin
    "dash": "DASH",         # Used in Latin America
    "litecoin": "LTC",
    "ethereum": "ETH",
    "binancecoin": "BNB",
    "tron": "TRX",          # Popular for USDT transfers
}


def fetch_prices() -> list[dict]:
    """Fetch current prices for all tracked tokens."""
    ids = ",".join(TRACKED_TOKENS.keys())
    url = f"{COINGECKO_API}/simple/price"
    params = {
        "ids": ids,
        "vs_currencies": "usd",
        "include_24hr_vol": "true",
        "include_24hr_change": "true",
        "include_market_cap": "true",
    }
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    records = []
    now = datetime.now(timezone.utc).isoformat()
    for token_id, ticker in TRACKED_TOKENS.items():
        if token_id in data:
            info = data[token_id]
            records.append({
                "token_id": token_id,
                "ticker": ticker,
                "price_usd": info.get("usd", 0),
                "market_cap_usd": info.get("usd_market_cap", 0),
                "volume_24h_usd": info.get("usd_24h_vol", 0),
                "change_24h_pct": info.get("usd_24h_change", 0),
                "timestamp": now,
                "source": "coingecko",
            })
    return records


def fetch_historical(token_id: str, days: int = 90) -> list[dict]:
    """Fetch historical daily prices for a token."""
    url = f"{COINGECKO_API}/coins/{token_id}/market_chart"
    params = {"vs_currency": "usd", "days": str(days), "interval": "daily"}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    records = []
    for price_point in data.get("prices", []):
        ts_ms, price = price_point
        records.append({
            "token_id": token_id,
            "ticker": TRACKED_TOKENS.get(token_id, token_id.upper()),
            "price_usd": price,
            "timestamp": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat(),
            "source": "coingecko_historical",
        })
    return records


def detect_anomaly(prices: list[dict], z_threshold: float = 2.5) -> list[dict]:
    """Detect price anomalies in a token's historical series."""
    if len(prices) < 30:
        return []

    import statistics
    values = [p["price_usd"] for p in prices]
    returns = [(values[i] - values[i - 1]) / values[i - 1] for i in range(1, len(values)) if values[i - 1] > 0]
    if len(returns) < 20:
        return []

    mean_ret = statistics.mean(returns)
    std_ret = statistics.stdev(returns)
    if std_ret == 0:
        return []

    anomalies = []
    for i, ret in enumerate(returns):
        z = (ret - mean_ret) / std_ret
        if abs(z) > z_threshold:
            anomalies.append({
                "token_id": prices[i + 1]["token_id"],
                "ticker": prices[i + 1]["ticker"],
                "anomaly_date": prices[i + 1]["timestamp"],
                "z_score": round(z, 3),
                "direction": "spike" if z > 0 else "crash",
                "price": prices[i + 1]["price_usd"],
                "pct_change": round(ret * 100, 2),
            })
    return anomalies


def ingest_and_detect():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    # Current prices
    prices = fetch_prices()
    for p in prices:
        producer.send("mda.crypto.prices", p)
    logger.info("Published %d current crypto prices", len(prices))

    # Historical + anomaly detection
    for token_id, ticker in TRACKED_TOKENS.items():
        try:
            hist = fetch_historical(token_id, days=90)
            anomalies = detect_anomaly(hist)
            for a in anomalies:
                producer.send("mda.crypto.anomalies", a)
            if anomalies:
                logger.info("Detected %d anomalies for %s", len(anomalies), ticker)
            time.sleep(2)  # Rate limit
        except Exception as e:
            logger.error("CoinGecko error for %s: %s", ticker, e)

    producer.flush()
    producer.close()


def run_polling_loop(interval_hours: int = 4):
    logger.info("CoinGecko poller started (interval=%dh)", interval_hours)
    while True:
        try:
            ingest_and_detect()
        except Exception as e:
            logger.error("CoinGecko polling error: %s", e)
        time.sleep(interval_hours * 3600)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_polling_loop()
