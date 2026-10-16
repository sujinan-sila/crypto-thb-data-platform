"""Binance trade stream -> Kafka topic `trades` (local Kafka or Azure Event Hubs)."""

import asyncio
import json
import logging
import os
import time

import websockets
from confluent_kafka import Producer

TOPIC = os.environ.get("KAFKA_TOPIC", "trades")
SYMBOLS = [s.strip().lower() for s in os.environ["SYMBOLS"].split(",")]
URL = "wss://data-stream.binance.vision/stream?streams=" + "/".join(
    f"{s}@trade" for s in SYMBOLS
)

log = logging.getLogger("producer")


def kafka_config() -> dict:
    """Plain local Kafka, or Event Hubs when EH_CONNECTION_STRING is set."""
    conn = os.environ.get("EH_CONNECTION_STRING")
    if not conn:
        return {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
            "enable.idempotence": True, 
            "acks": "all",
            "linger.ms": 50,
        }
    namespace = conn.split("sb://")[1].split(".")[0]
    return {
        "bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "$ConnectionString",  
        "sasl.password": conn,
        "acks": "all",
        "linger.ms": 50,
    }


def to_record(raw: str) -> tuple[str, str]:
    """WebSocket message -> (key, value) for Kafka."""
    data = json.loads(raw)["data"]
    data["received_at"] = int(time.time() * 1000)
    return data["s"], json.dumps(data)


def on_delivery(err, msg):
    if err is not None:
        log.error("delivery failed: %s", err)


async def run(producer: Producer):
    backoff = 1
    while True:
        try:
            async with websockets.connect(URL, ping_interval=20, ping_timeout=20) as ws:
                log.info("connected: %d symbols", len(SYMBOLS))
                backoff = 1
                sent, window_start = 0, time.time()
                async for raw in ws:
                    key, value = to_record(raw)
                    producer.produce(
                        TOPIC, key=key, value=value, on_delivery=on_delivery
                    )
                    producer.poll(0)
                    sent += 1
                    if time.time() - window_start >= 60:
                        log.info("sent %d trades in the last minute", sent)
                        sent, window_start = 0, time.time()
        except (websockets.ConnectionClosed, OSError) as exc:
            
            log.warning("disconnected (%s), reconnecting in %ss", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    producer = Producer(kafka_config())
    try:
        asyncio.run(run(producer))
    finally:
        producer.flush(10)
