import json
import os

os.environ.setdefault("SYMBOLS", "BTCUSDT")

from producer import kafka_config, to_record  # noqa: E402

SAMPLE = json.dumps({
    "stream": "btcusdt@trade",
    "data": {"e": "trade", "s": "BTCUSDT", "t": 1, "p": "83144.01", "q": "0.0001",
             "T": 1791914640658, "m": False},
})


def test_key_is_symbol():
    key, _ = to_record(SAMPLE)
    assert key == "BTCUSDT"


def test_value_keeps_trade_and_adds_received_at():
    _, value = to_record(SAMPLE)
    data = json.loads(value)
    assert data["t"] == 1 and data["p"] == "83144.01"
    assert "received_at" in data


def test_event_hubs_config(monkeypatch):
    monkeypatch.setenv(
        "EH_CONNECTION_STRING",
        "Endpoint=sb://ehns-test.servicebus.windows.net/;SharedAccessKeyName=x;SharedAccessKey=y",
    )
    cfg = kafka_config()
    assert cfg["bootstrap.servers"] == "ehns-test.servicebus.windows.net:9093"
    assert cfg["sasl.username"] == "$ConnectionString"


def test_local_config_without_event_hubs(monkeypatch):
    monkeypatch.delenv("EH_CONNECTION_STRING", raising=False)
    cfg = kafka_config()
    assert cfg["enable.idempotence"] is True
    assert "sasl.password" not in cfg
