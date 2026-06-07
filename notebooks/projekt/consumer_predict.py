import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker:9092")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "klines")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "alerts")
API_URL = os.getenv("API_URL", "http://localhost:8000/predict")
GROUP_ID = os.getenv("GROUP_ID", "ml-predict-consumer")
TZ = ZoneInfo("Europe/Warsaw")

consumer = KafkaConsumer(
    SOURCE_TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset="latest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def call_predict(price: float, volume: float) -> dict:
    resp = requests.post(API_URL, json={"price": price, "volume": volume}, timeout=5)
    resp.raise_for_status()
    return resp.json()


def main():
    print(f"[consumer] '{SOURCE_TOPIC}', API={API_URL} ")
    for msg in consumer:
        kline = msg.value
        if not kline.get("is_closed"):
            continue
        try:
            price = float(kline["close_price"])
            volume = float(kline["volume"])
        except (TypeError, ValueError, KeyError):
            continue
        try:
            result = call_predict(price, volume)
        except requests.RequestException as e:
            print(f"[consumer] błąd wywołania API: {e}")
            continue

        symbol = kline.get("symbol")
        is_anomaly = bool(result.get("anomaly"))
        score = result.get("score")
        print(f"[consumer] {symbol}, price={price}, volume={volume} -> anomaly={is_anomaly} (score={score})")

        if is_anomaly:
            event_time = None
            if kline.get("start_time"):
                event_time = datetime.fromtimestamp(kline["start_time"] / 1000, tz=TZ).strftime("%Y-%m-%d %H:%M:%S")

            alert = {
                "alert_type": "ANOMALY",
                "symbol": symbol,
                "event_time": event_time,
                "detected_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
                "price": price,
                "volume": volume,
                "score": score,
            }
            producer.send(ALERTS_TOPIC, value=alert)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        producer.flush()
        producer.close()
        consumer.close()
