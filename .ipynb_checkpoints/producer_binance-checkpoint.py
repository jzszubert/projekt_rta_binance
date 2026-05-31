import os
import asyncio
import json
from websockets.asyncio.client import connect
from kafka import KafkaProducer

COIN_PAIRS_JSON = os.getenv('COIN_PAIRS', '["btcusdt"]')
pairs = json.loads(COIN_PAIRS_JSON)

streams = []
for pair in pairs:
    base = pair.strip().lower()
    streams.append(f"{base}@trade")
    streams.append(f"{base}@kline_1m")

streams_path = '/'.join(streams)
WEBSOCKET_STREAM_URL = f'wss://stream.binance.com:9443/stream?streams={streams_path}'

FIELD_MAPPING = {
    '@trade': {
        'symbol': 's',
        'trade_time': 'T',
        'price': 'p',
        'quantity': 'q',
        'market_maker': 'm',
        'trade_id': 't'
    },
    '@kline_1m': {
        'symbol': 's',
        'start_time': 't',
        'open_price': 'o',
        'high_price': 'h',
        'low_price': 'l',
        'close_price': 'c',
        'volume': 'v',
        'taker_buy_volume': 'V',
        'is_closed': 'x'        
    }
}

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

async def produce():
    async with connect(WEBSOCKET_STREAM_URL) as websocket:
        while True:
            message = await websocket.recv()
            raw_data = json.loads(message)
            
            stream_name = raw_data.get('stream')
            tx = raw_data.get('data')

            if not tx or not stream_name:
                continue

            if '@trade' in stream_name:
                data_type = '@trade'
                target_topic = 'trades'
            elif '@kline_1m' in stream_name:
                data_type = '@kline_1m'
                target_topic = 'klines'
                tx = tx['k']
            else:
                continue

            current_map = FIELD_MAPPING.get(data_type)
            if not current_map:
                continue
                
            temp_tx = {}
            for key, value in current_map.items():
                temp_tx[key] = tx.get(value)
            
            print(f"[{target_topic}] {temp_tx}")
            producer.send(target_topic, value=temp_tx)

if __name__ == "__main__":
    try:
        asyncio.run(produce())
    except KeyboardInterrupt:
        producer.flush()
        producer.close()