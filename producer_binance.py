
import os
import asyncio
import json
from websockets.asyncio.client import connect
from kafka import KafkaProducer

COIN_PAIRS_JSON = os.getenv('COIN_PAIRS', '["btcusdt"]')
DATA_TYPE = os.getenv('DATA_TYPE', '@trade')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'trades')
ROWS_LIMIT = 100

pairs = json.loads(COIN_PAIRS_JSON)
streams = [f"{pair.strip().lower()}{DATA_TYPE}" for pair in pairs]
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
    '@ticker': {
        'symbol': 's',
        'event_time': 'E',
        'price': 'c',
        'price_change_percent': 'P',
        'high_price': 'h',
        'low_price': 'l', 
        'volume': 'v', 
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
        no_trades = 0
        while True:
            message = await websocket.recv()
            
            raw_data = json.loads(message)

            tx = raw_data.get('data')

            if not tx:
                continue

            if DATA_TYPE == '@kline_1m':
                tx = tx['k']

            current_map = FIELD_MAPPING[DATA_TYPE]
            
            temp_tx = {}

            for key, value in current_map.items():
                temp_tx[key] = tx.get(value)
            
            print(temp_tx)
            producer.send(KAFKA_TOPIC, value = temp_tx)
            no_trades += 1
            if no_trades == ROWS_LIMIT:
                producer.flush()
                producer.close()
                break



if __name__ == "__main__":
    asyncio.run(produce())
