
import os
import asyncio
import json
from websockets.asyncio.client import connect
from kafka import KafkaProducer

COIN_PAIR = os.getenv('COIN_PAIR', 'btcusdt')
DATA_TYPE = os.getenv('DATA_TYPE', '@trade')
WEBSOCKET_STREAM_URL = f'wss://stream.binance.com:9443/ws/{COIN_PAIR}{DATA_TYPE}' # "wss://stream.binance.com:9443/ws/btcusdt@trade

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'trades')

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
        'high': 'h',
        'low': 'l', 
        'volume': 'v', 
    },
    '@kline_1m': {
        'symbol': 's',
        'event_time': 't',
        'open_price': 'o',
        'high': 'h',
        'low': 'l',
        'close_price': 'c',
        'volume': 'v',
        'taker_buy_volume': 'V',
        'kline_closed': 'x'        
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
            
            tx = json.loads(message)

            if DATA_TYPE == '@kline_1m':
                tx = tx['k']

            current_map = FIELD_MAPPING[DATA_TYPE]
            
            temp_tx = {}

            for key, value in current_map.items():
                temp_tx[key] = tx[value]
            
            print(temp_tx)
            producer.send(KAFKA_TOPIC, value = temp_tx)
            no_trades += 1
            if no_trades == 10:
                producer.flush()
                producer.close()
                break



if __name__ == "__main__":
    asyncio.run(produce())
