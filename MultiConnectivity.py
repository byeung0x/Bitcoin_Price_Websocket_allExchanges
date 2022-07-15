
import websocket
import json
import datetime
import time
from multiprocessing import Process

from package.websocket_manager import WebsocketManager
from typing import DefaultDict, Deque, List, Dict, Tuple, Optional
from collections import defaultdict, deque
from gevent.event import Event
from itertools import zip_longest
import zlib

from dateutil import parser

from binance.websocket.spot.websocket_client import SpotWebsocketClient as Client

import requests

import ssl

import gzip
import codecs

class AAX:
    def __init__(self):
        self.ws = None
        self.reset_data()

    def reset_data(self):
        self._subscribe_params = []
        self._connect()

    def on_open(self,ws):
        print("AAX websocket open")
        self._subscribe(params={"e": "subscribe", "stream": ["BTCUSDT@trade"]})

    def on_message(self,ws,message):
        msg = json.loads(message)
        if msg['e'] == "BTCUSDT@trade":
            print("AAXXX",datetime.datetime.fromtimestamp(msg['t']/1000),abs(round(float(msg['p']),1)),round(float(msg['q']),4))

    def _connect(self):
        STREAM_HOST = 'wss://realtime.aax.com/marketdata/v2/'

        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(STREAM_HOST,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)
        self.ws.run_forever(ping_interval=1)

    def _reconnect(self,ws):
        if self.ws is not None:
            self.ws = None
            print("AAX websocket reconnecting")
            return self._connect()

    def _subscribe(self,params):
        self._subscribe_params.append(params)
        self.ws.send(json.dumps(params))
        print("AAX websocket subscribe",params)

    def on_error(self,ws, error):
        print(error)

    def on_close(self,ws,*args):
        print("AAX websocket closed")

class FTX:
    def __init__(self):
        self.ws = None
        self.reset_data()

    def reset_data(self):
        self._connect()

    def on_open(self,ws):
        print("FTX websocket open")
        self._subscribe(params={'op': 'subscribe','channel': 'trades', 'market': "BTC/USD"})

    def on_message(self,ws, message):
        msg = json.loads(message)
        if msg['channel'] == 'trades' and msg['type'] == 'update':
            for each in msg['data']:
                print("FTXXX",datetime.datetime.fromtimestamp(parser.parse(each['time']).timestamp()+28800),each['price'],each['size'])

    def _connect(self):
        STREAM_HOST = 'wss://ftx.com/ws/'

        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(STREAM_HOST,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)
        self.ws.run_forever(ping_interval=1)

    def _reconnect(self,ws):
        if self.ws is not None:
            self.ws = None
            print("FTX websocket reconnecting")
            return self._connect()

    def _subscribe(self,params):
        self.ws.send(json.dumps(params))
        print("FTX websocket subscribe",params)

    def on_error(self,ws, error):
        print(error)

    def on_close(self,ws,*args):
        print("FTX websocket closed")

class COINBASE:
    def __init__(self):
        self.ws = None
        self.reset_data()

    def reset_data(self):
        self._connect()

    def on_open(self,ws):
        print("COINBASE websocket open")
        self._subscribe(params=
            {
            "type": "subscribe",
            "product_ids": [
                "BTC-USD"
            ],
            "channels": ["matches"]
            }
        )

    def on_message(self,ws, message):
        msg = json.loads(message)
        if msg.get('type') == 'match':
            print("COINS",datetime.datetime.fromtimestamp(parser.parse(msg['time']).timestamp()),abs(round(float(msg['price']),1)),round(float(msg['size']),4))

    def _connect(self):
        STREAM_HOST = 'wss://ws-feed.exchange.coinbase.com'

        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(STREAM_HOST,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)
        self.ws.run_forever(ping_interval=1)

    def _reconnect(self,ws):
        if self.ws is not None:
            self.ws = None
            print("COINBASE websocket reconnecting")
            return self._connect()

    def _subscribe(self,params):
        self.ws.send(json.dumps(params))
        print("COINBASE websocket subscribe",params)

    def on_error(self,ws, error):
        print(error)

    def on_close(self,ws,*args):
        print("COINBASE websocket closed")

class KRAKEN:
    def __init__(self):
        self.ws = None
        self.reset_data()

    def reset_data(self):
        self._connect()

    def on_open(self,ws):
        print("KRAKEN websocket open")
        self._subscribe(params=
            {
            "event": "subscribe",
            "pair": [
                "XBT/USD"
            ],
            "subscription": {
                "name": "trade"
            }
            }
        )

    def on_message(self,ws, message):
        msg = json.loads(message)
        if isinstance(msg,list):
            if msg[2] == 'trade':
                for each in msg[1]:
                    print("KRAKN",datetime.datetime.fromtimestamp(float(each[2])),abs(round(float(each[0]),1)),round(float(each[1]),4))

    def _connect(self):
        STREAM_HOST = 'wss://ws.kraken.com'

        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(STREAM_HOST,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)
        self.ws.run_forever(ping_interval=1)

    def _reconnect(self,ws):
        if self.ws is not None:
            self.ws = None
            print("KRAKEN websocket reconnecting")
            return self._connect()

    def _subscribe(self,params):
        self.ws.send(json.dumps(params))
        print("KRAKEN websocket subscribe",params)

    def on_error(self,ws, error):
        print(error)

    def on_close(self,ws,*args):
        print("KRAKEN websocket closed")

class BYBIT:
    def __init__(self):
        self.ws = None
        self.reset_data()

    def reset_data(self):
        self._connect()

    def on_open(self,ws):
        print("BYBIT websocket open")
        self._subscribe(params={
            "topic": "trade",
            "params": {
                "symbol": "BTCUSDT",
                "binary": False
            },
            "event": "sub"
        })

    def on_message(self,ws,message):
        msg = json.loads(message)
        if msg['topic'] == "trade" and 'data' in msg:
            print("BYBIT",datetime.datetime.fromtimestamp(msg['data']['t']/1000),abs(round(float(msg['data']['p']),1)),round(float(msg['data']['q']),4))

    def _connect(self):
        STREAM_HOST = 'wss://stream.bybit.com/spot/quote/ws/v2'

        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(STREAM_HOST,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)
        self.ws.run_forever(ping_interval=1)

    def _reconnect(self,ws):
        if self.ws is not None:
            self.ws = None
            print("BYBIT websocket reconnecting")
            return self._connect()

    def _subscribe(self,params):
        self.ws.send(json.dumps(params))
        print("BYBIT websocket subscribe",params)

    def on_error(self,ws, error):
        print(error)

    def on_close(self,ws,*args):
        print("BYBIT websocket closed")

class KUCOIN:
    def __init__(self):
        self.ws = None
        self.reset_data()

    def reset_data(self):
        url = 'https://api.kucoin.com'
        params = '/api/v1/bullet-public'
        res = requests.post(f"{url}{params}",timeout=5).json()
        self._wss = res['data']['instanceServers'][0]['endpoint']
        self._token = res['data']['token']

        self._connect()

    def on_open(self,ws):
        print("KUCOIN websocket open")
        self._subscribe(params={
            "id": 333,                          
            "type": "subscribe",
            "topic": "/market/match:BTC-USDT"                           
        })

    def on_message(self,ws,message):
        msg = json.loads(message)
        if 'data' in msg:
            if msg['data']['type'] == 'match':
                print("KUCON",datetime.datetime.fromtimestamp(float(msg['data']['time'])/1000000000),abs(round(float(msg['data']['price']),1)),round(float(msg['data']['size']),4))

    def _connect(self):
        STREAM_HOST = f'{self._wss}?token={self._token}&[connectId=222]'

        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(STREAM_HOST,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)
        self.ws.run_forever(ping_interval=1)

    def _reconnect(self,ws):
        if self.ws is not None:
            self.ws = None
            print("KUCOIN websocket reconnecting")
            return self._connect()

    def _subscribe(self,params):
        self.ws.send(json.dumps(params))
        print("KUCOIN websocket subscribe",params)

    def on_error(self,ws, error):
        print(error)

    def on_close(self,ws,*args):
        print("KUCOIN websocket closed")

class BITFINEX:
    def __init__(self):
        self.ws = None
        self.reset_data()

    def reset_data(self):
        self._connect()

    def on_open(self,ws):
        print("BITFINEX websocket open")
        self._subscribe(params={"event": "subscribe", "channel": "trades", "symbol": "tBTCUSD"})

    def on_message(self,ws, message):
        msg = json.loads(message)
        if isinstance(msg,list):
            if isinstance(msg[1],str):
                if msg[1] == 'te':
                        print("BFNEX",datetime.datetime.fromtimestamp(msg[2][1]/1000),abs(round(float(msg[2][3]),1)),abs(round(float(msg[2][2]),4)))
                    

    def _connect(self):
        STREAM_HOST = 'wss://api-pub.bitfinex.com/ws/2'

        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(STREAM_HOST,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)
        self.ws.run_forever(ping_interval=1)

    def _reconnect(self,ws):
        if self.ws is not None:
            self.ws = None
            print("BITFINEX websocket reconnecting")
            return self._connect()

    def _subscribe(self,params):
        self.ws.send(json.dumps(params))
        print("BITFINEX websocket subscribe",params)

    def on_error(self,ws, error):
        print("Bitfinex Error",error)

    def on_close(self,ws,*args):
        print("Bitfinex websocket closed")

class GATEIO:
    def __init__(self):
        self.ws = None
        self.reset_data()

    def reset_data(self):
        self._connect()

    def on_open(self,ws):
        print("GATEIO websocket open")
        self._subscribe(params={"id":222, "method":"trades.subscribe", "params":["BTC_USDT"]})

    def on_message(self,ws, message):
        msg = json.loads(message)
        if 'method' in msg:
            if msg['method'] == 'trades.update':
                for each in msg['params'][1]:
                    print("GATIO",datetime.datetime.fromtimestamp(each['time']),abs(round(float(each['price']),1)),round(float(each['amount']),4))
                    
    def _connect(self):
        STREAM_HOST = 'wss://ws.gate.io/v3/'

        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(STREAM_HOST,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)
        self.ws.run_forever(ping_interval=1)

    def _reconnect(self,ws):
        if self.ws is not None:
            self.ws = None
            print("GATEIO websocket reconnecting")
            return self._connect()

    def _subscribe(self,params):
        self.ws.send(json.dumps(params))
        print("GATEIO websocket subscribe",params)

    def on_error(self,ws, error):
        print("GATEIO Error",error)

    def on_close(self,ws,*args):
        print("GATEIO websocket closed")

class GEMINI:
    def __init__(self):
        self.ws = None
        self.reset_data()

    def reset_data(self):
        self._connect()

    def on_open(self,ws):
        print("GEMINI websocket open")
        
    def on_message(self,ws, message):
        msg = json.loads(message)
        print(msg)
        if 'events' in msg:
            if msg['type'] == 'update':
                if msg['events'] != []:
                    for each in msg['events']:
                        if each['type'] == 'trade':
                            print("GEMNI",datetime.datetime.fromtimestamp(float(msg['timestamps'])/1000),abs(round(float(each['price']),1)),round(float(each['amount']),4))
                    
    def _connect(self):
        STREAM_HOST = 'wss://api.gemini.com/v1/marketdata/BTCUSD?bids=false&offers=false'

        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(STREAM_HOST,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)
        self.ws.run_forever(ping_interval=3,sslopt={"cert_reqs": ssl.CERT_NONE})

    def _reconnect(self,ws):
        if self.ws is not None:
            self.ws = None
            print("GEMINI websocket reconnecting")
            return self._connect()

    def _subscribe(self,params={}):
        self.ws.send(json.dumps(params))
        print("GEMINI websocket subscribe",params)

    def on_error(self,ws, error):
        print("GEMINI Error",error)

    def on_close(self,ws,*args):
        print("GEMINI websocket closed")
"""
class HUOBIGLOBAL:
    def __init__(self):
        from huobi.client.market import MarketClient
        market_client = MarketClient()
        market_client.sub_trade_detail("btcusdt", self.callback)

    def callback(self,trade_event: 'TradeDetailEvent'):
        for each in trade_event.data:
            print("HUOBG",datetime.datetime.fromtimestamp(float(each.ts)/1000),abs(round(float(each.price),1)),round(float(each.amount),4))
"""
class HUOBIGLOBAL:
    def __init__(self):
        self.ws = None
        self.reset_data()

    def reset_data(self):
        self._connect()

    def on_open(self,ws):
        print("HUOBIGLOBAL websocket open")
        self._subscribe(params={
        "sub": "market.btcusdt.trade.detail",
        "id": "222"
        })

    def on_message(self,ws,message):
        msg = json.loads(gzip.decompress(message).decode("utf-8"))
        if 'tick' in msg:
            for each in msg['tick']['data']:
                print("HUOBG",datetime.datetime.fromtimestamp(float(each['ts'])/1000),abs(round(float(each['price']),1)),round(float(each['amount']),4))
        elif 'ping' in msg:
            self._pong(params={'pong':msg['ping']})

    def _pong(self,params):
        self.ws.send(json.dumps(params))

    def _connect(self):
        STREAM_HOST = 'wss://api.huobi.pro/ws'

        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(STREAM_HOST,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)
        self.ws.run_forever(ping_interval=1)

    def _reconnect(self,ws):
        if self.ws is not None:
            self.ws = None
            print("HUOBIGLOBAL websocket reconnecting")
            return self._connect()

    def _subscribe(self,params):
        self.ws.send(json.dumps(params))
        print("HUOBIGLOBAL websocket subscribe",params)

    def on_error(self,ws, error):
        print(error)

    def on_close(self,ws,*args):
        print("HUOBIGLOBAL websocket closed")

class BINANCE:
    def __init__(self):
        my_client = Client()
        my_client.start()
        my_client.trade(
            symbol="btcusdt",
            id=222,
            callback=self.message_handler,
        )
    def message_handler(self,msg):
        if msg.get("p") is not None:
            print("BINCE",datetime.datetime.fromtimestamp(msg.get("T")/1000),abs(round(float(msg.get("p")),1)),round(float(msg.get("q")),4))

class BINANCEUS:
    def __init__(self):
        my_client = Client('wss://stream.binance.us:9443')
        my_client.start()
        my_client.trade(
            symbol="btcusdt",
            id=222,
            callback=self.message_handler,
        )
    def message_handler(self,msg):
        if msg.get("p") is not None:
            print("BNCUS",datetime.datetime.fromtimestamp(msg.get("T")/1000),abs(round(float(msg.get("p")),1)),round(float(msg.get("q")),4))

    
if __name__=='__main__':
    dict_processes = {}

    dict_processes['AAX_run'] = Process(target=AAX,args=()) #ok
    dict_processes['FTX_run'] = Process(target=FTX,args=())#ok
    dict_processes['Binance_run'] = Process(target=BINANCE,args=())#ok
    dict_processes['Coinbase_run'] = Process(target=COINBASE,args=())#ok
    dict_processes['Kraken_run'] = Process(target=KRAKEN,args=())#ok
    dict_processes['Bybit_run'] = Process(target=BYBIT,args=())#ok
    dict_processes['Kucoin_run'] = Process(target=KUCOIN,args=())#ok
    dict_processes['Binanceus_run'] = Process(target=BINANCEUS,args=())
    dict_processes['Bitfinex_run'] = Process(target=BITFINEX,args=())#ok
    dict_processes['Gateio_run'] = Process(target=GATEIO,args=())#ok
    dict_processes['Gemini_run'] = Process(target=GEMINI,args=())
    dict_processes['Huobiglobal_run'] = Process(target=HUOBIGLOBAL,args=())#ok
    

    for each in dict_processes:
        dict_processes[each].start()
        time.sleep(0.2)

    for each in dict_processes:
        dict_processes[each].join()


"""

to execute (this is Mac OS), run this command: *you cannot run this command if you don't have my virtual environment 'venv_pj1' im sorry:)
source ~/Desktop/python_project1/venv_pj1/bin/activate && python3 ~/Desktop/python_project_multi_exchange/MultiConnectivity.py

to activate virtual environment only, run this command:
~/Desktop/python_project1/venv_pj1/bin/activate



"""







