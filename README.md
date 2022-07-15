# Bitcoin_Price_Websocket_allExchanges
Description:
Print streaming Bitcoin price using websocket in multiple exchanges (Binance,Huobi,Coinbase,etc). 

Infrastructure:
Python3
Some Exchange-developed package
Multi Processing (which utilize one physical thread for each exchange connection)

Supported Exchange:
AAX
Binance
Binance US
Bitfinex
Bybit
Coinbase
FTX
Gate.io
Gemini
Huobi Global
Kraken
Kucoin

(more to add upon request)

Remark:
1)I have not added reconnect logic in this version. That means when network issue happened (either you disconnected or exchange server disconnected), you will drop the connection instead of keep receiving the price flow in. This part will be enhanced in later version.
2)No requirements.txt for now. Please install packages one-by-one.

creator:yeunghy0516@gmail.com
