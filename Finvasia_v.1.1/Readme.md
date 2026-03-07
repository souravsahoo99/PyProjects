#####################_ System Components _########################

Shoonya_API_helper
helper_wrapper
TradeManager
Strategy layer        <-(coming in future)

########################_ LOGIC FLOW _############################

*
|      start program
|       ↓
|      login broker
|       ↓
|      start websocket
|       ↓
|      get LTP
|       ↓
|      strategy signal
|       ↓
|      place order
|       ↓
|      monitor trade
|       ↓
|      exit trade
*

####################_ Runtime Interaction _######################

PROGRAM START
      │
      ▼
Create ShoonyaEngine
      │
      ▼
Broker Login
      │
      ▼
Start WebSocket
      │
      ▼
Create LTP Handlers
      │
      ├─ WebsocketLTP
      └─ RestLTP
      │
      ▼
Create TradeManager
      │
      ▼
TradeManager.run()
      │
      ▼
STATE MACHINE LOOP

#####################_  Data Flow During Runtime  _#########################

================ Price Flow ===============
Broker
   │
   ▼
WebSocket
   │
   ▼
ShoonyaEngine tick cache
   │
   ▼
WebsocketLTP
   │
   ▼
get_best_ltp()
   │
   ▼
TradeManager

>> fallback
REST API
   │
   ▼
RestLTP
   │
   ▼
get_best_ltp()
================ Order Flow ===============
TradeManager
     │
     ▼
engine.api.place_order()
     │
     ▼
Shoonya_API_helper
     │
     ▼
NorenApi
     │
     ▼
Broker

#####################  _  #########################

