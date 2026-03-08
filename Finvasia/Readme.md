#####################_ System Components _########################

Shoonya_API_helper
helper_wrapper
token_registry
instrument_node
market_data    -data pipelines
pineseries_adapter
indicator utils
strategy utils
signal_engine
trade_manager
main.py

########################_ CORE LOGIC FLOW _############################

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

USER STRATEGY CONFIG
        │
        ▼
main.py (Orchestration Engine)
        │
        ├──────────── Signal Layer ─────────────
        │
        │        InstrumentNode (per instrument)
        │                │
        │                ▼
        │        MarketDataManager
        │                │
        │                ├── Tick Pipeline
        │                │        │
        │                │        └── TickCandleAggregator
        │                │
        │                └── REST Pipeline
        │                         │
        │                         └── RestCandleAggregator
        │
        │                ▼
        │          SignalEngine
        │                │
        │                ▼
        │        GLOBAL SIGNAL BUS
        │
        └──────────── Execution Layer ───────────
                 │
                 ▼
          TradeManager_v2
                 │
                 ├── Parent Signal Detection
                 │
                 ├── Product Type Routing
                 │       ├─ OPT → CE/PE selection
                 │       ├─ FUT → fixed token
                 │       └─ SPOT/STOCK → parent token
                 │
                 ├── Child Signal Confirmation
                 │
                 └── Order Execution


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

