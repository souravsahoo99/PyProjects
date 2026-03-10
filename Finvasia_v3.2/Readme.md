#####################_ System Components _########################

ShoonyaAPI_helper
helper_wrapper
token_registry
instrument_node
market_data    <-data pipelines
pineseries_adapter
indicator_utils
strategy_utils
signal_engine
trade_manager
main_.py

####################_Broker API - public call functions_################

api_helper.py
    ├── Order class
    ├── ShoonyaApi (wrapper over NorenApi)
            ├── login()
            ├── set_session()
            ├── get_quotes()
            ├── get_time_price_series()
            ├── place_order()
            ├── modify_order()
            ├── cancel_order()
            ├── start_websocket()
            ├── subscribe()
            ├── close_websocket()
            ├── get_positions()
            ├── get_holdings()
            ├── get_limits()
            ├── single_order_history()
            ├── get_order_book()
            ├── get_trade_book()
            ├── searchscrip()
            ├── get_option_chain()
            ├── get_security_info()
            ├── get_daily_price_series()
            ├── span_calculator()
            ├── option_greek()

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

####################_ Runtime Interaction of main engine_######################

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

