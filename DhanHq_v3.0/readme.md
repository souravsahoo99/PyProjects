PROGRAM START
│
└── main.py (__main__)
    │
    ├── Supervisor Layer
    │   │
    │   ├── Create Event Loop
    │   │
    │   ├── Register Signals
    │   │     ├── SIGINT
    │   │     └── SIGTERM
    │   │
    │   └── Run Engine
    │         └── engine_bootloader()
    │
    │
    └───────────────────────────────────────────────
                    ENGINE BOOT
    └───────────────────────────────────────────────
        │
        ├── Initialize Broker Engine
        │     └── APIEngine()
        │
        ├── Initialize Market Data Map
        │
        ├── Load Instrument Registry
        │     └── TokenRegistry.load_master()
        │
        ├── Start WebSocket
        │     ├── engine.start_ws()
        │     └── engine.wait_for_ws()
        │
        └── System Ready
              │
              ▼


══════════════════════════════════════════════════════════════
NODE CREATION LAYER
══════════════════════════════════════════════════════════════

main.py
│
└── build_signal_nodes()
    │
    └── Loop STRATEGY_CONFIG
        │
        ├── Resolve Parent Token
        │     └── registry.get_token / get_futures
        │
        ├── Determine Node Scope
        │     ├── OPT  → CHILD
        │     └── STOCK/FUT → PARENT
        │
        └── Create InstrumentNode
              │
              └── InstrumentNode
                   │
                   ├── engine
                   ├── exchange
                   ├── symbol
                   ├── token
                   ├── instrument_type
                   └── node_scope



══════════════════════════════════════════════════════════════
NODE INITIALIZATION
══════════════════════════════════════════════════════════════

InstrumentNode.initialize()
│
├── Subscribe WebSocket
│     └── engine.subscribe(exchange, token)
│
├── Create DataServant
│     └── DataServant(engine)
│
├── Create SignalEngine
│     │
│     ├── engine
│     ├── symbol
│     ├── token
│     ├── instrument_type
│     └── node_scope
│
├── Strategy Discovery
│     │
│     └── StrategyExecutor.get_strategies()
│           │
│           ├── Filter by INSTRUMENT_SCOPE
│           └── Filter by NODE_SCOPE
│
├── Determine Required Timeframes
│     └── strategies → REQUIRED_TIMEFRAMES
│
└── Create MarketDataManager
      │
      ├── websocket tick ingestion
      ├── candle aggregation
      └── timeframe buffers



══════════════════════════════════════════════════════════════
DATA PIPELINE ARCHITECTURE
══════════════════════════════════════════════════════════════

Broker WebSocket
│
└── APIEngine
     │
     └── MarketDataManager
          │
          ├── Tick Stream
          │
          ├── Candle Aggregation
          │     ├── 1m
          │     ├── 3m
          │     ├── 5m
          │     └── 15m
          │
          └── Candle Buffers
                │
                ▼
             SignalEngine



══════════════════════════════════════════════════════════════
SIGNAL ENGINE EXECUTION
══════════════════════════════════════════════════════════════

SignalEngine.run()
│
├── Loop forever
│
├── Fetch Latest Candle
│     └── market_data.get("1m")
│
├── Detect New Candle
│
├── Build DataFrame
│
├── Update ORB State
│
├── Update Market Profile
│
└── Execute Strategies
      │
      ▼



══════════════════════════════════════════════════════════════
STRATEGY EXECUTION TREE
══════════════════════════════════════════════════════════════

StrategyExecutor.run()
│
├── StrategyORB
│     ├── breakout
│     └── breakdown
│
├── StrategyVWAPDeviation
│     ├── VWAP bands
│     └── deviation logic
│
├── StrategyMarketProfileBreak
│     ├── VAH breakout
│     └── VAL breakdown
│
├── StrategyMTFTrend
│     └── DataServant fetch
│         └── 5m timeframe
│
├── StrategyHTFBreakout
│     └── DataServant fetch
│         └── 15m timeframe
│
└── StrategyMTFVWAP
      └── DataServant fetch
          └── 3m timeframe


Strategies evaluate sequentially

First valid result → returned



══════════════════════════════════════════════════════════════
SIGNAL GENERATION
══════════════════════════════════════════════════════════════

SignalEngine
│
└── _publish_signal()
     │
     ├── Deduplicate
     │
     ├── Validate Publisher
     │
     └── Push to Global Bus
          │
          ▼
        SIGNALS[]



══════════════════════════════════════════════════════════════
TRADE EXECUTION LAYER
══════════════════════════════════════════════════════════════

main.py
│
└── build_trade_managers()
    │
    ├── Discover Options
    │
    ├── Determine CE/PE tokens
    │
    └── Create TradeManager
          │
          ▼
      TradeManager.run()
          │
          ├── Monitor SIGNALS bus
          │
          ├── Validate token match
          │
          ├── Execute trade
          │     └── broker API
          │
          ├── Manage stoploss
          │
          └── Manage trailing



══════════════════════════════════════════════════════════════
MONITORING SYSTEM
══════════════════════════════════════════════════════════════

DisplayMonitor Thread
│
├── Node status
├── LTP updates
├── Open trades
└── Signal logs



══════════════════════════════════════════════════════════════
ASYNC TASK ORCHESTRATION
══════════════════════════════════════════════════════════════

Async Tasks Created
│
├── Node SignalEngine loops
│
├── TradeManager loops
│
└── DisplayMonitor thread

All tasks merged via

asyncio.gather()



══════════════════════════════════════════════════════════════
SUPERVISOR PROTECTION
══════════════════════════════════════════════════════════════

If engine crashes
│
├── catch exception
│
├── restart attempt
│
└── restart engine



══════════════════════════════════════════════════════════════
FINAL EXECUTION TREE
══════════════════════════════════════════════════════════════

main.py
│
├── Supervisor
│
├── Engine Boot
│
├── Nodes
│   │
│   └── InstrumentNode
│        │
│        ├── MarketDataManager
│        │
│        ├── DataServant
│        │
│        └── SignalEngine
│              │
│              └── StrategyExecutor
│                    │
│                    └── Strategies
│
└── TradeManager
      │
      └── Broker Execution