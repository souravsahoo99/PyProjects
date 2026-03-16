┌───────────────────────────────────────────────────────────────────────────────┐
│                               SYSTEM BOOT                                     │
│                                                                               │
│  main.py (Orchestrator)                                                       │
│                                                                               │
│  ├─ Load configuration                                                        │
│  ├─ Initialize APIEngine                                                      │
│  ├─ Initialize DataServant                                                    │
│  ├─ Initialize StrategyExecutor                                               │
│  ├─ Initialize TradeManager                                                   │
│  └─ Spawn InstrumentNodes                                                     │
└───────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                          INSTRUMENT NODE (RUNTIME UNIT)                        │
│                                                                               │
│  Example Nodes                                                                │
│                                                                               │
│  ├─ Parent Node                                                               │
│  │    symbol: NIFTY                                                           │
│  │    type: FUT                                                               │
│  │    scope: PARENT                                                           │
│  │                                                                            │
│  └─ Child Nodes                                                               │
│       symbol: NIFTY_CE / NIFTY_PE                                             │
│       type: OPT                                                               │
│       scope: CHILD                                                            │
└───────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                           DATA PIPELINE LAYER                                  │
│                                                                               │
│  DataServant                                                                  │
│  (centralized candle gateway)                                                 │
│                                                                               │
│  exchange|token → MarketDataManager                                           │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐  │
│  │ MarketDataManager                                                       │  │
│  │                                                                         │  │
│  │   REST Aggregator                                                       │  │
│  │     └─ 1m CandleBuffer                                                  │  │
│  │                                                                         │  │
│  │   Tick Aggregator                                                       │  │
│  │     ├─ 30s CandleBuffer                                                 │  │
│  │     ├─ 15s CandleBuffer                                                 │  │
│  │     └─ 10s CandleBuffer                                                 │  │
│  │                                                                         │  │
│  │   Tick Queue                                                            │  │
│  │                                                                         │  │
│  │   Async Workers                                                         │  │
│  │     ├─ REST polling loop                                                │  │
│  │     └─ Tick aggregation loop                                            │  │
│  └─────────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                        SERIES ADAPTER LAYER                                    │
│                                                                               │
│  Converts CandleBuffers into PineScript-style series                         │
│                                                                               │
│  SeriesAdapter                                                                │
│                                                                               │
│  ├─ open()                                                                    │
│  ├─ high()                                                                    │
│  ├─ low()                                                                     │
│  ├─ close()                                                                   │
│  └─ volume()                                                                  │
│                                                                               │
│  Series indexing                                                              │
│                                                                               │
│     close[0] → current candle                                                 │
│     close[1] → previous candle                                                │
│     close[2] → earlier candle                                                 │
└───────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                           SIGNAL ENGINE LOOP                                   │
│                                                                               │
│  Runs continuously inside each InstrumentNode                                 │
│                                                                               │
│  LOOP                                                                         │
│   │                                                                           │
│   ├─ fetch candle buffers                                                     │
│   │                                                                           │
│   ├─ create Pine-style series                                                 │
│   │                                                                           │
│   ├─ build strategy context                                                   │
│   │                                                                           │
│   │   context = {                                                             │
│   │       symbol                                                              │
│   │       token                                                               │
│   │       servant                                                             │
│   │       open/high/low/close                                                 │
│   │       df                                                                  │
│   │   }                                                                       │
│   │                                                                           │
│   ├─ request strategies from StrategyExecutor                                 │
│   │                                                                           │
│   └─ evaluate strategies                                                      │
└───────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                         STRATEGY EXECUTION LAYER                               │
│                                                                               │
│  StrategyExecutor                                                             │
│                                                                               │
│  Filters strategies using:                                                    │
│                                                                               │
│     instrument_type                                                           │
│     node_scope                                                                │
│                                                                               │
│                                                                               │
│  PARENT NODE STRATEGIES                                                       │
│                                                                               │
│     StrategyORB                                                               │
│     StrategyVWAPDeviation                                                     │
│     StrategyMarketProfileBreak                                                │
│                                                                               │
│                                                                               │
│  CHILD NODE STRATEGIES                                                        │
│                                                                               │
│     StrategyChildBreakout                                                     │
│     StrategyChildPullback                                                     │
│     StrategyChildMomentum                                                     │
│     StrategyChildLiquiditySweep                                               │
│                                                                               │
│                                                                               │
│  Each strategy returns                                                        │
│                                                                               │
│     BUY                                                                       │
│     SELL                                                                      │
│     None                                                                      │
└───────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                              SIGNAL BUS                                        │
│                                                                               │
│  Global signal stream                                                         │
│                                                                               │
│  Example runtime events                                                       │
│                                                                               │
│  09:30:10  CHILD_PULLBACK BUY                                                 │
│  09:30:12  CHILD_MOMENTUM BUY                                                 │
│  09:30:14  CHILD_BREAKOUT BUY                                                 │
│                                                                               │
│  09:30:16  PARENT_ORB BUY                                                     │
│                                                                               │
└───────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                           TRADE MANAGER                                       │
│                                                                               │
│  Continuous monitoring loop                                                   │
│                                                                               │
│  ├─ receive parent signal                                                     │
│  │                                                                            │
│  ├─ search signal bus                                                         │
│  │                                                                            │
│  ├─ match child readiness                                                     │
│  │                                                                            │
│  │   Parent BUY                                                               │
│  │       └─ choose CE readiness                                               │
│  │                                                                            │
│  │   Parent SELL                                                              │
│  │       └─ choose PE readiness                                               │
│  │                                                                            │
│  ├─ build order request                                                       │
│  │                                                                            │
│  └─ execute via APIEngine                                                     │
└───────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                            BROKER LAYER                                        │
│                                                                               │
│  APIEngine                                                                    │
│                                                                               │
│  ├─ place order                                                               │
│  ├─ confirm order                                                             │
│  ├─ fetch positions                                                           │
│  └─ update trade state                                                        │
└───────────────────────────────────────────────────────────────────────────────┘


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




# Runtime behavior


Tick arrives
     │
MarketDataManager updates candles
     │
SeriesAdapter builds series
     │
SignalEngine evaluates strategies
     │
Child strategies populate signal bus
     │
Parent strategy emits direction
     │
TradeManager matches signals
     │
Order sent to broker

