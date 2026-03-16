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


