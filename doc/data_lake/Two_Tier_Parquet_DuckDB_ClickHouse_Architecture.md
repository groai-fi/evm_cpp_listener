# Two-Tier Institutional Data Lake Architecture: Apache Arrow / Parquet + Embedded DuckDB & Future ClickHouse

> **Bottom Line Up Front (BLUF)**:  
> Quant desks and institutional trading firms build their market-data persistence in two decoupled tiers on top of the real-time Redis Streams hotbus:
> 1. **Tier 1 (Build First — Quant Enclave & Golden Record):** An in-process **Apache Arrow + Embedded DuckDB** archiver worker with **zero listening network ports** that consumes `evml:{<shard>}:events`, `evml:{<shard>}:fast`, and `evml:{<shard>}:registry` via `XREADGROUP`, sorts batches by `(block_number, log_index)`, writes Zstd-compressed Hive-partitioned `.parquet` files atomically to local NVMe (or private MinIO/S3), and calls `XACK` only after `fsync` + atomic `rename()`.
> 2. **Tier 2 (Plug In Later — Multi-User Live OLAP & Grafana):** A **ClickHouse** server that backfills its historical tables directly from Tier 1's `.parquet` files in one SQL command and tails Redis via an independent consumer group (`cg_clickhouse_hot`) with **zero changes** to `evm_listener`, live trading bots, or Tier 1.

---

## 1. Why Quants & Institutions Choose `DuckDB + Arrow/Parquet`

Generic YAML ETL tools (such as Benthos / Redpanda Connect) and traditional row-oriented databases are rarely used by quant desks for core on-chain tick/log storage because quantitative research requires four strict guarantees:

| Institutional Requirement | Why `DuckDB` / `Arrow` + `Parquet` Wins | Why Generic ETL / Row DBs Lose |
| :--- | :--- | :--- |
| **1. Zero-Copy Memory Handoff (`Arrow` ABI)** | **DuckDB**, **Polars**, **PyArrow**, and **C++ `libarrow`** share the exact same in-memory Apache Arrow columnar layout. A quant queries 100M swaps in DuckDB and hands the memory pointer to Polars, NumPy, or C++ in **0 ms (zero serialization)**. | Traditional SQL DBs serialize rows over a TCP socket and allocate new Python/C++ objects on every query. |
| **2. Exact DeFi Numeric Precision (`uint256` / `sqrtPriceX96`)** | EVM values (`sqrtPriceX96`, `uint128 liquidity`, `int256 amount0`) exceed 64-bit floats ($2^{53}$). DuckDB/Parquet natively store `HUGEINT` (`int128`), `DECIMAL(38,0)`, or raw hex/binary alongside decoded fields without precision loss. | Generic JSON-to-Parquet connectors frequently coerce numbers into `double` (`float64`), silently corrupting wei-level swap math. |
| **3. Immutable Point-in-Time Reproducibility** | Hive-partitioned Parquet files (`stream=events/chain_id=8453/dt=2026-11-03/*.parquet`) are **immutable, append-only artifacts**. A backtest run today and 6 months from now reads the exact same bytes. | Mutable databases can have rows updated, deduplicated differently, or re-indexed between backtest runs. |
| **4. Zero-Exfiltration Security (Enclave Compliance)** | Runs **in-process (`libduckdb` / `pyarrow`) with zero listening network ports**, writing directly to local encrypted NVMe or private on-prem MinIO. | Running a database daemon exposes listening ports (`9000`/`5432`) inside the trading subnet. |

---

## 2. End-to-End 2-Tier Institutional Architecture Chart

```
====================================================================================================
                                 LAYER 0: REAL-TIME HOTBUS (RAM)
====================================================================================================
  ┌──────────────────────────┐          ┌──────────────────────────┐
  │    evm_cpp_listener      │          │       cpp-ethcall        │
  │ (WS Tip + Safe/Finalized)│          │ (Warm HTTP + Multicall3) │
  └────────────┬─────────────┘          └────────────┬─────────────┘
               │ XADD (Lua fenced)                   │ XADD (Optional state snapshots)
               ▼                                     ▼
  ┌────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                                  REDIS STREAMS (In-Memory Bus)                                 │
  │   • evml:{shard}:fast     (1h retention, ~80ms latency, finality="latest", observed_ms)        │
  │   • evml:{shard}:events   (7d retention, ~22s safe / ~13m finalized / revert before-images)    │
  │   • evml:{shard}:registry (30d retention, factory PoolCreated / GaugeCreated)                  │
  └──────┬───────────────────────────────────┬───────────────────────────────────────┬─────────────┘
         │ Consumer Group 1                  │ Consumer Group 2                      │ Consumer Group 3
         │ (cg_live_strategy)                │ (cg_parquet_archiver)                 │ (cg_clickhouse_hot)
         ▼                                   ▼                                       ▼
┌─────────────────┐  =================================================  ============================
│ LIVE EXECUTION  │   TIER 1: QUANT ENCLAVE (BUILD NOW — 0 OPEN PORTS)   TIER 2: MULTI-USER OLAP
│ Strategy / Bot  │  =================================================   (PLUG IN LATER — 0 REFACTOR)
│ (~1–5ms react)  │                          │                          ============================
└─────────────────┘                          ▼                                       │
                    ┌─────────────────────────────────────────────────┐              │
                    │         Archiver Worker (Python / C++)          │              │
                    │  1. XREADGROUP batch (e.g. 5,000 msgs or 60s)   │              │
                    │  2. Validate schema v=1 & preserve 256-bit hex  │              │
                    │  3. In-Memory Apache Arrow Table                │              │
                    │  4. Embedded DuckDB:                            │              │
                    │     ORDER BY block_number, log_index            │              │
                    │     COPY TO '.tmp_part.parquet' (ZSTD)          │              │
                    │  5. fsync() + atomic rename() -> part.parquet   │              │
                    │  6. Redis XACK (guaranteed zero data loss)      │              │
                    └────────────────────────┬────────────────────────┘              │
                                             │ Direct Disk / S3 Write                │
                                             ▼                                       ▼
                    ┌─────────────────────────────────────────────────┐    ┌───────────────────────┐
                    │   IMMUTABLE PARQUET DATA LAKE (NVMe / MinIO)    │    │   ClickHouse Server   │
                    │  /data/parquet/                                 │    │  (ReplacingMergeTree) │
                    │   ├─ stream=events/chain_id=8453/dt=2026-11-03/ │───►│  • Backfills directly │
                    │   │   └─ part-1762185600.parquet (ZSTD, Sorted) │    │    from Tier 1 Parquet│
                    │   ├─ stream=fast/chain_id=8453/dt=2026-11-03/   │    │  • Live tail from CG3 │
                    │   └─ stream=registry/chain_id=8453/dt=2026-11/  │    └───────────┬───────────┘
                    └────────────────────────┬────────────────────────┘                │
                                             │ Zero-Copy Arrow mmap                    │ TCP :9000 / HTTP :8123
                                             ▼                                         ▼
                    ┌─────────────────────────────────────────────────┐    ┌───────────────────────┐
                    │        QUANT RESEARCH & BACKTESTING             │    │  LIVE OPS & GRAFANA   │
                    │  • Embedded DuckDB SQL (in Jupyter / CLI)       │    │  • 24/7 Grafana Panels│
                    │  • Polars / PyArrow (Zero-copy DataFrames)      │    │  • Multi-User Ad-Hoc  │
                    │  • C++ Backtester (libparquet / libarrow)       │    │    SQL Across Desk    │
                    └─────────────────────────────────────────────────┘    └───────────────────────┘
```

---

## 3. Inside Tier 1: The Atomic Write-Rename-Ack Protocol

To ensure a crash or power loss never loses events or exposes a half-written `.parquet` file to concurrent quant queries, the archiver follows a strict 5-step atomic commit protocol:

```
Redis Stream                   Archiver Worker (Arrow + DuckDB)               Local NVMe / MinIO
────────────                   ────────────────────────────────               ──────────────────
     │                                        │                                       │
     │ 1. XREADGROUP (count=5000, block=2s)   │                                       │
     ├───────────────────────────────────────►│ (Messages sit in Redis PEL un-ACKed)  │
     │                                        │                                       │
     │                                        │ 2. Build Arrow Table in RAM           │
     │                                        │    Sort by (block_number, log_index)  │
     │                                        │                                       │
     │                                        │ 3. DuckDB COPY ... TO '.tmp_part.pq'  │
     │                                        ├──────────────────────────────────────►│ (Hidden dotfile;
     │                                        │                                       │  ignored by glob)
     │                                        │ 4. fsync + rename('.tmp..', 'part..') │
     │                                        ├──────────────────────────────────────►│ (Atomically visible
     │                                        │                                       │  to quant queries)
     │ 5. XACK (all 5000 msg_ids)             │                                       │
     │◄───────────────────────────────────────┤                                       │
     │ (Cleared from PEL)                     │                                       │
```

### Three Critical Engineering Guarantees:
1. **No Half-Written Files Seen by Readers:** DuckDB and Polars glob patterns (`read_parquet('.../dt=*/part-*.parquet')`) match `part-*.parquet` and ignore `.tmp_*.parquet`. Because POSIX `rename()` on the same filesystem is atomic, a file appears to readers only after its Parquet footer and Zstd row groups are 100% written and flushed.
2. **Crash Recovery via Redis PEL (`XAUTOCLAIM`):** If the archiver crashes at Step 2 or Step 3 before `XACK`, those messages remain in Redis's Pending Entries List (PEL). On startup, the archiver drains its PEL (`XREADGROUP` with ID `0` / `XAUTOCLAIM`) before reading new messages (`>`), guaranteeing **at-least-once persistence** with zero gaps.
3. **7-Day Outage Tolerance:** Because `evml:{<shard>}:events` retains 7 days of history in Redis ([`base.toml:139`](../../evm_listener/config/base.toml#L139)) and `evm_listener`'s [`ConsumerLag`](../../evm_listener/src/sink/consumer_lag.cpp) automatically tracks any registered consumer group via `XINFO GROUPS`, `evm_listener` will export Prometheus lag metrics (`evml_consumer_lag_entries{group="cg_parquet_archiver"}`) and alert operators if the archiver falls behind.

---

## 4. Quant Research Superpower: Joining `events` and `fast` in DuckDB

Because [`evml::pipeline::to_fields`](../../evm_listener/src/pipeline/event.cpp#L63-L97) stamps every event on both `evml:{<shard>}:fast` and `evml:{<shard>}:events` with the identical deterministic `event_id` (`"{chain_id}:{block_hash}:{log_index}"`) and millisecond timestamps (`observed_ms`, `emitted_ms`), quants can join the two streams directly over Parquet files without running any database server:

```sql
-- Execute via DuckDB CLI, Python duckdb.sql(), or Polars:
SELECT
    e.block_number,
    e.contract,
    e.event,
    e.event_id,
    f.observed_ms                                   AS ws_tip_observed_ms,
    e.observed_ms                                   AS safe_sweep_observed_ms,
    (e.observed_ms - f.observed_ms)                 AS safe_confirmation_lag_ms,
    (f.observed_ms - (e.block_timestamp * 1000))    AS ws_propagation_delay_ms,
    e.type                                          AS final_disposition -- 'log' or 'revert'
FROM read_parquet('/data/parquet/stream=events/chain_id=8453/**/*.parquet') e
LEFT JOIN read_parquet('/data/parquet/stream=fast/chain_id=8453/**/*.parquet') f
    USING (event_id)
WHERE e.dt >= '2026-11-01';
```

### What This Unlocks for Strategy Backtesting:
1. **Zero Look-Ahead Bias:** Backtests can trigger on `f.observed_ms` (the exact millisecond the WebSocket tip frame arrived at the host) rather than theoretical block timestamps (`block_timestamp * 1000`).
2. **Empirical Reorg & False-Signal Measurement:** Any entry present in `stream=fast` that has no matching `type = 'log'` in `stream=events` (or has a subsequent `type = 'revert'` in `stream=events`) represents a reorged tip log—allowing quants to measure exact reorg probabilities by pool and time-of-day.

---

## 5. How Tier 2 (ClickHouse) Plugs In Later With Zero Refactor

When the team scales to multiple simultaneous researchers and 24/7 Grafana operational dashboards:

1. **Instant Historical Backfill From Tier 1 Parquet:**
   ClickHouse natively parses Zstd Parquet files. You hydrate months of history into ClickHouse in seconds without touching Redis:
   ```sql
   INSERT INTO defi_warehouse.base_events
   SELECT * FROM file('/data/parquet/stream=events/chain_id=8453/**/*.parquet', Parquet);
   ```
2. **Independent Live Consumer Group (`cg_clickhouse_hot`):**
   Attach a second Redis Stream consumer group (`cg_clickhouse_hot`) that inserts 1-second micro-batches into a ClickHouse `ReplacingMergeTree(emitted_ms)` table ordered by `(chain_id, block_number, log_index, event_id, type)`.
3. **Tier 1 Remains the Immutable Cold Archive:**
   Tier 1 continues writing immutable `.parquet` files to NVMe/MinIO as the cryptographically reproducible backtesting dataset and disaster-recovery backup.
