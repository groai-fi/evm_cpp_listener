# Implementation Plan: Tier 1 Redis-to-Parquet Archiver (Apache Arrow + Embedded DuckDB)

> **Goal**: Build a production-grade, zero-open-port archiver service (`tools/parquet_archiver/`) that tails `evml:{<shard>}:events`, `evml:{<shard>}:fast`, and `evml:{<shard>}:registry` via Redis consumer groups (`XREADGROUP`), writes sorted Zstd-compressed `.parquet` files atomically (`write .tmp` $\rightarrow$ `fsync` $\rightarrow$ `rename` $\rightarrow$ `XACK`), and provides ready-to-query DuckDB views for quant backtesting.

---

## 1. Architectural Scope & Non-Goals

### In Scope (Tier 1)
- **Zero-Exfiltration In-Process Archiver**: Single daemon/worker using `redis-py` + `pyarrow` + `duckdb` (or C++ `redis++` + `libarrow`/`libduckdb`) with zero listening TCP ports.
- **Exact Schema `v=1` Mapping**: Captures all 23 fields emitted by [`evml::pipeline::to_fields`](../../evm_listener/src/pipeline/event.cpp#L63-L97) plus `redis_stream_id` and Hive partition column `dt` (`YYYY-MM-DD`).
- **Crash-Safe PEL Recovery**: Recovers unacknowledged messages from Redis's Pending Entries List (`XREADGROUP ... 0` / `XAUTOCLAIM`) on startup before tailing new messages (`>`).
- **Atomic File Visibility**: Stages Parquet output to `.tmp_<stream>_<first_id>_<last_id>.parquet`, calls `os.fsync()`, atomically renames to `part-<first_id>-<last_id>.parquet`, and only then issues `XACK`.
- **Daily Compaction & Dedup CLI**: Merges small intraday `part-*.parquet` files into a single sorted daily file per partition while deduplicating any crash-recovery replays by `(redis_stream_id, event_id, type)`.

### Out of Scope (Deferred to Tier 2)
- Deploying a multi-node ClickHouse server (documented as a drop-in Tier 2 upgrade in [`Two_Tier_Parquet_DuckDB_ClickHouse_Architecture.md`](Two_Tier_Parquet_DuckDB_ClickHouse_Architecture.md#5-how-tier-2-clickhouse-plugs-in-later-with-zero-refactor)).
- Modifying `evm_listener`'s C++ ingestion pipeline (`evm_listener` already emits all required fields and automatically monitors all Redis consumer groups via [`ConsumerLag`](../../evm_listener/src/sink/consumer_lag.cpp)).

---

## 2. Canonical Parquet Schema (Mapped 1-to-1 to `schema v=1`)

Every `.parquet` file in `stream={events|fast|registry}/chain_id={chain_id}/dt={YYYY-MM-DD}/` enforces the following strict physical schema (matching [`event.cpp:72-96`](../../evm_listener/src/pipeline/event.cpp#L72-L96)):

| Column Name | Parquet / DuckDB Type | Source in Redis Stream (`to_fields`) | Notes |
| :--- | :--- | :--- | :--- |
| `redis_stream_id` | `VARCHAR` | Redis entry ID (e.g. `"1762185600123-0"`) | Monotonic stream offset; used for replay dedup |
| `v` | `UTINYINT` | `fields["v"]` (`1`) | Schema version |
| `type` | `VARCHAR` | `fields["type"]` (`"log"` \| `"revert"`) | Dictionary-encoded |
| `finality` | `VARCHAR` | `fields["finality"]` (`"latest"` \| `"safe"` \| `"finalized"` \| `"depth"`) | Dictionary-encoded |
| `event_id` | `VARCHAR` | `fields["event_id"]` (`"{chain_id}:{block_hash}:{log_index}"`) | Primary join key across `fast` and `events` |
| `chain_id` | `UBIGINT` | `fields["chain_id"]` (`8453`) | Chain ID |
| `contract` | `VARCHAR` | `fields["contract"]` (`"eurc_usdc_cl1_pool"`, etc.) | Dictionary-encoded |
| `address` | `VARCHAR` | `fields["address"]` (`"0x..."` 20-byte lowercase hex) | Dictionary-encoded |
| `event` | `VARCHAR` | `fields["event"]` (`"Swap"`, `"Mint"`, `"Burn"`, etc.) | Dictionary-encoded |
| `signature` | `VARCHAR` | `fields["signature"]` | Canonical ABI event signature |
| `block_number` | `UBIGINT` | `fields["block_number"]` | Primary sort key (`ORDER BY block_number, log_index`) |
| `block_hash` | `VARCHAR` | `fields["block_hash"]` (`"0x..."` 32-byte lowercase hex) | Block hash for EIP-1898 pinning |
| `block_timestamp` | `UBIGINT` | `fields["block_timestamp"]` | Unix seconds (`0` on `fast` if WS log arrived before `newHeads`) |
| `tx_hash` | `VARCHAR` | `fields["tx_hash"]` (`"0x..."` 32-byte lowercase hex) | Transaction hash |
| `tx_index` | `UINTEGER` | `fields["tx_index"]` | Transaction index in block |
| `log_index` | `UINTEGER` | `fields["log_index"]` | Secondary sort key (`ORDER BY block_number, log_index`) |
| `topics` | `VARCHAR` | `fields["topics"]` (`["0x...","0x..."]`) | JSON array of 32-byte hex topic strings |
| `data` | `VARCHAR` | `fields["data"]` (`"0x..."`) | Full ABI-encoded hex payload (preserves exact 256-bit words) |
| `first_source` | `VARCHAR` | `fields["first_source"]` (`"quicknode"`, `"infura"`, etc.) | Provider attribution |
| `observed_ms` | `UBIGINT` | `fields["observed_ms"]` | Host wall-clock ms when RPC/WS frame arrived |
| `emitted_ms` | `UBIGINT` | `fields["emitted_ms"]` | Host wall-clock ms when handed to `RedisSink` |
| `host` | `VARCHAR` | `fields["host"]` | Active leader host ID |
| `shard` | `VARCHAR` | `fields["shard"]` (`"base_eurc_usdc"`) | Shard identifier |
| `listener_version`| `VARCHAR` | `fields["listener_version"]` | Binary version + git SHA |

---

## 3. Storage & Partition Layout

```text
${PARQUET_ARCHIVE_DIR}/
  ├── stream=events/
  │   └── chain_id=8453/
  │       └── dt=2026-11-04/
  │           ├── part-1762214400120-0_1762217999850-2.parquet
  │           └── ...
  ├── stream=fast/
  │   └── chain_id=8453/
  │       └── dt=2026-11-04/
  │           └── part-1762214400050-0_1762214999910-0.parquet
  └── stream=registry/
      └── chain_id=8453/
          └── dt=2026-11-04/
              └── part-1762214400000-0_1762280000000-0.parquet
```

- **Partitioning Rule**: Partitioned by `dt = UTC date of observed_ms` (`YYYY-MM-DD`). If a single in-memory batch spans midnight UTC, the flusher splits the batch by `dt` and writes one atomic file per `dt` folder before issuing a single `XACK` for the batch.
- **Compression & Row Groups**: `COMPRESSION ZSTD`, `ROW_GROUP_SIZE 100000`, sorted by `(block_number ASC, log_index ASC, observed_ms ASC)` so Parquet row-group min/max statistics enable fast block-range predicate pushdown.

---

## 4. Phased Implementation Plan & Verification Gates

### Phase 1: Project Scaffolding & Dynamic `.env` Configuration
**Files to create:**
- `tools/parquet_archiver/archiver.py` (core archiver module)
- `tools/parquet_archiver/schema.py` (strict schema validator & DuckDB table/Parquet writer)
- `tools/parquet_archiver/compact.py` (daily partition compaction & deduplication CLI)
- `tools/parquet_archiver/views.sql` (DuckDB analytical views for quant research)

**Configuration variables (loaded dynamically via `.env` walk-up, zero hardcoded secrets):**
- `REDIS_URL` (required; same Redis instance used by `evm_listener`)
- `EVML_SHARD_ID` (default `"base_eurc_usdc"`)
- `PARQUET_ARCHIVE_DIR` (default `"./data/parquet"`)
- `PARQUET_CONSUMER_GROUP` (default `"cg_parquet_archiver"`)
- `PARQUET_FLUSH_MAX_ROWS` (default `5000` for `events`/`fast`, `100` for `registry`)
- `PARQUET_FLUSH_INTERVAL_S` (default `60` seconds)

- **Verify Phase 1:** Run `python3 tools/parquet_archiver/archiver.py --check-config` and verify it resolves `.env` and validates target directories without hardcoded credentials.

---

### Phase 2: Atomic DuckDB Writer (`schema.py`)
**Implementation Details:**
1. Accept a list of `(redis_stream_id, fields_dict)` entries fetched from Redis.
2. Validate required `v = "1"` fields and fail fast with explicit context if a corrupt record is encountered.
3. Group entries by `(stream_kind, chain_id, dt_utc)`.
4. For each partition:
   - Load rows into an in-memory DuckDB connection (`duckdb.connect(":memory:")`).
   - Execute:
     ```sql
     COPY (
         SELECT * FROM batch_table
         ORDER BY block_number ASC, log_index ASC, observed_ms ASC
     ) TO '/path/to/.tmp_part-....parquet'
     (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
     ```
   - Call `os.fsync()` on the temporary file, atomically `os.replace(tmp_path, final_path)`, and `os.fsync()` the parent directory.

- **Verify Phase 2:** Unit test feeding 1,000 synthetic `Swap` and `revert` events across a UTC midnight boundary; assert 2 `.parquet` files are created in consecutive `dt=` folders, zero `.tmp_*` files remain, and `duckdb.sql("SELECT count(*) FROM read_parquet(...)")` returns `1000` with exact column types.

---

### Phase 3: Redis Stream Consumer Group Loop & PEL Recovery (`archiver.py`)
**Implementation Details:**
1. On startup, ensure `cg_parquet_archiver` exists on `evml:{<shard>}:events`, `evml:{<shard>}:fast`, and `evml:{<shard>}:registry` (`XGROUP CREATE <key> cg_parquet_archiver 0 MKSTREAM`, ignoring `BUSYGROUP`).
2. **Startup PEL Recovery (`drain_pending`)**:
   - Before reading new messages (`>`), read unacknowledged messages for this consumer via `XREADGROUP GROUP cg_parquet_archiver <consumer> STREAMS ... 0` and claim idle orphaned PEL entries via `XAUTOCLAIM` (`min-idle-time = 60000 ms`).
   - Flush recovered PEL entries to Parquet and `XACK` them.
3. **Live Tail Loop**:
   - Poll `XREADGROUP GROUP cg_parquet_archiver <consumer> COUNT 1000 BLOCK 2000 STREAMS ... >`.
   - Flush each stream's buffer when `len(buf) >= PARQUET_FLUSH_MAX_ROWS` or `now - last_flush >= PARQUET_FLUSH_INTERVAL_S`.
   - Handle `SIGINT` / `SIGTERM` gracefully by flushing any buffered in-memory records to Parquet and calling `XACK` before exiting.

- **Verify Phase 3:**
  - Confirm `evm_listener`'s `/metrics` endpoint automatically reports `evml_consumer_lag_entries{group="cg_parquet_archiver"} == 0` after a flush.

---

### Phase 4: Daily Compaction & Quant SQL Views (`compact.py` & `views.sql`)
**Implementation Details:**
1. `compact.py`: For any completed UTC date `dt < today_utc` with $> 1$ `part-*.parquet` file:
   - Read all `part-*.parquet` files in that partition, deduplicate by `(redis_stream_id, event_id, type)` (handling any crash-retry duplicates), sort by `(block_number, log_index, observed_ms)`, write to `.tmp_compacted.parquet`, atomically move to `compacted-<dt>.parquet`, and remove the old `part-*.parquet` files.
2. `views.sql`: Provide 3 standard DuckDB views:
   - `v_canonical_events`: Returns net canonical logs after subtracting any log that has a matching `type = 'revert'` with the same `event_id`.
   - `v_fast_vs_safe_latency`: Joins `stream=fast` and `stream=events` on `event_id` to compute WebSocket arrival lead time (`e.observed_ms - f.observed_ms`) and reorg survival rate.
   - `v_eurc_usdc_swaps`: Decodes Slipstream `Swap(address,address,int256,int256,uint160,uint128,int24)` `data` payload into `amount0`, `amount1`, `sqrtPriceX96`, `liquidity`, and `tick`.

- **Verify Phase 4:** Run `compact.py` on a partition with duplicate injected `redis_stream_id`s and verify row count deduplicates cleanly and `v_canonical_events` excludes reverted logs.

---

### Phase 5: Integration & Crash-Recovery Test Suite (`tests/test_parquet_archiver.py`)
**Test Cases:**
1. **End-to-End Flush & Type Fidelity Test:** Publish real Base `Swap` + `revert` events to Redis, run one archiver cycle, and assert all 24 Parquet columns match the input values and `XPENDING` is `0`.
2. **Simulated Crash Before `XACK` + Compaction Dedup:** Simulate a crash after `os.replace()` but before `XACK`; restart the archiver, verify PEL recovery re-flushes and `XACK`s the batch, then run `compact.py` and verify exact deduplicated row count.
3. **Concurrent Reader Safety Test:** While flushing 20 batches, continuously query `read_parquet('.../part-*.parquet')` in a separate thread and assert `0` read errors (proving `.tmp_*` atomic rename isolation).
