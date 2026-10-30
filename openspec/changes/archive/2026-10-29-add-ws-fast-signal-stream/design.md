# Design

## Context

See `proposal.md` for motivation and `specs/fast-signal-stream/spec.md` for behavioral requirements.

Currently, `Host::on_ws_log` (`evm_listener/src/app/host.cpp:157-159`) forwards incoming WebSocket `rpc::Log` frames from the active WebSocket provider (`quicknode` primary) to `WsReconciler::on_ws_log` (`evm_listener/src/app/ws_reconciler.cpp:14-21`), which validates the `(address, topic0)` route and stages the log in `PendingBuffer`. No Redis write occurs until `Sweeper::safe_step` sweeps the block 20–60 seconds later.

At the same time, `RedisSink` (`evm_listener/src/sink/redis_sink.hpp`) runs a dedicated sink thread with two connections (`data_` for pipelined Lua script calls and `lease_` for lease renewals/heartbeats) and a disk spill WAL (`Spill`) for authoritative `safe`/`finalized` emissions.

## Goals / Non-Goals

**Goals:**
- Deliver routed WebSocket logs for `fast_stream = true` contracts to `evml:{<shard>}:fast` within ~50–150 ms of block production (zero extra HTTP RPC round-trips on the hot path).
- Preserve 100% of existing invariants (`I1`–`I6`, `P1`) on `evml:{<shard>}:events`, `evml:{<shard>}:ckpt`, `evml:{<shard>}:released`, and `--audit-stream`.
- Prevent Redis key/RAM bloat: zero `evml:{<shard>}:dedup:*` keys for fast emissions and bounded `MINID ~` stream retention (`fast_retention_ms = 3600000` ms = 1 hour default).
- Guarantee strict fault isolation so a slow/disconnected Redis never spills `fast` records to `/tmp/evml-spill` or delays authoritative `safe`/`finalized` writes.

**Non-Goals:**
- Emitting `type = "revert"` records on `evml:{<shard>}:fast` (the fast stream is a low-latency tip signal feed; authoritative reorg reconciliation and reverts remain on `evml:{<shard>}:events`).
- Auditing `evml:{<shard>}:fast` in `--audit-stream` (which audits finalized canonical completeness on `events` and `registry`).
- Supporting `finality = "latest"` as a contract's primary cold-ledger `finality` setting in TOML (`[[contracts]].finality` remains `safe | finalized | depth`; the hot plane is enabled orthogonally via `fast_stream = true`).

## Decisions

### Decision 1: Orthogonal `fast_stream = true` Contract Flag + Separate `evml:{<shard>}:fast` Key
- **Choice:** Add `bool fast_stream{false}` to `core::Contract` / `core::Route` (`evm_listener/src/core/config.hpp`) and `Keys::fast()` returning `evml:{<shard>}:fast` (`evm_listener/src/sink/keys.hpp`).
- **Rationale:** Keeping `fast_stream` orthogonal to `contract.finality` allows `eurc_usdc_cl1_pool` and `eurc_usdc_cl50_pool` to emit **both** an immediate ~80 ms hot signal to `evml:{<shard>}:fast` (`finality = "latest"`) **and** the authoritative ~22 s `safe` record to `evml:{<shard>}:events` (`finality = "safe"`).
- **Alternatives Considered:**
  - *Emitting tip logs directly into `evml:{<shard>}:events`:* Rejected because writing unconfirmed tip logs into `events` would set `evml:{<shard>}:dedup:<event_id>`, causing the subsequent `safe` sweep to be dropped as `DUP` without recording the event in `evml:{<shard>}:released` (breaking safe-window reorg detection and `--audit-stream`).

### Decision 2: Dedicated `emit_fast.lua` Script (Leader-Fenced, `XADD MINID ~`, Zero Dedup Keys)
- **Choice:** Add a 6-line embedded Lua script `evm_listener/src/sink/lua/emit_fast.lua` registered alongside `emit.lua`, `ckpt.lua`, `final_trim.lua`, and `lease.lua` in `evm_listener/CMakeLists.txt`:
  ```lua
  #!lua
  -- emit_fast.lua
  -- KEYS: 1 fast_stream  2 leader
  -- ARGV: 1 minid_ms  2 token  3.. field,value...
  if ARGV[2] ~= '0' and redis.call('GET', KEYS[2]) ~= ARGV[2] then return 'FENCED' end
  return redis.call('XADD', KEYS[1], 'MINID', '~', ARGV[1], '*', unpack(ARGV, 3))
  ```
- **Rationale:**
  - Uses the `{<shard>}` hash tag on both `KEYS[1]` (`evml:{<shard>}:fast`) and `KEYS[2]` (`evml:{<shard>}:leader`), maintaining single-slot Redis Cluster compatibility.
  - Enforces the exact same leader-lease fencing (`FENCED`) as `emit.lua` so a zombie/partitioned old leader can never inject stale tip signals after failover.
  - Performs zero `GET`/`SET` on `dedup:<event_id>` and zero `ZADD`/`HSET` on `released`/`released_data`, keeping Redis RAM footprint under ~1 MB for a 1-hour rolling window (`fast_retention_ms = 3600000`).

### Decision 3: Best-Effort, Non-Spilling `emit_fast` on `Sink` / `RedisSink`
- **Choice:** Add `virtual void emit_fast(pipeline::Event e) {}` to `pipeline::Sink` (`evm_listener/src/pipeline/sink.hpp`) and implement it in `RedisSink`:
  - At `RedisSink::emit_fast(Event e)` entry: if `mode_.load() != SinkMode::Connected` or `saturated()` is true, immediately increment `fast_dropped_total_` and return without locking or enqueuing.
  - Otherwise, enqueue a `detail::FastEmitItem{std::move(e)}` into `queue_`.
  - On the sink worker thread:
    - `FastEmitItem` is pipelined with `EmitItem` in `held_` when `mode_ == SinkMode::Connected` (Incrementing `fast_emitted_total_` on success).
    - If Redis disconnects (`go_degraded` / `queue_to_spill`), any `FastEmitItem` in `held_` or `queue_` is **discarded** (incrementing `fast_dropped_total_`) rather than written to `Spill`.
- **Rationale:** Reuses the existing `data_` connection and batching loop (avoiding a 3rd TCP connection per shard) while guaranteeing that `Spill` (`/tmp/evml-spill`) only ever stores authoritative `safe`/`finalized` items and that `FastEmitItem` never delays barriers (`CkptItem`, `FinalTrimItem`, `ReadItem`).
- **Alternatives Considered:**
  - *Opening a 3rd dedicated Redis TCP connection for `fast` emits:* Unnecessary overhead; when `RedisSink` is `Connected`, `data_` is idle >95% of the time (sweeps only run every 4s/12s) and dispatches queued items in `< 1 ms`.

### Decision 4: Zero-RPC Block Timestamp Lookup from In-Memory `ReorgDetector` Ring
- **Choice:** In `Host::on_ws_log`, construct the `pipeline::Event` using `make_event(..., /*block_timestamp=*/ts, ...)` where `ts` is looked up in O(1) from `rt_->ring` (`ReorgDetector`) if `newHeads` has already delivered the header for `log.block_number` with matching `log.block_hash` (falling back to `0` if the `logs` frame arrived ahead of `newHeads`), then override `ev.finality = core::Finality::Latest`.
- **Rationale:** Standard Ethereum/Base `eth_subscribe("logs")` notifications omit `blockTimestamp`. Making an HTTP `eth_getBlockByNumber` call on the hot path would add 70–90 ms of HTTP latency and consume RPC quota; checking the in-memory `ReorgDetector` ring takes nanoseconds and zero network I/O.

## Risks / Trade-offs

- **[Risk] Unconfirmed tip logs may be reverted by a rare L2 sequencer micro-reorg** → **Mitigation:** Every entry on `evml:{<shard>}:fast` carries `finality = "latest"`. Downstream consumers (Bot B) use `fast` strictly for price/volatility/drift detection and defensive LP withdrawals, while relying on `evml:{<shard>}:events` (`finality = "safe"` / `"finalized"`) for authoritative position accounting and re-entry.
- **[Risk] WebSocket burst during extreme market volatility could grow the in-memory sink queue** → **Mitigation:** `RedisSink::emit_fast` checks `mode() == SinkMode::Connected && !saturated()` before enqueuing and drops `FastEmitItem` if the sink degrades or saturates, protecting memory and authoritative `events` latency.
- **[Risk] Existing unit tests switch exhaustively on `core::Finality` (`Safe`, `Finalized`, `Depth`)** → **Mitigation:** Audit all `switch (finality)` sites across `evm_listener/src/` and `evm_listener/tests/` (`to_string(Finality)` returns `"latest"` for `Finality::Latest`) while keeping TOML `parse_finality` restricted to `safe | finalized | depth` so `Finality::Latest` is only produced by the runtime hot path.

## Migration Plan

1. Implement config, Lua script (`emit_fast.lua`), `Keys::fast()`, `RedisSink::emit_fast`, `Host::on_ws_log` fast emission, and Prometheus metrics (`evml_fast_emitted_total`, `evml_fast_dropped_total`).
2. Verify all unit and integration tests (`ctest` in Debug ASan/UBSan and Release) pass, including new unit tests for config parsing, `emit_fast.lua`, `RedisSink` fast-drop under `Degraded`/saturation, and `Host` end-to-end WebSocket fast emission.
3. Deploy to Railway `staging` (`staging` branch) and verify sub-150ms `Swap` entries in `evml:{base_eurc_usdc}:fast` alongside `evml:{base_eurc_usdc}:events`.
4. **Rollback:** Set `fast_stream = false` in `base.toml` (or revert commit); downstream consumers reading `evml:{base_eurc_usdc}:events` are completely unaffected.

