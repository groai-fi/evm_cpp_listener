# Proposal

## Why

On Base (`chain_id = 8453`, 2.0-second block time), the `safe` finality tag (`G_s`) lags the live chain tip by 10–30 blocks (~20–60 seconds) while waiting for L1 batch submission. During violent FX or crypto market shakes, waiting 20–60 seconds to observe `Swap` events exposes concentrated-liquidity LP positions (`eurc_usdc_cl1_pool`, `eurc_usdc_cl50_pool`) to severe adverse selection (Loss-Versus-Rebalancing, LVR), even though `evm_listener` already receives those `Swap` logs over WebSocket within ~50–150 ms of block production.

## What Changes

- Introduce an institutional **Dual-Plane ("Hot Signal / Cold Ledger")** architecture by adding a dedicated low-latency WebSocket tip stream (`evml:{<shard>}:fast`) alongside the existing authoritative `safe`/`finalized` settlement stream (`evml:{<shard>}:events`):
  - **Hot Signal Plane (`evml:{<shard>}:fast`, ~50–150 ms latency):** When the active leader receives a routed log over WebSocket (`Host::on_ws_log`) for a contract/event configured for the fast plane, it immediately enqueues a lightweight `fast` stream emission (`finality = "latest"`) trimmed with a short time-based `MINID ~` retention window (default `fast_retention_ms = 3600000` = 1 hour) and **zero `dedup:*` keys**.
  - **Cold Settlement Plane (`evml:{<shard>}:events`, `safe` ~22s / `finalized` ~21m):** Remains 100% unchanged—preserving all existing invariants (`I1`–`I6`), `emit.lua` deduplication, `PendingBuffer` cross-check, `released` reorg window, disk spill WAL, and `--audit-stream` exactness guarantees.
- Add optional per-contract or `[sink]` configuration in TOML (`fast_stream = true | false` on `[[contracts]]`, defaulting to `false`, and `fast_retention_ms = 3600000` in `[sink]`) and enable `fast_stream = true` on `eurc_usdc_cl1_pool` and `eurc_usdc_cl50_pool` in `evm_listener/config/base.toml`.
- Enforce strict **Hot/Cold Fault Isolation**: `fast` emissions are best-effort, memory-bounded, and never written to the disk spill WAL (`/tmp/evml-spill`); if the sink is degraded or saturated, `fast` emissions are dropped with a counter increment (`evml_fast_dropped_total`) so the hot path can never backpressure or stall the authoritative `safe`/`finalized` ledger.
- Expose Prometheus metrics (`evml_fast_emitted_total`, `evml_fast_dropped_total`) for institutional observability.

## Capabilities

### New Capabilities
- `fast-signal-stream`: Sub-150ms WebSocket tip emission pipeline (`evml:{<shard>}:fast`) for real-time price, tick, and volatility circuit-breaker signals, isolated from the authoritative `safe`/`finalized` settlement ledger.

### Modified Capabilities
None.

## Impact

- **Configuration (`evm_listener/src/core/config.hpp`, `config.cpp`, `evm_listener/config/base.toml`):** Adds optional `fast_stream` boolean on `[[contracts]]` (default `false`) and optional `fast_retention_ms` in `[sink]` (default `3600000` ms = 1 hour). Backward-compatible with all existing TOML configs.
- **Event & Key Schema (`evm_listener/src/core/config.hpp`, `evm_listener/src/pipeline/event.hpp`, `evm_listener/src/sink/keys.hpp`):** Adds `Finality::Latest` (`"latest"`) for hot-plane events and `Keys::fast()` returning `evml:{<shard>}:fast`.
- **Sink & Lua (`evm_listener/src/sink/redis_sink.hpp`, `redis_sink.cpp`, `redis_conn.hpp`, `redispp_conn.cpp`):** Adds non-spilling, lease-fenced `emit_fast` (`XADD evml:{<shard>}:fast MINID ~ <cutoff_ms> * ...`) executed on the data connection when `SinkMode::Connected` and dropped with telemetry when `Degraded`/`Replaying` or above soft queue capacity.
- **Host & Telemetry (`evm_listener/src/app/host.cpp`, `evm_listener/src/ops/metrics.hpp`, `metrics.cpp`):** Emits routed `fast_stream` logs from `Host::on_ws_log` while continuing to stage them in `WsReconciler` for `safe` cross-checking, and exports `evml_fast_emitted_total` and `evml_fast_dropped_total`.

