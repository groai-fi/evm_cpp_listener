# Proposal

## Why

In `evm_listener`, the Redis sink enforces time-based stream trimming via `XADD ... MINID ~ now - retention_ms` (`retention_ms = 172800000` = 48 hours in `base.toml`). Redis trims stream entries past the retention horizon regardless of whether downstream consumers have consumed them.

To protect downstream consumers against silent data loss, `evm_listener` monitors consumer lag (`consumer_lag.cpp`) and emits operational alerts (`alerts.cpp`) if a registered consumer group stalls or falls behind (`consumer_lag_warn_s = 43200` = 12h, `consumer_lag_crit_s = 86400` = 24h).

Currently, `evm_listener/config/base.toml` only tracks the trading bot group `strat:aero_eurcusdc:botB_v1`. Phase 0 introduces `cg_parquet_archiver`, the historical analytics and cold-storage archiver that ingests canonical events from Redis streams into Parquet datasets. Registering `cg_parquet_archiver` in `base.toml` under `[sink].consumer_groups` is required so the listener monitors its consumption lag and alerts operators well before stream retention trimming occurs.

## What Changes

1. **Production Configuration (`evm_listener/config/base.toml`)**:
   - Update `[sink].consumer_groups` to register `cg_parquet_archiver` on `stream = "events"` alongside `strat:aero_eurcusdc:botB_v1`:
     ```toml
     consumer_groups     = [
       { stream = "events", group = "strat:aero_eurcusdc:botB_v1" },
       { stream = "events", group = "cg_parquet_archiver" },
     ]
     ```
   - Maintain strict stream isolation: do NOT register consumer groups for `stream = "fast"` (rejected at configure time with `ConfigError`) or `stream = "registry"` (triggers false alerts because `registry` stream does not exist until on-chain events occur).

2. **Unit Test Synchronization (`evm_listener/tests/config_test.cpp`)**:
   - In `TEST_CASE("base.toml: the downstream consumer group is watched...")`, assert `c.sink.consumer_groups.size() == 2` and verify both `"strat:aero_eurcusdc:botB_v1"` and `"cg_parquet_archiver"` belong to `StreamKind::Events`.
   - Update the `mutate()` pattern removing `consumer_groups` to match the exact multiline array text in `base.toml`.
   - In `SECTION("consumer group: unknown stream, empty name, duplicate, wrong shape")`, update the array mutation string to match the multiline format and update the expected size assertion from 2 to 3.

## Capabilities

### New Capabilities
- `sink-consumer-groups`: Configuration and monitoring of downstream Redis stream consumer groups on authoritative settlement streams to detect stalls and prevent retention trimming data loss.

### Modified Capabilities
- None.

## Impact

- **Configuration**: `evm_listener/config/base.toml`
- **Tests**: `evm_listener/tests/config_test.cpp`
- **Ops/Monitoring**: Active `XINFO GROUPS` polling and lag alert monitoring for `cg_parquet_archiver` in `consumer_lag.cpp` and `alerts.cpp`.
- **Zero Binary Changes**: No C++ logic changes required in `evml_core` or `evm_listener`.
