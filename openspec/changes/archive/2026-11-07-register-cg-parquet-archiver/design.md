# Design

## Context

`evm_listener` is the primary on-chain event publisher for the Base EURC/USDC pipeline, streaming canonical logs to Redis `evml:{base_eurc_usdc}:events`. Downstream services rely on Redis consumer groups to process events:
1. `strat:aero_eurcusdc:botB_v1`: Low-latency automated trading bot.
2. `cg_parquet_archiver`: Analytical archiver writing raw events to Parquet tables in object storage.

Because `[sink]` enforces a 48-hour rolling retention (`retention_ms = 172800000`), slow or stalled consumers risk data loss if unread messages are trimmed by `XADD ... MINID ~`. `evm_listener` periodically probes `XINFO GROUPS` on configured consumer groups. If a group's lag exceeds 12 hours (`consumer_lag_warn_s = 43200`), a Warning is posted; if lag exceeds 24 hours (`consumer_lag_crit_s = 86400`), a Critical alert fires.

## Goals / Non-Goals

**Goals:**
- Register `cg_parquet_archiver` on `stream = "events"` in `evm_listener/config/base.toml`.
- Synchronize `evm_listener/tests/config_test.cpp` assertions and `mutate()` helpers in lock-step.
- Protect production from false alerts by ensuring `fast` and `registry` streams are never registered.
- Maintain 100% test pass rate with clean exit code 0.

**Non-Goals:**
- Creating consumer groups in Redis from C++ (consumer groups are initialized by downstream worker processes).
- Changing C++ configuration parser semantics (existing parser already supports multiple consumer groups).
- Monitoring consumer lag on the WebSocket `fast` signal stream or `registry` stream.

## Decisions

### Decision 1: Register `cg_parquet_archiver` strictly on `events` stream
- **Choice**: Add `{ stream = "events", group = "cg_parquet_archiver" }` to `[sink].consumer_groups`.
- **Rationale**:
  1. `stream = "fast"` is rejected by `config.cpp:611` (`must be events|registry, got "fast"`).
  2. `stream = "registry"` is populated only upon on-chain factory/voter events. Because the stream starts at `"gate"`, the stream key does not exist initially in Redis. If registered, `consumer_lag.cpp` reports `missing = true`, and `alerts.cpp:266` triggers a false `Consumer Group Missing` warning after 60 seconds (`consumer_group_missing_s`).
  3. The Parquet archiver archives settlement events from `events`.

### Decision 2: Multi-line TOML Array Formatting
- **Choice**:
  ```toml
  consumer_groups     = [
    { stream = "events", group = "strat:aero_eurcusdc:botB_v1" },
    { stream = "events", group = "cg_parquet_archiver" },
  ]
  ```
- **Rationale**: Clear formatting that produces concise git diffs when downstream groups are added or removed.

### Decision 3: Exact `mutate()` Pattern Alignment in `config_test.cpp`
- **Choice**:
  - Update `TEST_CASE("base.toml: the downstream consumer group is watched...")`:
    ```cpp
    REQUIRE(c.sink.consumer_groups.size() == 2);
    CHECK(c.sink.consumer_groups[0].stream == StreamKind::Events);
    CHECK(c.sink.consumer_groups[0].group == "strat:aero_eurcusdc:botB_v1");
    CHECK(c.sink.consumer_groups[1].stream == StreamKind::Events);
    CHECK(c.sink.consumer_groups[1].group == "cg_parquet_archiver");
    ```
  - Replace the removal mutation pattern in line 98 with the exact multiline block.
  - In `SECTION("consumer group: unknown stream, empty name, duplicate, wrong shape")`:
    - Update `mutate(base, "[" + good + "]", "\"g\"")` at line 352 to replace the multiline array literal.
    - Update line 357 from `CHECK(c.sink.consumer_groups.size() == 2);` to `CHECK(c.sink.consumer_groups.size() == 3);` to reflect appending a third group onto the 2 base groups.
- **Rationale**: `mutate()` asserts `REQUIRE(pos != std::string::npos)`. Exact string alignment prevents test harness panics.

## Risks / Trade-offs

- **[Risk] Test failure if multiline spacing in `base.toml` differs from test strings** → **Mitigation**: Define a shared constant or exact string literal in `config_test.cpp` matching `base.toml` byte-for-byte.
- **[Risk] Alert raised if archiver group does not exist in Redis on deployment** → **Mitigation**: Operational deployment plan must ensure `cg_parquet_archiver` creates its Redis consumer group before or concurrently with `evm_listener` restart.
