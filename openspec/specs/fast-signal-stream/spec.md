# fast-signal-stream Specification

## Purpose

Provides a sub-150ms WebSocket tip signal stream (`evml:{<shard>}:fast`) for real-time market data, volatility detection, and defensive LP circuit-breaker triggers, while keeping the authoritative `safe` and `finalized` settlement stream (`evml:{<shard>}:events`) completely isolated and invariant-protected.

## Requirements

### Requirement: Per-Contract Fast Signal Stream Opt-In Configuration
The system SHALL support an optional `fast_stream` boolean attribute on each `[[contracts]]` entry (defaulting to `false`) and an optional `fast_retention_ms` integer attribute in `[sink]` (defaulting to `3600000` milliseconds = 1 hour, minimum `1000` milliseconds). The Base EURC/USDC configuration (`base.toml`) SHALL enable `fast_stream = true` on `eurc_usdc_cl1_pool` and `eurc_usdc_cl50_pool` while leaving gauge, factory, and voter contracts at `fast_stream = false`.

#### Scenario: Contract omits fast_stream in TOML
- **WHEN** a `[[contracts]]` entry in the TOML configuration does not specify `fast_stream`
- **THEN** the configuration parser SHALL default `fast_stream` to `false` for that contract and SHALL NOT emit WebSocket logs for that contract to `evml:{<shard>}:fast`

#### Scenario: Contract enables fast_stream in TOML
- **WHEN** a `[[contracts]]` entry specifies `fast_stream = true`
- **THEN** the configuration parser SHALL mark all routed events of that contract as eligible for immediate emission to `evml:{<shard>}:fast` upon WebSocket arrival

#### Scenario: Invalid fast_retention_ms rejected at startup
- **WHEN** `[sink]` specifies `fast_retention_ms = 0` or a negative value
- **THEN** configuration parsing SHALL fail fast with a descriptive configuration error before opening network connections

### Requirement: Immediate WebSocket Tip Emission to Fast Stream
When the active leader instance receives a non-removed WebSocket log (`removed == false`) matching a configured contract and event topic where `fast_stream = true`, the system SHALL immediately emit a schema `v=1` event entry to the Redis stream `evml:{<shard>}:fast` with `finality = "latest"`, without waiting for the `safe` or `finalized` gate poll. The same WebSocket log SHALL still be staged for subsequent `safe` or `finalized` sweep cross-checking.

#### Scenario: Live WebSocket Swap log arrives on fast-enabled pool
- **WHEN** the active leader receives a valid `Swap` log over WebSocket for `eurc_usdc_cl1_pool` (`fast_stream = true`)
- **THEN** the system SHALL write an entry to `evml:{<shard>}:fast` containing schema `v=1` fields with `type = "log"`, `finality = "latest"`, `block_timestamp = "0"`, and `event_id = "<chain_id>:<block_hash>:<log_index>"`, AND the log SHALL also be staged in the pending buffer for the `safe` sweep cross-check

#### Scenario: Live WebSocket log arrives on non-fast contract
- **WHEN** the active leader receives a WebSocket log for a contract with `fast_stream = false` (such as `eurc_usdc_cl50_gauge` or `aero_voter`)
- **THEN** the system SHALL stage the log for the authoritative `safe`/`finalized` cross-check and SHALL NOT write an entry to `evml:{<shard>}:fast`

#### Scenario: Standby instance does not emit to fast stream
- **WHEN** an instance is in `standby` role or is demoting after lease loss
- **THEN** the instance SHALL NOT emit any entries to `evml:{<shard>}:fast`

### Requirement: Zero-Key-Flood Retention and Leader Fencing on Fast Stream
Emissions to `evml:{<shard>}:fast` SHALL be fenced on the current leader lease token (when leasing is enabled), SHALL trim stream entries older than `now_ms - fast_retention_ms` via `XADD MINID ~`, and SHALL NOT create per-event `evml:{<shard>}:dedup:*`, `evml:{<shard>}:released`, or `evml:{<shard>}:released_data` keys.

#### Scenario: Fast stream write does not create dedup or released keys
- **WHEN** an event is emitted to `evml:{<shard>}:fast`
- **THEN** Redis SHALL contain the new stream entry in `evml:{<shard>}:fast` trimmed by `MINID ~`, and no `evml:{<shard>}:dedup:<event_id>` key SHALL be created by the fast emission

#### Scenario: Fenced leader fast emission is rejected
- **WHEN** a superseded leader attempts to write a fast emission to `evml:{<shard>}:fast` after another host has acquired a newer lease epoch
- **THEN** Redis SHALL reject the write with `FENCED` and the entry SHALL NOT be appended to `evml:{<shard>}:fast`

### Requirement: Hot/Cold Fault Isolation and Non-Spilling Best-Effort Delivery
The `evml:{<shard>}:fast` stream SHALL operate as a best-effort hot signal channel that never backpressures, blocks, or corrupts the authoritative `evml:{<shard>}:events` settlement pipeline or the local disk spill WAL.

#### Scenario: Redis is unreachable or sink is spilling/replaying
- **WHEN** a `fast_stream` WebSocket log arrives while the Redis sink is in `Degraded`, `Replaying`, `Fenced`, or `Failed` mode, or when the sink queue is saturated
- **THEN** the system SHALL drop the fast emission without writing it to the disk spill WAL (`/tmp/evml-spill`), SHALL increment the dropped fast-emission counter, and SHALL preserve full disk spill capacity for authoritative `safe`/`finalized` records

#### Scenario: Authoritative safe sweep processes a block previously emitted on fast stream
- **WHEN** the `safe` gate advances over a block whose `Swap` log was earlier emitted to `evml:{<shard>}:fast`
- **THEN** the authoritative sweep SHALL still emit the verified `safe` event to `evml:{<shard>}:events` because the fast emission did not set `evml:{<shard>}:dedup:<event_id>`

### Requirement: Fast Stream Prometheus Telemetry
The system SHALL expose Prometheus counter metrics tracking total fast-stream events emitted (`evml_fast_emitted_total`) and total fast-stream events dropped due to sink backpressure, outage, or fencing (`evml_fast_dropped_total`).

#### Scenario: Prometheus metrics scrape after fast emission and drop
- **WHEN** an operator scrapes `/metrics` after 10 fast events have been written to `evml:{<shard>}:fast` and 2 fast events were dropped during a temporary Redis disconnect
- **THEN** `/metrics` SHALL report `evml_fast_emitted_total 10` and `evml_fast_dropped_total 2`

