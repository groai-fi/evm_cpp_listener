# Spec Delta

## Purpose

Provides configuration and health tracking for downstream Redis stream consumer groups on the authoritative `events` stream, enabling proactive lag alerting before Redis `MINID` stream retention trims unread messages.

## ADDED Requirements

### Requirement: Consumer Group Registration for Parquet Archiver
The production configuration (`evm_listener/config/base.toml`) SHALL register `cg_parquet_archiver` under `[sink].consumer_groups` for `stream = "events"` alongside `strat:aero_eurcusdc:botB_v1`. It SHALL NOT register consumer groups for `stream = "fast"` or `stream = "registry"`.

#### Scenario: Parsed base.toml contains both downstream consumer groups
- **WHEN** `evm_listener/config/base.toml` is parsed by the configuration engine
- **THEN** `c.sink.consumer_groups` SHALL contain exactly 2 entries, both with `stream == StreamKind::Events`, corresponding to `"strat:aero_eurcusdc:botB_v1"` and `"cg_parquet_archiver"`.

#### Scenario: Invalid stream name in consumer group rejected at startup
- **WHEN** a consumer group specifies a stream other than `"events"` or `"registry"` (such as `"fast"`)
- **THEN** the configuration parser SHALL throw a `ConfigError` with `must be events|registry`.

#### Scenario: Omitting consumer_groups disables consumer group lag probes
- **WHEN** the `consumer_groups` key is omitted or removed from the configuration
- **THEN** `c.sink.consumer_groups` SHALL be empty and no consumer group lag probes SHALL be performed.
