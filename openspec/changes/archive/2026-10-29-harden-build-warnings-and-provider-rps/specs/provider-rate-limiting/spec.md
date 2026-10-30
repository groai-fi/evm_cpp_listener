# Spec Delta

## Purpose

Controls per-provider HTTP RPC request pacing via token-bucket rate limiters (`reconcile_rps` and `reconcile_burst`), allowing narrow-range RPC providers with higher RPS quotas to catch up faster without over-driving conservative providers.

## ADDED Requirements

### Requirement: Per-Provider Reconcile Rate Limit Overrides
The system SHALL support optional `reconcile_rps` and `reconcile_burst` integer attributes (`>= 1`, with `reconcile_burst >= reconcile_rps`) on each `[[providers]]` entry in TOML configuration, overriding the `[redundancy]` defaults for that specific provider's token bucket.

#### Scenario: Provider omits per-provider rate limit keys
- **WHEN** a `[[providers]]` entry does not specify `reconcile_rps` or `reconcile_burst`
- **THEN** the provider's token bucket SHALL use the global `[redundancy].reconcile_rps` and `[redundancy].reconcile_burst` values

#### Scenario: Provider specifies higher reconcile_rps and reconcile_burst
- **WHEN** a `[[providers]]` entry specifies `reconcile_rps = 10` and `reconcile_burst = 15` (such as `quicknode` in `base.toml`)
- **THEN** the provider's token bucket SHALL pace reconcile calls for that provider at 10 requests per second with a burst capacity of 15, while other providers continue using `[redundancy]` defaults

#### Scenario: Invalid per-provider reconcile_rps or reconcile_burst rejected at startup
- **WHEN** a `[[providers]]` entry specifies `reconcile_rps = 0` or `reconcile_burst < reconcile_rps`
- **THEN** configuration parsing SHALL fail fast with a descriptive configuration error before opening network connections

