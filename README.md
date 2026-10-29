# EVM C++ Listener (`evm_listener`)

## 1. What It Is

`evm_listener` is a 24/7, configuration-driven C++20 EVM event ingestion service that streams verified raw contract logs from **Base mainnet (`chainId = 8453`)** into **Redis Streams** for shard `base_eurc_usdc` (Aerodrome Slipstream EURC/USDC: the `ts=1` and `ts=50` concentrated-liquidity pools, the live `ts=50` gauge, `CLFactory`, and `Voter`), performing cross-provider canonical block-hash verification, safe-reorg detection with full before-image retractions, epoch-fenced active-passive leader election, and zero transaction signing.

---

## 2. Quick Start

### Build & Test (Devcontainer / Docker Compose)

```bash
# Start devcontainer services (dev, redis:8.4, toxiproxy)
make up

# Build Debug binary and tests (ASan + UBSan enabled)
make build

# Run the full unit + integration test suite
make test

# Build and audit the production Docker image (docker/Dockerfile.prod)
make prod-image
make prod-audit
```

### Run Locally

Copy [`evm_listener/.env.example`](evm_listener/.env.example) to `evm_listener/.env` and populate your RPC and Redis environment variables (never commit real secrets).

```bash
# Run a single listener instance against evm_listener/config/base.toml
./build/Debug/evm_listener/evm_listener --config evm_listener/config/base.toml

# Run active-passive pair (listener-a + listener-b) in local Docker Compose
docker compose -f .devcontainer/docker-compose.yml --profile listeners up -d
```

### Operator CLI Subcommands

```bash
# Offline config summary + live RPC chainId & contract bytecode check
evm_listener --config evm_listener/config/base.toml --check-config

# Verify Redis connectivity, version (>= 7.0), address family, maxmemory-policy, and AOF
evm_listener --config evm_listener/config/base.toml --check-redis

# Binary-search exact deployment block of a contract via eth_getCode
evm_listener --config evm_listener/config/base.toml --find-deploy-block 0xc5E51044eB7318950B1aFb044FccFb25782C48c1

# Live transport probe over WebSocket + HTTP failover chains for N seconds
evm_listener --config evm_listener/config/base.toml --probe-transport 15

# Backfill a newly added contract up to the stored checkpoint (fenced by shard lease)
evm_listener --config evm_listener/config/base.toml --backfill-contract eurc_usdc_cl50_pool

# Audit Redis streams against an independent eth_getLogs re-read over [from_block, to_block]
evm_listener --config evm_listener/config/base.toml --audit-stream 52060000 52060500
```

---

## 3. Configuration

Primary configuration lives in [`evm_listener/config/base.toml`](evm_listener/config/base.toml) and references **environment variable names only** (never raw URLs with keys or tokens). Environment variables are documented in [`evm_listener/.env.example`](evm_listener/.env.example) and resolved at startup by walking parent directories for `.env`, `.env.local.dev`, and `.env.local` (without overwriting existing process environment variables).

### `base.toml` Sections
| Section | Purpose |
| :--- | :--- |
| `[shard]` | Shard namespace `id = "base_eurc_usdc"` (`evml:{base_eurc_usdc}:*`) and `host_id` (`$EVML_HOST_ID` or hostname). |
| `[chain]` | `name = "base"`, `chain_id = 8453`, `block_time_ms = 2000`, `default_finality = "finalized"`, `hash_ring_size = 1024`, `start_policy = "contract_start"`, `safe_poll_ms = 4000`, `finalized_poll_ms = 12000`, `pending_max_logs = 200000`. |
| `[[providers]]` | Per-provider `ws_env`, `http_env` (or credential-free `http_url`), and measured `max_getlogs_range` (`quicknode = 5`, `infura = 500`, `alchemy = 2000`, `ankr = 10000`, `base_public = 500`). |
| `[redundancy]` | `mode = "failover"`, `ws_order = ["quicknode", "infura", "alchemy"]`, `getlogs_order = ["infura", "alchemy", "ankr", "base_public", "quicknode"]`, `failover_after = 3`, `stale_after_blocks = 5`, `prefer_primary_after_s = 600`, `quota_cooldown_s = 3600`, `reconcile_rps = 2`, `reconcile_burst = 4`. |
| `[lease]` | Active-passive Redis lease (`enabled = true`, `ttl_ms = 15000`, `renew_ms = 5000`). |
| `[[contracts]]` | Tracked contracts, `start_block`, `finality` (`"safe"` for pools, `"finalized"` for gauge/registry), target `stream` (`"events"` or `"registry"`), and canonical ABI event signatures. |
| `[backfill]` | AIMD minimum page size (`min_range = 10`) and `request_timeout_ms = 15000`. |
| `[watchdog]` | Stall thresholds (`head_stall_ms = 15000`, `subscribe_timeout_ms = 10000`, `progress_stall_ms = 120000`, `safe_gate_stuck_s = 600`, `gate_lag_alert_s = 3600`, `process_stall_ms = 30000`). |
| `[sink]` | `redis_url_env = "REDIS_URL"`, `retention_ms = 172800000` (48 h), `dedup_ttl_s = 604800` (7 d), `queue_capacity = 100000`, `spill_dir = "/tmp/evml-spill"`, `spill_max_bytes = 536870912` (512 MiB), `require_noeviction = true`, `consumer_groups`, and lag thresholds (`consumer_lag_warn_s = 43200`, `consumer_lag_crit_s = 86400`). |
| `[ops]` | `http_listen = "[::]:9464"`, `heartbeat_s = 10`, `alert_webhook_env`, `alert_webhook_token_env`, `alert_service = "evm-listener"`, `deadman_ping_env`, `deadman_ping_s = 60`, `http_user_agent = "evm-listener/{version}"`. |

### Environment Variables & Provider-Enable Rule
- **Provider-enable rule:** A `[[providers]]` entry is enabled in a chain (`ws_order` or `getlogs_order`) **if and only if** its corresponding environment variable (`ws_env` for WebSocket, `http_env` for HTTP `getLogs` — or literal `http_url` for `base_public`) is set and non-empty.
- **Standard environment variable names** (from [`evm_listener/.env.example`](evm_listener/.env.example)):
  - Base RPC/WSS: `BASE_RPC_URL_QUICKNODE`, `BASE_WSS_URL_QUICKNODE`, `BASE_RPC_URL`, `BASE_WSS_URL`, `BASE_RPC_URL_ALCHEMY`, `BASE_WSS_URL_ALCHEMY`, `BASE_RPC_URL_ANKR`
  - Sink & Ops: `REDIS_URL`, `EVML_HOST_ID`, `N8N_ALERT_WEBHOOK_URL`, `N8N_ALERT_WEBHOOK_TOKEN`, `DEADMAN_PING_URL`
  - Test harness only: `TOXIPROXY_URL`

---

## 4. Operations

See the [**Operations Runbook (`doc/runbook.md`)**](doc/runbook.md) for:
- Triage and remediation steps for all 25 alert conditions and events.
- Standard procedures for Railway deployment, rollback, forced leader takeover, partial checkpoint recovery, adding/backfilling a contract (`--backfill-contract`), Redis data-loss re-backfill, `Consumer Entries Trimmed` recovery, provider key rotation, and `n8n` / `healthchecks.io` setup.

Operational HTTP endpoints are served on `[ops].http_listen` (default `[::]:9464`, overridden by `$PORT` on Railway):
- `GET /healthz`: Liveness probe (`200 ok` iff internal `io_tick` and `sink_tick` heartbeats are fresh within `process_stall_ms`; `503` otherwise).
- `GET /readyz`: Readiness & status JSON (`200` for a live, non-saturated leader with a fresh `safe` gate or a healthy standby connected to Redis; `503` otherwise).
- `GET /metrics`: Prometheus text-format metrics (`evml_*`).

---

## 5. Consumer Contract

Downstream consumers (such as Bot B reading `strat:aero_eurcusdc:botB_v1`) must integrate against the guarantees and schema below.

### 5.1 Streams & Schema (`v = 1`)
Events are written as flat string key-value pairs to two Redis Streams sharing the `{base_eurc_usdc}` hash tag:
- `evml:{base_eurc_usdc}:events` — Pool (`Swap`, `Mint`, `Burn`, `Collect`) and Gauge (`Deposit`, `Withdraw`, `ClaimRewards`, `NotifyReward`, `ClaimFees`) events.
- `evml:{base_eurc_usdc}:registry` — Factory (`PoolCreated`) and Voter (`GaugeCreated`, `GaugeKilled`, `GaugeRevived`) events.

Every stream entry contains **23 string fields**:

| Field | Example Value | Description |
| :--- | :--- | :--- |
| `v` | `"1"` | Schema version (`1`). |
| `type` | `"log"` \| `"revert"` | `"log"` for a canonical event; `"revert"` to retract an earlier `"safe"` log. |
| `finality` | `"safe"` \| `"finalized"` | Finality class at emission time. |
| `event_id` | `"8453:0x9b1f...c204:3"` | Deterministic unique ID: `{chain_id}:{block_hash}:{log_index}`. |
| `chain_id` | `"8453"` | EVM chain ID (Base mainnet = `8453`). |
| `contract` | `"eurc_usdc_cl50_pool"` | Configured contract name from `base.toml`. |
| `address` | `"0xe846373c1a92b167b4e9cd5d8e4d6b1db9e90ec7"` | Lowercase `0x`-prefixed 20-byte contract address. |
| `event` | `"Swap"` | Event name (prefix of `signature` before `'('`). |
| `signature` | `"Swap(address,address,int256,int256,uint160,uint128,int24)"` | Canonical ABI event signature. |
| `block_number` | `"52060100"` | Decimal block number. |
| `block_hash` | `"0x9b1f...c204"` | Lowercase `0x`-prefixed 32-byte block hash. |
| `block_timestamp` | `"1727890000"` | Unix epoch timestamp in seconds (decimal) from the canonical block header. |
| `tx_hash` | `"0x4a7e...8810"` | Lowercase `0x`-prefixed 32-byte transaction hash. |
| `tx_index` | `"12"` | Decimal transaction index within the block. |
| `log_index` | `"3"` | Decimal log index within the block. |
| `topics` | `"[\"0xc42079f9...\",\"0x0000...\"]"` | JSON array of lowercase `0x`-prefixed 32-byte topic hashes (`topics[0]` is `keccak256(signature)`). |
| `data` | `"0x00000000..."` | Lowercase `0x`-prefixed hex ABI data payload (`"0x"` if empty). |
| `first_source` | `"infura"` | Name of the RPC provider that supplied the page. |
| `observed_ms` | `"1727890026120"` | Listener wall-clock timestamp (ms since Unix epoch) when the log was validated. |
| `emitted_ms` | `"1727890026120"` | Listener wall-clock timestamp (ms since Unix epoch) when the entry was built (refreshed on `revert`). |
| `host` | `"a"` | Emitting listener host ID (`EVML_HOST_ID`). |
| `shard` | `"base_eurc_usdc"` | Shard identifier. |
| `listener_version` | `"evm_listener 0.1.1 (d6fab71)"` | Listener build version string. |

### 5.2 The 3 Finality States a Consumer Sees
1. **`type=log, finality=safe`**:
   - Emitted for safe-class contracts (`eurc_usdc_cl1_pool` and `eurc_usdc_cl50_pool`) as soon as the block reaches the provider's `safe` tag (~20–30 s after block production on Base).
   - **No second `"finalized"` entry is emitted** when a `safe` log later crosses the `finalized` tag.
   - In the rare event of a safe-tag reorganization, a `type=revert` entry is emitted for the retracted `event_id`.
2. **`type=log, finality=finalized`**:
   - Permanent and never reverted.
   - Emitted for finalized-class contracts (`eurc_usdc_cl50_gauge`, `aero_cl_factory`, `aero_voter`) and for any safe-class log that was missed by the safe sweep and discovered late by the final sweep (or backfilled below `ckpt.finalized`).
3. **`type=revert`**:
   - Retracts an earlier `type=log, finality=safe` entry that vanished in a safe reorganization.
   - Carries the **full before-image** (identical `event_id`, `contract`, `address`, `event`, `signature`, `block_number`, `block_hash`, `block_timestamp`, `tx_hash`, `tx_index`, `log_index`, `topics`, and `data` as the original log, with `type="revert"` and a fresh `emitted_ms`), allowing stateless contra-entry accounting without looking up the prior log.
4. **Finality Watermark (`ckpt.finalized`)**:
   - Every event with `block_number ≤ ckpt.finalized` (stored in the Redis hash `evml:{base_eurc_usdc}:ckpt`, field `finalized`) is permanently finalized.
   - **Concrete example:** A `Swap` at block `52060100` (`log_index = 3`, block hash `0xaaa...`) is emitted with `type="log"`, `finality="safe"`, `event_id="8453:0xaaa...:3"`. If a safe reorg replaces block `52060100` with `0xbbb...`, the listener emits a `type="revert"` entry with `event_id="8453:0xaaa...:3"` (retracting the orphaned `Swap`) followed by a `type="log"`, `finality="safe"` entry with `event_id="8453:0xbbb...:3"` from the new canonical fork. Once `HGET evml:{base_eurc_usdc}:ckpt finalized` reaches `≥ 52060100`, block `52060100` will never change again.

### 5.3 Identity, Idempotency & Ordering (`event_id`)
- **`event_id = "{chain_id}:{block_hash}:{log_index}"`**: Uniquely identifies a log on a specific block fork. A reorg produces a different `block_hash` and therefore a distinct `event_id`.
- **Consumer ordering & idempotency requirement:** While normal-path sweeps emit logs in increasing `(block_number, log_index)` order per class, corrections (`type=revert`, late safe logs emitted at `finalized`, and `--backfill-contract` runs) and cross-host failovers can arrive out of block order. Consumers **must** be idempotent per `event_id` and order state transitions by `(block_number, log_index)`.
- **Example:** If `--backfill-contract` backfills a newly added pool while the main stream is already at block `52060500`, historical entries for blocks `52060000..52060500` will appear in the Redis stream after existing entries for block `52060500`.

### 5.4 At-Least-Once Delivery & `log → revert → log` Flips
- **At-least-once delivery:** Redis Lua deduplication (`evml:{base_eurc_usdc}:dedup:<event_id>`) tracks the last emitted `type` (`"log"` or `"revert"`) for 7 days (`dedup_ttl_s = 604800`). Within the 7-day window, duplicate emissions of the same type are suppressed atomically in Redis, while re-backfills after 7 days can re-emit historical `type=log` entries.
- **`log → revert → log` transitions are legal:** If branch A is emitted (`type=log`), reorged out in favor of branch B (`type=revert`), and then branch A becomes canonical again in a subsequent flip, the same `event_id` is legally emitted a second time as `type=log`.
- **Example:** For `event_id = "8453:0xaaa...:3"`, the sequence `log → revert → log` has a net-live state of `log` (active), whereas `log → revert` has a net-live state of `revert` (retracted).

### 5.5 48-Hour Hard Read Deadline (`retention_ms`)
- Stream entries older than `retention_ms = 172800000` (**48 hours**) are trimmed automatically on every write via `XADD ... MINID ~ <now_ms - 172800000>`, **regardless of whether a consumer group has read or acknowledged them**.
- Every downstream consumer **must** register its Redis Stream consumer group in `[sink] consumer_groups` in [`base.toml`](evm_listener/config/base.toml):
  ```toml
  consumer_groups = [{ stream = "events", group = "strat:aero_eurcusdc:botB_v1" }]
  ```
- The leader probes registered consumer groups every 30 s and raises `Consumer Lagging` at **12 h** (`warning`), `Consumer Near Trim` at **24 h** (`critical`), and `Consumer Entries Trimmed` (`critical`) if `XTRIM` deletes unread entries past the group's cursor.

### 5.6 Listener Liveness (`hb:<host>`)
- **Never infer a dead listener from stream silence.** On Base, gauge and registry contracts can go hours without an on-chain event during quiet market periods.
- Instead, inspect the leader heartbeat key `evml:{base_eurc_usdc}:hb:<host>` (written every `10 s` with a `30 s` `EX` TTL):
  ```json
  {"ts":1727890030000,"role":"leader","state":"live","head":52060312,"G_s":52060300,"G_f":52059780,"C_s":52060300,"C_f":52059780,"active_provider":"infura","epoch_n":1,"version":"0.2.0"}
  ```
- If `hb:<host>` exists (TTL > 0), has `"role":"leader"`, `"state":"live"`, and `C_s` is tracking `G_s`, the listener is healthy and the market is simply quiet.

### 5.7 Accepted v1 Risks
1. **Single Redis Node:** Railway Redis is a single instance (`W5`). During a temporary Redis outage, the active leader continues sweeping into its process-scoped disk spill (`/tmp/evml-spill`, up to 512 MiB) and replays in order on reconnect. If Redis suffers permanent volume loss, operators re-backfill from `start_block` (see [`doc/runbook.md`](doc/runbook.md)).
2. **Redis AOF `everysec` 1-Second Window:** A hard Redis crash can lose up to 1 second of acknowledged writes (both stream entries and checkpoint updates). On recovery, the listener re-sweeps from the older persisted checkpoint and re-emits the lost second of events. Only if those exact blocks also underwent a safe reorg during the same 1-second crash window could a consumer that read the pre-crash stream hold an un-reverted log.
3. **7-Day Dedup Key TTL on Re-Backfill:** `evml:{base_eurc_usdc}:dedup:<event_id>` keys expire after 7 days (`604800 s`) to bound Redis memory (~10 MB). Re-running a historical backfill covering blocks older than 7 days will re-emit `type=log` entries, which consumers must ignore via their own `event_id` idempotency check.

---

## 6. Architecture

- [**System Modules & Architecture (`doc/System_Modules_and_Architecture.md`)**](doc/System_Modules_and_Architecture.md) — Module boundaries (`core`, `rpc`, `provider`, `chain`, `pipeline`, `sink`, `ops`, `app`), concurrency model (single-threaded Boost.Asio `io_context` + dedicated Redis sink thread + watchdog thread), and data flow.
- [**Aerodrome Slipstream EURC/USDC & Dynamic Discovery (`doc/Dynamic_Discovery.md`)**](doc/Dynamic_Discovery.md) — Verified Base contract addresses, event ABI signatures, `topic0` hashes, and static-shard discovery design.

