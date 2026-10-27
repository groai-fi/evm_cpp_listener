# 01: Review Findings (docs + repo)

Severity: 🔴 blocker / correctness · 🟠 production risk · 🟡 minor / doc error

## Repo state

| ID | Sev | Finding | Evidence | Fix (phase) |
|---|---|---|---|---|
| R1 | ✅ | **Resolved** (2026-10-05): you scrubbed the rule file. Re-verified: 0 of 5 `.env` secret values appear in repo files. Rotation is still recommended, because the values were copied into the global agent rules. | `.agent/rules/no-hardcoded-secrets-in-scripts.md` | `tools/check_secrets.sh` pre-commit (P0) |
| R2 | ✅/🟠 | `//` comments fixed by you. **New:** `find_package(simdjson QUIET)` at the root continues silently when simdjson is missing (fail-fast violation) and belongs in the subproject. | root `CMakeLists.txt` L8–9 | Move to `evm_listener` as `REQUIRED` (P0) |
| R3 | 🟡 | Root requires `pkg_check_modules(UUID)`, which the listener does not need. | root L10 | Remove; `common/` keeps its own (P0) |
| R4 | 🔴 | `evm_listener/CMakeLists.txt` is empty. | | Write in P0 |
| R5 | 🟡 | Devcontainer updated by you: `cpp:1-bookworm` + Conan, and it works. That's fine; we stay on Debian bookworm, not Ubuntu. Remaining items: apt `-dev` libraries conflict with Conan (system Boost 1.81 could be picked up); Redis runs inside the container, but you prefer Docker Redis. | `.devcontainer/*` | Compose with a `redis` service; drop the apt libs (P0, Q7) |
| R6 | 🟠 | `.env.example` (tracked) and `.env.local` are empty. | | P0 |
| R7 | 🟠 | `.env` has HTTPS URLs only: no WSS variables, no QuickNode. | | New `*_WSS_URL*` vars (Q4) |
| R8 | 🟡 | `systemd_config/` is empty. | | Not needed: prod is Railway. Mention only; leave it for you to delete |
| R9 | 🟡 | `.gitignore` is missing `CMakeUserPresets.json` (Conan output), `*.wal`, `wal/`. | | P0 |
| R10 | 🟡 | `common/` Redis client is blocking, uses a mutex, and puts `using namespace std` in headers. Not suitable for reuse. | `common/include/redis_client_base.h` | Leave untouched; the listener uses redis-plus-plus |

## Design_v1.md

| ID | Sev | Finding | Fix |
|---|---|---|---|
| D1 | 🔴 | Mainnet parameters: `chain_id=1`, `confirmations=12`, `eth-mainnet` URLs. Also, 12 confirmations is a heuristic, not ETH finality. | Base 8453 with the tag-based finality gate (master §2) |
| D2 | 🔴 | State-machine order `BACKFILL → SUBSCRIBING` leaves a gap. §3's own text says to subscribe first. | `Subscribing (buffer) → Backfilling → drain → Live` |
| D3 | 🔴 | Checkpoint advances only "每個 log". With events once a day, the checkpoint barely moves and every restart backfills days of blocks. | Advance on **gate movement** (finalized head), independent of events |
| D4 | 🔴 | "`MULTI/EXEC` 或 Lua" for dedup + XADD. MULTI cannot make XADD conditional on the `SET NX` result. | Lua / Redis Function only |
| D5 | 🔴 | The `revert` event reuses the original's dedup key, so `SET NX` silently drops the revert. | Dedup key includes type: `…:{logIndex}:revert` |
| D6 | 🔴 | Backfill to `latest` via `getLogs`: (a) never reports `removed`, so a reorg during downtime is never reverted; (b) a lagging load-balanced backend can return `[]` for a recent range (silent miss). | Backfill/reconcile only up to the gate; canonical-hash check |
| D7 | 🟠 | "Redis (本機)" contradicts cross-host redundancy (p.s.: "至少兩台 host"). Per-host Redis means no shared dedup and two divergent streams. | One shared HA Redis (Q3); hash-tagged keys |
| D8 | 🟠 | No detection of a **silently dead `logs` subscription** while `newHeads` keeps flowing (a known provider failure mode). | Reconciler sweep per gated range on a second provider |
| D9 | 🟠 | `head_lag_blocks` ("provider latest − local head") has no independent reference defined. | HTTP `eth_blockNumber` poll across all providers |
| D10 | 🟠 | The p.s. suggests alerting on `seconds_since_last_event`. With daily events this alerts all the time. | Liveness = heads/gate/heartbeat, never event recency |
| D11 | 🟠 | Dedup TTL of 1 day is shorter than a plausible single-host outage. When that host comes back and backfills, it produces duplicates. | TTL ≥ max outage (default 14 d); downstream still idempotent on `event_id` |
| D12 | 🟠 | systemd only. The devcontainer and Railway have no systemd. | Also `/healthz` + Docker HEALTHCHECK |
| D13 | 🟡 | `stall_timeout 45s` = 22 Base blocks, too lax. | 15 s (≈7 blocks), configurable per chain |
| D14 | 🟡 | vcpkg (§7) contradicts your Conan preference. | Conan 2 |
| D15 | 🟡 | "靜態連結單一 binary": a fully static glibc breaks `getaddrinfo`/NSS. | Static third-party libs; dynamic glibc |
| D16 | 🟡 | Unit has `After=redis.service`, which assumes local Redis. | Drop it for remote Redis |

## Sample_features.md

| ID | Sev | Finding | Fix |
|---|---|---|---|
| S1 | 🔴 | Checkpoint is set to the live (unsafe) head on `newHeads`, so logs can be skipped. The doc admits this risk itself. | Gate + reconcile-based checkpoint |
| S2 | 🟠 | Dedup is in memory only, so it is lost on restart and not shared across hosts. | Redis Lua dedup |
| S3 | 🟠 | Backfill `sync timeout 120s` is a fixed **total**. After a long outage, backfill never finishes, which becomes a restart loop. | Per-request timeout + progress watchdog |
| S4 | 🟠 | `getLogs` page halving never grows back. JSON-RPC ids are fixed constants (`kPage`), which is fragile. | AIMD range; monotonically increasing ids with a pending map |
| S5 | 🟠 | Webhook: one blocking connection per POST, ack file rewritten per event, no `fsync`, unbounded WAL. | Redis sink + CRC WAL with rotation and cap |
| S6 | 🟡 | `Deposit/Withdraw(address,uint256,uint128)` signatures are unverified. | Config takes the signature; optional `expect_topic0` asserted at startup |
| S7 | 🟡 | `on_head` reorg only logs. | Reorg detector + metrics + revert path |
| S8 | 🟡 | Flashblocks are mentioned. Preconfirmation hashes are not final. | Out of scope for v1 |

## Preferred_tech_stack.md

| ID | Sev | Finding |
|---|---|---|
| P1 | 🟡 | L67 says "**simdjson** is the … implementation of the EVM". That should be **evmone**. |
| P2 | 🟡 | L23: simdjson is parse-only and cannot format requests. Use Boost.JSON or a string builder. |
| P3 | 🟡 | L22 `wss://://alchemy.com` typo. |
| P4 | 🟠 | `-march=native -ffast-math`: native breaks portability (arm64 Mac container vs x86 prod; heterogeneous hosts), fast-math is pointless here, and simdjson already does runtime CPU dispatch. Use `-O2/-O3`, optional LTO. |
| P5 | 🟠 | Versions are stale (boost 1.86, simdjson 3.10). Boost ≥1.89 removed the `Boost.System` stub library, so `find_package(Boost COMPONENTS system)` must become `Boost::headers` (**verify during P0**). |
| P6 | 🟠 | **evmone is not on ConanCenter** (verified: 404). evmc, intx and ethash are. |
| P7 | 🟡 | secp256k1, RLP and signing are out of scope (no tx signing). |
| P8 | 🟡 | "simdjson … without heap allocations" ignores the `SIMDJSON_PADDING` requirement. Plan: one reusable padded buffer per connection. |
| P9 | 🟠 | **evmone for listener testing (your request): pushback.** evmone executes bytecode only. It has no JSON-RPC, no blocks, no `eth_subscribe`, no logs-by-filter, no reorgs. Testing a listener needs a **node or node mock**: an in-process scripted mock provider (unit/integration) plus **Anvil** (`anvil_reorg`, `evm_mine`) for e2e. Recommend dropping evmone from v1. |
