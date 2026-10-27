# Phase 5: Operations on Railway (prod) and Docker (local), rev 10

**Goal:** the process is observable, restarts itself when it wedges, and survives a provider outage, a container/host loss, and a Redis outage, all **without systemd**.

## 5.0 Railway constraints (verified 2026-10-05 against Railway docs) and their consequences
| Railway fact | Consequence for the design |
|---|---|
| Healthcheck is used **only at deploy time**, not for continuous liveness | Liveness is our job: an in-process watchdog thread **self-aborts** a wedged process → the restart policy restarts it |
| Restart policy `ALWAYS` / `ON_FAILURE` + `restartPolicyMaxRetries` | Use `ALWAYS`. Exit codes still distinguish config errors (2) from runtime failures (1) and watchdog aborts (134) in the logs |
| SIGTERM, then SIGKILL after `RAILWAY_DEPLOYMENT_DRAINING_SECONDS` (**default 0**) | Set `RAILWAY_DEPLOYMENT_DRAINING_SECONDS=30`, otherwise graceful drain never runs. Correctness does not depend on the drain (checkpoint/re-sweep); it only shortens the re-sweep |
| Volumes: **no replicas**, brief downtime on redeploy, mounted as **root** | **No volume** (r6, D1): the spill lives on ephemeral container disk (`/tmp/evml-spill`) and is wiped at process start. Cross-host = **separate services**, not replicas |
| Private network `*.railway.internal` is IPv6 (AAAA) | hiredis/redis-plus-plus must resolve IPv6. **Verified by `--check-redis` in the Phase 6 Railway smoke (70_phase6_testing_rollout.md §6.5).** Ops HTTP binds `[::]` |
| No built-in Prometheus | Alerts are sent by the listener itself (5.3). `/metrics` stays available for an optional Grafana Alloy scraper later |

## 5.1 Three-layer watchdog
| Layer | Detects | Action |
|---|---|---|
| Session | no newHeads 15 s · no pong (Beast idle 20 s) · subscribe ACK timeout · TCP_USER_TIMEOUT | reconnect with backoff; failover after 3 failures or staleness |
| Pipeline | a cursor not advancing while its gate is ahead (`progress_stall_ms`, per cursor; rev 5, G4) · gate not advancing (`gate_lag_alert_s`) · sink saturated · lease lost | `report_bad` on the current getLogs provider + WARN; 3 consecutive deadlines → `PipelineStalled` / alert (no demotion, rev 8 D4) |
| Process | io loop or sink thread frozen | **watchdog thread** (the only extra thread) checks two atomic heartbeats every 1 s (io tick, sink loop). Stale > 30 s → log CRITICAL with the last known state → `std::abort()` → Railway restarts it. Local Docker: `restart: always` + HEALTHCHECK |

The watchdog thread touches only atomics and `write(2)` to stderr. It takes no locks and allocates nothing, so it still works when the process is wedged.

## 5.1a WS liveness mode `ping`: moved out of Phase 5 (rev 9)
Opt-in `[ws] liveness = "ping"` (replaces the billed `newHeads` subscription with a periodic `eth_blockNumber` over the live WS) is now a **separate job after Phase 6**: see [71_job_ws_ping_liveness.md](71_job_ws_ping_liveness.md). Phase 5 implements `heads` mode only (the current behaviour); the `[ws]` config section is **not** added in Phase 5. The §5.3.3 `WebSocket Stalled` alert uses the `heads` trigger until that job lands.

## 5.2 HTTP endpoints (Beast on the io thread, `[::]:${PORT:-9464}`)
- `/healthz`: 200 unless a heartbeat is stale. Used as the Railway **deploy** healthcheck path.
- `/readyz`: 200 iff the process is leader and Live, or a healthy standby; sink not saturated; gate age OK. Returns a JSON body with state.
- `/metrics`: Prometheus text (prometheus-cpp core `TextSerializer`).
- The service gets **no public domain**: endpoints stay on the private network. No secrets appear in any response.
- The acceptor sets `ip::v6_only(false)` explicitly (rev 10), so `[::]` also accepts IPv4 (`127.0.0.1` in local tests) whatever the host's `net.ipv6.bindv6only` default is.

## 5.3 Alerting without Prometheus
### 5.3.1 n8n alert webhook (existing workflow "EVM Listener Alert Webhook" → Slack `#strategy-production`)
- `POST ${N8N_ALERT_WEBHOOK_URL}` with headers `Content-Type: application/json` and `X-API-Key: ${N8N_ALERT_WEBHOOK_TOKEN}`. The token is never logged; `redact()` covers the URL.
- Payload (your schema):
  ```json
  { "severity": "critical|warning|info", "service": "evm-listener", "shard": "base_eurc_usdc",
    "host": "<EVML_HOST_ID>", "title": "<alert title>", "message": "<one line>",
    "ts": "<ISO-8601 UTC>", "details": { "...": "alert-specific numbers" } }
  ```
- Rules are evaluated in-process every 10 s. Each alert in §5.3.3 is one of two kinds (rev 10):
  - **Condition** (a state that is true or false now, e.g. Safe Gate Stuck): one *fire* message, a 15 min re-notify while it is still true, and one *resolve* message (`severity=info`, title prefixed `RESOLVED:`) when it clears.
  - **Event** (something that happened once, e.g. Provider Failover): one message per occurrence, rate-limited per key (provider, group, …) to one per 15 min. **No resolve message**: there is no state that "clears". Example: QuickNode → Infura failover pages once; it does not send `RESOLVED:` 10 s later because Infura is healthy, and it does not re-notify every 15 min while QuickNode stays down (Provider Quota Exhausted / Auth Failure cover that state).
  - The POST is async on the io thread.
  - **Delivery failure is visible but not fatal.** A non-2xx or timeout is retried 3× with jittered backoff. Then ERROR is logged and `evml_alert_send_failures_total` is incremented; `/readyz` stays unaffected. The dead-man's switch covers the case where alerts can't get out.

### 5.3.2 Dead-man's switch: healthchecks.io (decided)
- `DEADMAN_PING_URL` is already in `evm_listener/.env`. It goes into Railway variables for both services. The URL is a credential: anyone holding it can keep the check green, so it is never logged or committed.
- **Who pings:** only the **leader**, and only when healthy. Healthy means all of:
  - state is `CatchingUp` or `Live`;
  - the sink is not saturated;
  - `C_s` has advanced in the last 10 min, or `G_s` hasn't moved either.
- The standby never pings. If the leader dies, the standby takes over within ≤ 20 s and starts pinging, so a normal failover never pages. If both services are down, or Redis is down so that nobody leads and no data flows, the pings stop.
- **Check settings in the healthchecks.io dashboard:** period 1 min, grace 2 min → page after ~3 min of silence, independent of Railway and n8n. Route the check's integration to the same Slack channel (or email/phone).
- Ping = `GET <url>` with a 10 s timeout and the same User-Agent, async on the io thread. A failure is logged and counted (`evml_deadman_ping_failures_total`), never fatal.
- **Empty `DEADMAN_PING_URL`** → pinging is skipped, a WARN is logged at startup, and `/readyz` JSON shows `"deadman":"disabled"`. It is visible, but not an error, so local dev works without it.

### 5.3.3 Alert table (severity mapped to your schema)
| Title | Condition | severity | `details` |
|---|---|---|---|
| WebSocket Stalled | `heads`: **no newHeads** from the active provider > 30 s (Base produces one every 2 s). `ping` (job 71 (WS ping), after Phase 6): `ping_stall_count` bad pings in a row | critical | `seconds_since_last_head` (or `seconds_since_last_good_ping`), `liveness_mode, head_lag_blocks, last_processed_block (=C_s), provider` |
| Provider Failover | active provider switched | warning | `from, to, reason` |
| Provider Quota Exhausted | "Monthly capacity limit exceeded" / sustained 429 | warning, once per incident (critical if its chain has no healthy provider left) | `provider, chain, error_code, message, next_probe_ts` |
| getLogs Provider Lagging | getLogs provider's `safe` > 30 blocks behind the WS provider's for 2 min → getLogs failover | warning | `provider, its_safe, ws_safe` |
| Provider Auth Failure | 401/403 | critical | `provider, http_status` |
| Safe Gate Stuck | `safe` not advanced > `safe_gate_stuck_s` (10 min) | critical | `G_s, head, seconds_since_advance` |
| Finalized Gate Stuck | `finalized` not advanced > 60 min | critical | `G_f, head` |
| Checkpoint Lag | `G_s − C_s` > 300 blocks or `G_f − C_f` > 2000 for 15 min | critical | `class, gate, ckpt, lag_blocks` |
| Safe Reorg | a released safe-class event vanished → revert emitted | critical | `fork_block, reverted_count, reemitted_count` |
| Sink Degraded | spill non-empty > 5 min | critical | `spill_bytes, oldest_record_age_s` |
| Sink Saturated | spill cap reached (pipeline frozen) | critical | `spill_bytes, spill_max_bytes` |
| Redis Unreachable (r6, P2) | Redis connect/PING failing > 60 s (startup wait or leader outage) | critical | `since_s, addr_family` |
| Sink Fenced (r6, P3) | a write or renew returned `FENCED`; queue and spill discarded, demoted to standby | warning | `old_term, new_term, discarded_records` |
| Sink Invariant (r6) | `emit.lua` returned `BELOW_FINALIZED` (item dropped) | critical | `event_id, block, c_f` |
| WS Missed Logs | a sweep found logs the WS never delivered | warning | `count, provider, block_range` |
| Provider Inconsistency / Gate Regressed (finalized) | > 0 | critical | `provider, block, expected_hash, got_hash` |
| Provider Inconsistent (rev 5, G2) | `report_bad` on a provider (bad getLogs response, or canonical-hash disagreement); rate-limited to one per provider per 15 min | warning | `provider, reason, block_range, count_1h` |
| Safe Sweep Unreliable (rev 5, D2) | > 3 late logs (missed by the safe sweep, found by the final sweep) in 1 h | warning | `count_1h, provider, last_block` |
| Pipeline Stalled (rev 5, G4) | a cursor missed 3 consecutive progress deadlines, or a sweep returned `Stalled` (min range still `RangeTooLarge`, or no agreement after `max_bad_attempts` bad pages) | critical | `class, cursor, gate, provider, detail` |
| Leader Changed | lease taken over by another host | warning | `old_host, new_host` |
| Redis Misconfigured | eviction policy ≠ `noeviction` or persistence off (startup check) | critical | `maxmemory_policy, appendonly, redis_version` |
| Consumer Lagging (r6, Q2) | `max(undelivered_age_s, pending_age_s)` > `consumer_lag_warn_s` (12 h) | warning | `stream, group, undelivered_age_s, pending_age_s, lag, retention_s` |
| Consumer Near Trim (r6, Q2) | same > `consumer_lag_crit_s` (24 h) | critical | same |
| Consumer Entries Trimmed (r6, Q2) | `trimmed_unread` | critical (data loss already happened; recovery = consumer re-reads from its own store, or a backfill into a fresh stream) | `stream, group, last_delivered_id, max_deleted_id` |
| Consumer Group Missing (r6, Q2) | configured group not found for 10 min | warning | `stream, group` |

The four consumer alerts come from the Phase 4 §4.10 probe and are rate-limited to one per group per 15 min.

**Kinds (rev 10, §5.3.1):** **Event** alerts are Provider Failover, Safe Reorg, Sink Fenced, Sink Invariant, WS Missed Logs, Provider Inconsistent, Leader Changed and Consumer Entries Trimmed. Every other row is a **condition** alert.

**Never** alert on event silence. Your sample `seconds_since_last_event > 120s` would page during every quiet minute; gauge events can be hours apart (master W6). The sample's title "WebSocket Stalled" is kept, with a head-based trigger.

## 5.4 Metrics (unchanged list from the previous revision, plus)
`evml_role{role=leader|standby}`, `evml_lease_renew_failures_total`, `evml_provider_active{provider}`, `evml_provider_failovers_total`, `evml_alerts_fired_total{alert}`, and (rev 5) `evml_provider_inconsistency_total{provider}`, `evml_safe_missed_total`, `evml_reverify_total{result}`, `evml_getlogs_throttled_total{provider}`, and (rev 6) `evml_consumer_undelivered_age_seconds{stream,group}`, `evml_consumer_pending_age_seconds{stream,group}`, `evml_consumer_lag_entries{stream,group}`, `evml_spill_discarded_records_total`.

## 5.4a `LogSource` adapter over `ProviderPool` (rev 5, G1/G2)
The production implementation of Phase 3's `LogSource` interface:
- **Token bucket per provider** (`reconcile_rps`, default 2, burst 4), so catch-up bursts don't cause 429s in the first place.
- **`report_bad(p, reason)`** → `on_failure(Retry)` on that provider and `provider_inconsistency_total{p}`. After `failover_after` failures the provider is `Down` and recovers only through a probe.
- **Page retries exclude** the provider that just failed or answered badly (`pick_getlogs(exclude={p})`).
- **`block(n)` is independent of the getLogs provider:** it reads the `ReorgDetector` ring first (fed by the WS provider); on a ring miss it calls `eth_getBlockByNumber` on a provider **other than** the one that returned the logs. Disagreement → no release + `report_bad`; no quorum after N attempts → `block(n)` returns nullopt, which the sweeper treats as `Retry`; the progress deadline then raises `PipelineStalled` (rev 9).

## 5.5 Cross-host redundancy: active-passive via a Redis lease (✅ accepted)
Why: it keeps 1× RPC quota on plans that are already limited. With pools at `safe`, a takeover adds ≤ 20 s on top of the ≈ 20–30 s safe-class latency, and only during the takeover itself.

- Two Railway services from the same image: `evml-base-a` and `evml-base-b`, ideally in **different regions**, no volume (r6, D1). They share the same shard id `base_eurc_usdc`.
- Lease (r6, P3): `lease.lua` over keys `evml:{base_eurc_usdc}:leader` (token, PX TTL) and `evml:{base_eurc_usdc}:epoch` (latest token, no TTL). `acquire` mints token `n|host|nonce` with an incremented epoch; the leader `renew`s every 5 s (`OK` / `RESUMED` / `FENCED`). Every write script is fenced on the token (Phase 4 §4.2, §4.8).
- **Standby:** no WS and no reconciler. It polls for the lease every 5 s and runs no sink writes; it has no spill (the spill is process-scoped). RPC cost is ≈0.
- **Takeover:** the standby acquires the lease → enters Recovering from the shared `ckpt {safe, finalized}` → sweeps both ranges → Live. Expected takeover time is ≤ 20 s.
- **Demotion (r6, P3):** if renew returns `FENCED`, or another term holds the lease ⇒ stop releasing, drop WS, discard the queue and spill (`SinkFenced`). If Redis is merely unreachable ⇒ keep sweeping into the spill; the first op on reconnect is `renew` (`OK`/`RESUMED` → replay the spill; `FENCED` → demote) (Phase 4 §4.8). Split-brain of any length is harmless: only the token in `leader` can write.
- **Redis down for both:** no one can renew. The current leader keeps running in spill-degraded mode; the standby cannot acquire. On Redis recovery the leader's first op is `renew`: `RESUMED` if nobody took over, so the spill is replayed; otherwise `FENCED` and the spill is discarded (fencing, not ownership, makes this safe).
- **Remaining SPOF = the Railway Redis single node.** Covered by the spill (outage, same process) and re-backfill from `start_block` (data loss). Documented as an accepted v1 risk.

## 5.6 Railway service configuration (`railway.json` at the repo root, config-as-code; rev 8, D10)
```json
{ "$schema": "https://railway.com/railway.schema.json",
  "build":  { "builder": "DOCKERFILE", "dockerfilePath": "docker/Dockerfile.prod" },
  "deploy": { "healthcheckPath": "/healthz", "healthcheckTimeout": 120,
              "restartPolicyType": "ALWAYS" } }
```
**No `startCommand` (rev 9 fix).** Railway runs a custom start command in exec form **in place of the image `ENTRYPOINT`**, which would bypass `tini` (no signal forwarding, no zombie reaping). The image carries the command instead: `ENTRYPOINT ["/usr/bin/tini","--","/usr/local/bin/evm_listener"]` + `CMD ["--config","/app/config/base.toml"]`. `docker run <image> --version` still works because it replaces only `CMD`.

Service variables (set in the Railway UI, never committed):
- Providers (each one is enabled by being set): `BASE_WSS_URL_QUICKNODE`, `BASE_RPC_URL_QUICKNODE`, `BASE_WSS_URL`, `BASE_RPC_URL`, `BASE_RPC_URL_ANKR`, `BASE_RPC_URL_ALCHEMY` (it rejoins automatically when its quota resets). The public Base endpoint is a config literal and needs no variable.
- `REDIS_URL=${{Redis.REDIS_URL}}`, `EVML_HOST_ID=a|b`.
- `N8N_ALERT_WEBHOOK_URL`, `N8N_ALERT_WEBHOOK_TOKEN`, `DEADMAN_PING_URL` (the same URL on both services; only the leader pings).
- `RAILWAY_DEPLOYMENT_DRAINING_SECONDS=30`.

No volume (r6, D1): `spill_dir` defaults to `/tmp/evml-spill` on ephemeral disk.

New config keys (rev 5, in `base.toml`, with defaults; rev 8 D9 fixes the sections): `[chain] pending_max_logs = 200000` (PendingBuffer cap), `[redundancy] reconcile_rps = 2`, `[redundancy] reconcile_burst = 4` (must be ≥ rps), `[watchdog] progress_stall_ms` (already present). The `[ws]` section (rev 7) moves to job 71 (WS ping).

## 5.6a Contract coverage and `--backfill-contract <name>` (rev 5, N2)
- **Startup check (Recovering):** compare the config with the stored `ckpt.contracts` manifest (Phase 4 §4.1). A contract with `start_block ≤ C_s` that is not in the manifest ⇒ **exit 2**; the message names the contract and the exact command to run. A contract with `start_block > C_s` is added to the manifest without a backfill, since the sweeps will reach it. Removed contracts just drop out of the manifest.
- **The tool** (run with both listener services stopped; they would exit 2 at startup anyway until the manifest covers the contract):
  - one-off `Sweeper` with a filter for that address only;
  - `[start_block, C_f]` is emitted at `finality=finalized`;
  - for a safe-class contract, `(C_f, C_s]` is emitted at `finality=safe` and tracked in `released`, so the normal final sweep later diffs it like any other safe event;
  - all emissions go through the same sink (spill, dedup); the manifest is updated only after the last page is acknowledged;
  - exit 0 on success, 1 on a stall (rerunnable; it restarts from `start_block` and dedup collapses repeats).
- Automatic backfill as a third sweep stays in v1.5.

Logging: one JSON object per line on stdout with `level` and `message` keys, plus fields. That is the shape Railway's log explorer parses as structured logs. **Verify on the first deploy.**

## 5.7 Docker image (`docker/Dockerfile.prod`, multi-stage, used by Railway and locally; rev 8, D10)
- Builder: `debian:bookworm` + the toolchain + Conan, matching the devcontainer, so there is one ABI everywhere. Release build with static third-party libraries and dynamic glibc.
- Runtime: `debian:bookworm-slim` + `ca-certificates` (TLS verification of provider certs) + `tini` as PID 1 for signal forwarding.
- `STOPSIGNAL SIGTERM`. `HEALTHCHECK` is honoured locally only.
- Image size target < 60 MB.

## 5.8 Local Docker (`.devcontainer/docker-compose.yml`, `listener-a` / `listener-b` under the `listeners` profile; rev 8, D10)
`redis` (`redis:8.4`, pinned to match Railway prod 8.4.0; your local 8.6.2 is compatible; `--appendonly yes --maxmemory-policy noeviction`) · `toxiproxy` (chaos) · `listener-a` and `listener-b` (lease testing) · optional `anvil`. The devcontainer joins the same compose network (see Phase 0).

## 5.9 Rollout on Railway
Deploy `evml-base-a` → wait for Live and `checkpoint_lag` < 1 page → deploy `evml-base-b` (becomes standby) → force a takeover test (restart a) → confirm the LeaderChanged alert and a gap-free stream.
On a redeploy the other service takes over through the lease.

## Tests
- Chaos (toxiproxy, local compose): latency 5 s, `reset_peer`, timeout, bandwidth 0 on provider and Redis links → automatic recovery with an exact stream.
- `docker pause listener-a` → watchdog cannot run while the process is paused, so the lease expires and b takes over within 20 s. After unpause, a either demotes itself on its next renew attempt, or (if its watchdog fires first) restarts and comes back as standby; both are correct (rev 8 D2).
- io-freeze test hook (rev 8 D3: `EVML_TEST_HOOKS` build, `--test-freeze-io-after <s>`) → watchdog aborts within `process_stall_ms` (30 s), exit 134. SIGSTOP cannot target one thread.
- Alert rules: unit tests that feed synthetic metric snapshots and expect each condition alert to fire, re-notify after 15 min and resolve, and each event alert to fire once per occurrence, respect its 15 min rate limit and never send `RESOLVED:` (rev 10).
- Alert transport: a MockProvider-style HTTP sink asserts the exact JSON schema, the `X-API-Key` header, and the 3× retry followed by the failure metric, and checks that the token never appears in any log line.
- Live (manual, once): send a `severity=info` test alert to the real n8n webhook → it appears in `#strategy-production`.
- Dead-man (local, mock HTTP sink): only the leader pings; the standby sends 0 pings; pings stop when the sink saturates or `C_s` stalls; an empty URL gives a startup WARN and `"deadman":"disabled"`.
- LogSource adapter (rev 5, MockProvider): token bucket caps the rate; `report_bad` moves a provider toward `Down` and the page is retried on another one; `block(n)` hits the ring before any RPC and never asks the provider that returned the logs; disagreement ⇒ no release, no quorum ⇒ nullopt (`Retry`).
- Contract coverage (rev 5): adding a contract with `start_block ≤ C_s` ⇒ exit 2 naming it; `--backfill-contract` against Docker Redis + MockProvider emits exactly that contract's history (finalized below C_f, safe and tracked above), updates the manifest, and a second run adds 0 entries; the next startup passes. Lease (D8, rev 10): with no live leader the backfill acquires the lease, never gets `FENCED` and releases it at the end; while another host holds it, exit 1 within 60 s and nothing written.
- Ops HTTP dual-stack (rev 10, §5.2): `http://127.0.0.1:<port>/healthz` and `http://[::1]:<port>/healthz` both return 200.
- New alerts: Provider Inconsistent (event), Safe Sweep Unreliable and Pipeline Stalled (conditions) fire, and the conditions resolve, from synthetic snapshots.
- Rev 6 alerts: Redis Unreachable, Sink Fenced, Sink Invariant and the four consumer alerts fire from synthetic snapshots, and the condition ones resolve (including `ConsumerLag` snapshots from the Phase 4 §4.10 probe); consumer alerts respect the 15 min per-group rate limit.
- WS liveness `ping` tests moved to [71_job_ws_ping_liveness.md](71_job_ws_ping_liveness.md) §10 (rev 9).

## Acceptance
All chaos scenarios recover automatically. A lease takeover produces a gap-free, duplicate-free stream. In tests every condition alert fires and resolves, and every event alert fires (rev 10).

## Rev 5 changelog (3d review, see 80_phase3d_review.md)
Integrated above: §5.1 per-cursor progress deadline, §5.3.3 three new alerts, §5.4 metrics, §5.4a LogSource adapter, §5.6 config keys, §5.6a coverage check and `--backfill-contract`, and the matching tests. Refinement made while integrating: the backfill tool also covers `(C_f, C_s]` for safe-class contracts (tracked in `released`). Stopping at `C_f` would have left that window uncovered, because the manifest marks the contract as covered up to `C_s`.

## Rev 6 changelog (Phase 4 review, see 81_phase4_review.md)
Integrated above: D1 no volume, spill on ephemeral disk (§5.0, §5.6, §5.9); P3 epoch-fenced lease, standby without sink writes, fenced demotion vs unreachable-Redis spill (§5.5); P2/P3 sink alerts Redis Unreachable, Sink Fenced, Sink Invariant and spill wording for Sink Degraded/Saturated (§5.3.3); Q2 consumer-group lag alerts and metrics (§5.3.3, §5.4); `evml_spill_discarded_records_total` (§5.4).

## Rev 7 changelog (WS liveness option)
- §5.1a adds an opt-in `[ws] liveness = "ping"`: a JSON-RPC `eth_blockNumber` over the live WS every `ping_interval_s` replaces the `newHeads` subscription.
- The default stays `heads`, so it is not a breaking change.
- Accepted trade-offs in `ping` mode:
  - WS stall detection takes minutes instead of 15 s;
  - no unsafe-reorg purge (metric noise only);
  - canonical hashes come from `eth_getBlockByNumber` on a non-getLogs provider.
- Release, safe-reorg handling and recovery are unaffected.
- Also updated: the §5.3.3 `WebSocket Stalled` trigger, the §5.6 config keys and the tests.

## 5.H Host process (rev 8, D1; was missing from rev 7)
**Why:** `main.cpp` only dispatches CLI tools today. Nothing assembles pool + gate + driver + sink or runs the tick loop, so the image command (`--config` with no action flag) exits 2. This section is the spec for that process (task slice 5-D).

**CLI:** `--config <file>` with no action flag runs the listener. The existing tool flags stay.

**Threads (three):**
| Thread | Owns |
|---|---|
| io | provider pool, driver tick loop, ops HTTP, alert engine and sender, dead-man pinger, standby loop |
| sink | `RedisSink::start()` |
| watchdog | §5.1; touches only atomics and `write(2)` |
Cross-thread reads go only through `RedisSink::status()` and atomics.

**Startup order:**
1. `log::init`.
2. Ops HTTP up, so `/healthz` answers during the Redis wait.
3. Watchdog.
4. `await_redis_ready` and startup checks (they produce the Redis Unreachable / Misconfigured facts).
5. Lease acquire, or the standby loop (polls every `renew_ms`).
6. On leadership: sink (with the token) → pool → gate bridge → driver.

**Gate bridge:**
- New `PoolHandlers::on_tag(provider, Tag, number, hash)`, fired from the existing tag polls.
- Host feeds `FinalityGate::on_safe` / `on_finalized`, then `driver.set_gates`.
- `SafeRegression` / `SafeHashMismatch` → `on_safe_reorg_signal`. `FinalizedAnomaly` → `on_gate_frozen`.
- Heads feed `ReorgDetector`; a fork below `C_s` → `on_safe_reorg_signal`.
- WS logs feed `PendingBuffer(pending_max_logs)`; the comparison against sweep results gives `ws_extra_logs_total` and the WS Missed Logs alert. If the sweeper does not expose the data needed, the worker reports back before changing the sweeper.

**Heartbeat:** the host builds the §4.7 payload every `[ops].heartbeat_s` and calls `sink.set_heartbeat`; it also wires `host_id` and `heartbeat_ms` into `RedisSinkOptions`.

**Lease disabled (`[lease] enabled = false`):** skip acquire and the standby loop; build the sink with `lease_disabled_token()`. `/readyz` shows `"lease":"disabled"`.

**Per-leadership objects:** each leadership term builds fresh `RedisSink`, `ProviderPool`, `FinalityGate`, `ReorgDetector`, `PendingBuffer`, `Sweeper`, adapter and `PipelineDriver`, and destroys all of them on demotion. Only ops HTTP, alerts, metrics and the watchdog live for the whole process. (A new `Spill` wipes `spill_dir` in its constructor, [spill.cpp:58-66](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/spill.cpp#L58-L66), which is correct: a fenced term's spill is discarded anyway.)

**Watchdog and `/healthz` without a sink:** the watchdog checks the sink-loop heartbeat only while a sink exists (leader). On standby and during the startup Redis wait it checks the io heartbeat only, so `/healthz` stays 200 there. Threshold = `[watchdog].process_stall_ms`.

**Leader Changed source:** before `acquire`, the host reads the `epoch` key (latest token `n|host|nonce`). If it names another host, the new leader fires Leader Changed with `old_host` from it. Best effort: the read is not atomic with the acquire, which is fine for an alert.

**Demotion:** the host polls `sink.status().mode == Fenced` each tick, then stops the driver, stops the pool, destroys the sink and returns to the standby loop. The next acquire builds a fresh sink and a fresh spill.

**Shutdown:** SIGTERM/SIGINT → `driver.request_drain()` → wait for the tick loop → `sink.drain(25 s)` (releases the lease) → exit 0.

**Exit codes:** config error 2 · driver `Failed` 2 (prints `status().fatal`) · sink `Failed` or runtime error 1 · watchdog abort 134.

## Rev 8 changelog (Phase 5 readiness review, see 90_phase5_readiness_and_tasks.md)
Decisions approved by the user:
- **D1** §5.H added (above).
- **D2** Watchdog pause tolerance: it tracks its own wake gap. If its own sleep overran by more than 5 s, it resets the baselines and skips that check. It aborts only when a heartbeat has been stale for 30 s while the watchdog itself ran on time. This keeps `docker pause` / VM suspend from causing a false abort.
- **D3** The io-freeze test: a unit test of `Watchdog` with injected clock and abort callback, plus one integration test using a test-only build flag `EVML_TEST_HOOKS` and `--test-freeze-io-after <s>` that blocks the io thread (exit 134). SIGSTOP of one thread is impossible on Linux, so the rev 7 "`kill -STOP` of the io thread" test is replaced.
- **D4** No demotion on `PipelineStalled` in v1 (§5.1 "→ demote" is dropped): alert and `report_bad` only. Reason: a shared cause such as a provider outage would make the two hosts take turns leading.
- **D5** `$PORT`, when set, overrides the port in `[ops].http_listen` (the host part is kept); a malformed `PORT` exits 2.
- **D6** `/readyz` "gate age OK" = `G_s` advanced within `watchdog.safe_gate_stuck_s`.
- **D7** Alert ownership: pipeline, provider, sink and consumer alerts are leader-only. Redis Unreachable, Redis Misconfigured and Sink Fenced may come from either host. Leader Changed is fired by the new leader on acquire, when the previous lease holder differs from self.
- **D8** `--backfill-contract` acquires the lease (blocking, up to 60 s), exits 1 if another host holds it, runs fenced and releases at the end.
- **D9** New keys placed as in §5.6.
- **D10** Paths follow the repo (§5.6–5.8). `Dockerfile.prod` additionally gets `tini`, `STOPSIGNAL SIGTERM`, `HEALTHCHECK`, the config copied to `/app/config/`, and loses the removed `-DEVML_BUILD_COMMON=OFF`.
- **D11** (superseded by rev 9) `ping` mode is a separate job after Phase 6.

## Rev 9 changelog (ping split out)
- User decision 2026-10-22: the WS `ping` liveness mode is removed from Phase 5 (§5.1a, slice 5-J, the `[ws]` config section and its tests) and becomes the separate job [71_job_ws_ping_liveness.md](71_job_ws_ping_liveness.md), scheduled after Phase 6.
- Nothing else in Phase 5 depends on it. Phase 5 builds `heads` mode only. The full rev 7 design text lives in job 71 (WS ping). The file uses prefix 71 because 70 is reserved for the Phase 6 plan (`70_phase6_testing_rollout.md`, master plan table).

**Rev 9 review fixes (2026-10-22):**
- §5.1 Pipeline row: "→ demote" removed (D4 was only in the changelog).
- §5.6: `startCommand` removed, because it overrides the image `ENTRYPOINT` and would bypass `tini`.
- Tests: `kill -STOP` line replaced by the D3 hook; the `docker pause` outcome now accepts the D2 restart path.
- §5.H: lease-disabled branch, per-leadership rebuild list, watchdog/`/healthz` behaviour without a sink, and the Leader Changed source added (below).
- §5.4a: "no quorum → `Stalled`" cannot be signalled through today's `LogSource::block()` (nullopt = `Retry`, [sweeper.cpp:173-178](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/pipeline/sweeper.cpp#L173-L178)). v1 keeps `Retry`; the per-cursor progress deadline already raises `PipelineStalled` after 3 × `progress_stall_ms` (6 min at defaults). No interface change.

## Rev 10 changelog (2026-10-27, plan moved into the repo)
- The plan now lives at `doc/implmentation_plans/60_phase5_ops.md` (was in an agent conversation folder). Cross-links to sibling plans are relative.
- §5.3.1 / §5.3.3: alerts split into **condition** (fire, re-notify, resolve) and **event** (once per occurrence, rate-limited, no resolve). Rev 9 applied fire/resolve to every row, which would send a bogus `RESOLVED: Provider Failover` 10 s after a failover, or re-notify it for as long as the old provider stays down. Tests and Acceptance follow; Phase 6 entry criteria (plan 70) and slice 5-E (plan 90) updated to match.
- §5.2: explicit `v6_only(false)` on the `[::]` acceptor, with a dual-stack `/healthz` test (IPv4 and IPv6 loopback).
- §Tests: backfill lease tests for D8 (acquire, no `FENCED`, release; exit 1 when the lease is held). Plan 90 slices 5-B and 5-G updated to match.
- Stale rev 7 text fixed: §5.4a and §Tests still said "no quorum → `Stalled`", which the rev 9 review had already replaced with nullopt → `Retry`. The Pipeline Stalled row now names the two real `Stalled` sources in [sweeper.cpp](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/pipeline/sweeper.cpp) (min range still too large; no agreement after `max_bad_attempts`).

## Phase 5 results and deviations (per slice)

### 5-0 Prep: done (2026-10-22)
- Cross-reference fixes: plan 60 §5.0 IPv6 row now points at Phase 6 §6.5 (`--check-redis`); job 71 states §6.4 is confirmed.
- Master plan §5 status updated (Phase 4 committed, Phase 5 started).
- `-DEVML_BUILD_COMMON=OFF` removed from [Dockerfile.prod](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/docker/Dockerfile.prod). `mk/`, `Makefile`, `.devcontainer` and both `CMakeLists.txt` have no other reference.
- **Not verified:** the Dockerfile itself was not built (`make prod-image` is slice 5-H). The "no unused-variable warning" acceptance applies to that image build.
- **Baseline: 363 tests at the starting commit.** The first baseline run reported 362/363: `redis sink integration 6` failed with `[chain].pending_max_logs: unknown key`. Cause: that test parses `config/base.toml` at runtime, and I had already edited `base.toml` (5-A) while the old binaries were still running. A harness timing artifact from my own edit, not a product bug; the test passed after the rebuild.
- **Environment note:** `make build` first failed in CMake configure (`Library 'boost_json' not found`): the Conan generators in `build/Debug` were stale. `make conan` fixed it. No repo change was needed.

### 5-A Config additions: done
- `[chain] pending_max_logs` (default 200000, min 1), `[redundancy] reconcile_rps` (default 2, min 1) and `reconcile_burst` (default 4, min 1, must be ≥ `reconcile_rps`): all optional, all in `base.toml` at their defaults. Fields are in `ChainCfg` / `RedundancyCfg` with in-class defaults.
- `$PORT` (D5): applied in `Config::parse` through the injected `env_lookup`, so tests need no process env. It replaces the port after the last `:` and keeps the host. A malformed `PORT` (not digits, 0, > 65535, > 5 chars) or an `http_listen` without `:` throws `ConfigError` (exit 2).
- Tests added in `tests/config_test.cpp` (3 cases): defaults and bare-config defaults; out-of-range and burst < rps; `PORT` override, host kept, boundaries, and seven bad values.
- **Nothing consumes the new keys yet.** `PendingBuffer`, the token bucket and the ops HTTP server pick them up in 5-C / 5-D / 5-B.
- Gate: `make build` exit 0; `make test` exit 0, **366/366** (363 + 3 new).


### 5-B1 Watchdog: done (2026-10-27)
- New [watchdog.hpp](../../evm_listener/src/ops/watchdog.hpp) / [watchdog.cpp](../../evm_listener/src/ops/watchdog.cpp): one thread, two heartbeats (io tick, sink loop), 1 s check, D2 pause tolerance (own wake gap > interval + 5 s → baselines reset, check skipped), abort = one line on **stdout** then `std::abort()`.
- The abort line uses the `core/log` keys (`ts`, `level=critical`, `message`, `component=watchdog`) plus `heartbeat`, `stale_ms`, `limit_ms` and the last known state (`role`, `driver_state`, `c_s`, `c_f`, `g_s`, `g_f`, `sink_mode`). The io thread publishes the state each tick through `publish_state`; values are raw numeric codes whose meaning the host defines (5-D1).
- `stale_heartbeat()` gives `/healthz` the same rule as the abort (5-F).
- **Sink beat:** `RedisSinkOptions::loop_beat` (new, optional pointer, default null) is written by `RedisSink::run()` next to its own `heartbeat_ms_`. The storage is owned by the watchdog (`sink_beat_slot()`), so a per-leadership sink that is destroyed can never leave a dangling reader. The host must call `set_sink_registered(false)` **before** destroying the sink.
- No default threshold: `stall_ms == 0` throws. The host passes `[watchdog].process_stall_ms` (5-D1).
- Timestamp is formatted without `gmtime_r` (it takes a libc lock, unsafe while wedged). All buffers are on the stack. `snprintf` is not formally guaranteed allocation-free (glibc does not allocate for `%s` / `%llu`); plan 90 asked for a pre-formatted line, so this is a small deviation.
- **Not yet verified:** exit code 134 as PID 1 needs `tini` (5-H); asserted inside the prod image in 5-I. Nothing calls the watchdog yet (host = 5-D1), so no process is protected today.
- Tests: 14 in `tests/watchdog_test.cpp` (fake clock: fresh beats, io stall, sink stall, standby ignores sink, registration baseline, pause tolerance, fire once, schema + state, `stale_heartbeat`, sink slot, required threshold, double start; real thread: abort, quiet) and 1 in `tests/redis_sink_test.cpp` (loop beat reaches the slot). The real-thread "quiet" test uses a 5 s threshold so a loaded CI box cannot flake it; the beat-vs-stall logic is covered deterministically by the fake-clock tests.
- Review decision Q4: the planned block-info cache is **dropped** (plan 71 updated): the sweeper already reads each distinct block once per page.
- Gate: `make build` exit 0 (`-Werror`); `make test` exit 0, **381/381** (366 + 15).

### 5-B3 Ops HTTP server and 5-B4 Metrics: done (2026-10-27)
- [ops_http_server.{hpp,cpp}](../../evm_listener/src/ops/ops_http_server.hpp): `GET /healthz`, `/readyz`, `/metrics` through callbacks returning status + body + content type; query string ignored; unknown path 404; any other method 405 (`Allow: GET`). A throwing handler is logged (component `ops_http`) and answered with a generic 500 body. Per-connection read/idle deadline (`request_timeout`, default 10 s) guards against slow clients. `listen` is `host:port` with an IP literal (bracketed IPv6 allowed, port 0 = ephemeral); malformed values and null handlers throw `std::invalid_argument`. An IPv6 host binds with `v6_only(false)` (§5.2 rev 10). The caller passes `[ops].http_listen`; the host (5-D1) has not wired it yet.
- [metrics.{hpp,cpp}](../../evm_listener/src/ops/metrics.hpp): `counter_total(name, help, labels, total)` adds only the difference since the last call, so the sink / sweeper running totals do not double-count. **Deviation / decision:** a total that goes **down** (a per-leadership sink or sweeper was rebuilt and restarted at 0) is added as fresh increments, so the counter keeps rising. Gauges, name/help/type mismatches throw, invalid names throw (`std::invalid_argument` from prometheus-cpp). The class takes a mutex; it is cheap and uncontended in practice.
- Tests: 8 in `tests/ops_http_server_test.cpp` (routes, 404/405, throwing handler without leaking the text, dual-stack `127.0.0.1` and `::1` both 200, keep-alive, idle client closed, `stop()`, bad arguments) and 6 in `tests/metrics_test.cpp`. Dual-stack passed inside the dev container, so IPv6 loopback is available there.
- Test-harness note (bucket 3): a first `stop()` test hung because `io_context::poll()` with no remaining work leaves the context stopped; the test helper now calls `restart()` before driving it. No product change.
- **Not done:** `/healthz`, `/readyz`, `/metrics` callbacks are not connected to the watchdog, driver or sink yet (5-D1, 5-F). Plan §5.2 says the ops server runs on the io thread; the class does not start its own thread.
- Gate: `make build` exit 0 (`-Werror`); `make test` exit 0, **395/395** (381 + 14).

### 5-B2 Async HTTP(S) client: done (2026-10-27)
- [http_client.{hpp,cpp}](../../evm_listener/src/ops/http_client.hpp): `HttpClient(any_io_executor, HttpClientOptions{user_agent, timeout=10s, max_response_bytes=1MiB})` with `post_json(url, body, headers)` and `get(url, headers)` returning `HttpResponse{status, body}`. A non-2xx status is returned, not thrown. One connection per request (`Connection: close`), no pooling by design. TLS uses the system trust store, SNI and hostname verification.
- Errors are `rpc::TransportError` carrying only the redacted URL (`core::env::redact`); header values never appear in errors. `validate_headers` throws `std::invalid_argument` for CR/LF, bad names, and the client-owned headers Host, Content-Length, Connection, Transfer-Encoding. Reuses `rpc::parse_url` / `rpc::TransportError`.
- Test support: `MockRequest` in `tests/mock_http_server.{hpp,cpp}` gained `method` and `headers` (additive).
- Tests: 11 in `tests/http_client_test.cpp` (tag `[ops_client]`). Bucket 3 note: first build failed on `-Werror=range-loop-construct` in the test (copy in a range-for); fixed with a const reference.
- **Not verified:** the TLS success path has no unit test (no TLS mock). Only the TLS failure path (https against a plain port) is covered. Real certificate verification is first exercised by the manual alert test in 5-I.
- **Not done:** nothing uses the client yet (alerting 5-F).
- Gate: `make build` exit 0 (`-Werror`); `make test` exit 0, **406/406** (395 + 11).

### 5-C1 ProviderPool public API: done (2026-10-27)
- [provider_pool.hpp](../../evm_listener/src/provider/provider_pool.hpp) gains: `call_to(provider, RequestBuilder)` (one provider, no failover, paced, outcome folded into health via `note()` like `call()`), `pick_getlogs_excluding(set)` (thin wrapper over `ProviderBook::pick_getlogs(now, exclude)`), `report_bad(provider, reason)` (book `on_failure(Retry)`, transition reason `bad response: <reason>`, `reconcile()`), and counters `inconsistency_total(p)` / `throttled_total(p)` for the 5-F metrics (`evml_provider_inconsistency_total`, `evml_getlogs_throttled_total`).
- [token_bucket.hpp](../../evm_listener/src/provider/token_bucket.hpp): header-only, time injected. `reserve(now)` returns the wait for one token; reservations queue (empty bucket: waits of 1/rps, 2/rps, ...). One bucket per provider `Node`, built from the new `PoolConfig::reconcile_rps` / `reconcile_burst` (defaults 2 / 4, validated: rps >= 1, burst >= rps), mapped from `[redundancy]` in `make_pool_config`.
- **Decision:** `call_to` waits for its token (steady_timer) instead of failing, so the adapter (C2) never sees pacing as an error. `call()`, probes and polls are **not** paced (health signals and the existing failover path are unchanged).
- Call-site audit: no existing signature changed. `Node` ctor (private, one site), `PoolConfig` (built in `pool_config.cpp`, `tools.cpp` via `make_pool_config`, and tests with in-class defaults), `call_on` stays private.
- Unknown provider in `call_to` / `report_bad` / the counters throws `std::out_of_range` (`unordered_map::at`, message is generic; acceptable because callers pass names that came from the pool itself).
- Tests: 3 in `tests/token_bucket_test.cpp`; 4 in `tests/provider_pool_test.cpp` (call_to no failover + unknown provider, pacing waits and counts throttling, report_bad to Down + exclusion + counters, invalid pacing config); `pool_config_test` asserts the mapping.
- Bucket 3 note: a first assertion expected the `bad response: ...` transition reason on an **unstarted** pool; `emit_transition` fires only while running, so the test now asserts that no handler fires. **Gap:** the reason string itself is not covered by a test (needs a started pool with a mock WS); C3 should cover it through the adapter.
- Gate: `make build` exit 0 (`-Werror`); `make test` exit 0, **413/413** (406 + 7).
