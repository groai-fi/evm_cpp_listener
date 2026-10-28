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
| Safe Reorg | a released safe-class event vanished → revert emitted | critical | `new_reorgs, total, fork_point` |
| Sink Degraded | spill non-empty > 5 min | critical | `spill_bytes, oldest_record_age_s` |
| Sink Saturated | spill cap reached (pipeline frozen) | critical | `spill_bytes, spill_max_bytes` |
| Redis Unreachable (r6, P2) | Redis connect/PING failing > 60 s (startup wait or leader outage) | critical | `since_s, addr_family` |
| Sink Fenced (r6, P3) | a write or renew returned `FENCED`; queue and spill discarded, demoted to standby | warning | `discarded_records` |
| Sink Invariant (r6) | `emit.lua` returned `BELOW_FINALIZED` (item dropped) | critical | `new_refusals, total` |
| WS Missed Logs | a sweep found logs the WS never delivered | warning | `new_missed, total, provider` |
| Provider Inconsistency / Gate Regressed (finalized) | > 0 | critical | `reason` |
| Provider Inconsistent (rev 5, G2) | `report_bad` on a provider (bad getLogs response, or canonical-hash disagreement); rate-limited to one per provider per 15 min | warning | `provider, new_inconsistent, total` |
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

**Leader Changed source:** `lease.lua` `acquire` already reads `GET epoch` inside the script and returns `token .. '\n' .. (prev_epoch or '')` atomically. `try_acquire_lease` / `acquire_lease_blocking` return `LeaseGrant{token, epoch_n, old_host}` where `old_host` is populated iff the previous epoch's host differs from `host_id`. On leadership start, the host uses `grant.old_host` to fire `Leader Changed` and `grant.epoch_n` for the heartbeat `epoch_n` field.

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

### 5-C2 PoolLogSource: done; 5-C3 tests: mostly done (2026-10-27)
- [pool_log_source.{hpp,cpp}](../../evm_listener/src/app/pool_log_source.hpp) implements `pipeline::LogSource` over `ProviderPool` (`LogSource` and the sweeper are unchanged).
  - `get_logs`: tries the next getLogs provider via `call_to`, never one already tried in the call; `RangeTooLarge` returns at once; an undecodable answer is `report_bad` + next provider. All failing returns the last failure.
  - `report_bad(p)` → pool `report_bad` + steers the **next** `get_logs` / `current_provider()` away from `p` (spent after one get_logs). If `p` is the only usable provider it is asked again, so the sweeper ends in `Stalled` after `max_bad_attempts` instead of looping on `Retry`.
  - `block(n)`: ring hit with a timestamp = no RPC. Otherwise `eth_getBlockByNumber` on a provider other than the last logs provider; null block = skip (not reported); wrong block number = `report_bad` + next; hash differing from the ring = `report_bad` + nullopt. No independent provider = nullopt (never the logs provider).
- **Finding / deviation (plan gap):** `BlockInfo` needs a timestamp but the ring stored only `{number, hash, parent}`. Added `BlockRef::timestamp` (default 0 = unknown) and `ReorgDetector::ref_at()` (additive; `BlockRef{n, h, p}` sites still compile, one site in `reorg_detector_test.cpp`). A ring entry with timestamp 0 is cross-checked by an RPC instead of being trusted. **5-D4 must fill `timestamp` from `Head.timestamp` when feeding the ring**, otherwise the ring never saves an RPC.
- **Known limits:** (1) `logs_provider_` is the last answering provider only; in the split-by-address path earlier answering providers are not excluded from `block()`. (2) A block hash that disagrees with the ring during a real reorg books `report_bad` on an honest provider; three of those in a row take it Down (spec asks for report_bad on disagreement; revisit in D4 if it shows up).
- Tests: 14 in `tests/pool_log_source_test.cpp` (MockHttpServer, 3 providers; fall-through, RangeTooLarge, all-fail, undecodable, steering once, 3 reports to Down, single provider re-asked, `block()` independence, ring hit without RPC, ring entry without timestamp, ring disagreement, null/wrong block skipped, no independent provider, null pool).
- **C3 still open:** the "one `block(n)` call per distinct block per page" test needs the real `Sweeper` + this adapter; not written yet.
- Test-harness note: a first build failed only because shell quoting stripped the single quotes of `a` char literals while writing the test file; fixed in the file, no product change.
- Gate: `make build` exit 0 (`-Werror`); `make test` exit 0, **427/427** (413 + 14).

### 5-C3 adapter + sweeper test: done (2026-10-27)
- [pool_log_source_sweep_test.cpp](../../evm_listener/tests/pool_log_source_sweep_test.cpp): the real `Sweeper` over `PoolLogSource` and three mock HTTP providers. One page with 3 logs in 2 distinct blocks makes exactly 2 `eth_getBlockByNumber` calls (one per distinct block, not per log), none on the logs provider; 3 events emitted; `C_s` advances.
- Harness note (bucket 3): the Sweeper keys its pagers by the config getLogs provider names, so the mock providers use `cfg.getlogs_chain[i].name`, not arbitrary names.
- Gate: `make build` exit 0; `make test` exit 0, **428/428** (427 + 1).

### 5-D1 host skeleton: done (2026-10-27)
- [host.{hpp,cpp}](../../evm_listener/src/app/host.hpp): `run_host(cfg, HostOptions)` builds sink → pool → gate bridge → `PoolLogSource` → `Sweeper` → `PipelineDriver` and runs the tick loop on the calling (io) thread. `run_listener` adds log init and SIGTERM/SIGINT handlers (they only set an atomic flag). [tools.cpp](../../evm_listener/src/app/tools.cpp) `run_cli`: no action flag + `--config` now runs the listener; the three tool actions stay mutually exclusive; `user_agent()` moved out of the anonymous namespace so the host reuses it.
- Exit codes: 0 graceful stop; 1 sink `Failed`, Redis connection or pipeline-start error; 2 driver `Failed`, Redis version/eviction check failed, or `[lease] enabled = true` (refused until 5-D2). Shutdown order: stop the watchdog, stop the pool, `sink.drain(25 s)` on the main thread, exit.
- Ops routes (minimal): `/healthz` 503 when `watchdog.stale_heartbeat()` is non-null; `/readyz` 200 only when the driver is `live` (JSON with state, role, `"lease":"disabled"`); `/metrics` serialises the registry (empty until 5-F).
- **Deviations from §5.H / plan 90:**
  1. **Gate bridge pulled into D1** (user decision): `PoolHandlers::on_tag` (pool test added, see below), `FinalityGate` → `driver.set_gates`, `SafeRegression`/`SafeHashMismatch` → `on_safe_reorg_signal`, `FinalizedAnomaly` → `on_gate_frozen`. Only the **current getLogs provider's** tags feed the gate (the WS cross-check also reads tags). D3 shrinks to extra tests.
  2. **Startup thread for the Redis wait.** `await_redis_ready` blocks; on the io thread it would starve `/healthz` and the io beat and the watchdog would abort after 30 s. A short-lived thread runs it and posts `start_runtime` to the io thread. Thread count: io + sink + watchdog (+ the startup thread until Redis is ready).
  3. **io beat is a dedicated timer** (`beat_period`, default 1 s) that calls `beat_io`, `publish_state`, polls the stop flag and the sink mode. One tick can legitimately take many seconds, so "once per tick" would give false aborts. The stop flag is polled, not signal-driven, so shutdown starts within one beat.
  4. `[lease] enabled = true` exits 2 with a message until 5-D2. `base.toml` ships with the lease enabled, so a prod start needs D2.
  5. Cold start has no priming read: the pool's first `safe` poll comes after `safe_poll_ms` and the first `finalized` after `finalized_poll_ms` (4 s / 12 s in `base.toml`), so Live is delayed by up to ~12 s. Tests use 100 ms polls.
  6. `PoolLogSource` gets no ring yet (`nullptr`); heads → `ReorgDetector` is 5-D4 (which must also fill `BlockRef::timestamp`).
  7. Driver and sweeper clock = wall clock (`system_clock`) ms.
- `EVML_TEST_HOOKS` (CMake option, default ON for Debug, OFF for Release/prod image) compiles `--test-freeze-io-after <s>`; the io thread then blocks and the watchdog aborts.
- `PoolHandlers::on_tag` ([provider_pool.hpp](../../evm_listener/src/provider/provider_pool.hpp)): fires after every successful `read_tag` while the pool is running; `Tag` enum moved above `PoolHandlers`. Test: `pool: on_tag fires with provider/tag/number only while the pool is running`.
- Tests ([host_test.cpp](../../evm_listener/tests/host_test.cpp)): lease-enabled refusal (unit); in-process cold start against compose Redis + mock node reaches Live (c_s 52060300, c_f 52060250), writes the initial checkpoint, and a stop request exits 0 with the sink drained; child-process freeze test: the real binary with `--test-freeze-io-after 1` dies by SIGABRT and its log names `io_tick`.
- Caveats: exit **134 as PID 1 is still unverified** until 5-I (needs `tini`, slice 5-H); the test asserts SIGABRT from the child's wait status, which a shell reports as 134. Redis URL errors from `await_redis_ready` are logged as given by the library (no URL is added by the host).
- Gate: `make build` exit 0 (`-Werror`); `make test` exit 0, **432/432** (428 + 1 on_tag + 3 host). First host run failed only because the test did not call `log::init` (bucket 3, test harness); fixed in the test.

### 5-D2 lease and standby: done (2026-10-27)
- [host.cpp](../../evm_listener/src/app/host.cpp): with `[lease] enabled = true` the host starts in `standby`. An acquire worker thread (the former startup thread, relaunched per term) runs `sink::acquire_lease_blocking` (poll every `renew_ms`, sliced sleep, stop-aware) and posts `start_runtime(report, token)` to the io thread. Each term builds a fresh sink, pool, gate bridge, source, sweeper and driver (§5.H). Lease disabled keeps the D1 path (`lease_disabled_token()`).
- Demotion: the io beat sees `SinkMode::Fenced` → `demote()` (`driver.request_drain()`, cancel the tick sleep) → `pipeline_loop` exits → `end_term()`: unregister the sink beat, stop the pool, destroy the runtime (joins the sink thread), back to `standby`, relaunch the acquire worker. Sink `Failed` still exits 1 (2 for driver `Failed`).
- `/readyz`: 200 when `live` **or** a healthy `standby` (§5.1), 503 otherwise; JSON has `"lease":"enabled|disabled"`.
- `HostOptions::on_event` (`"leader"`, `"demoted"`) is called on the io thread; used by tests, later usable by 5-E.
- **Deviations:**
  1. **`epoch` read before acquire (Leader Changed) deferred to 5-E.** Its only consumer is the alert engine, and `RedisConn` has no generic GET, so it would be dead code now.
  2. `end_term()` joins the sink thread on the io thread; bounded by the 2 s Redis timeout, far below the 30 s watchdog.
  3. The D1 test "`[lease] enabled = true` is refused" was deleted (bucket 2, spec evolution: the host now accepts the lease).
- Tests ([host_test.cpp](../../evm_listener/tests/host_test.cpp)): `RunningHost` helper (one host per thread). (a) Takeover: hosts A and B on one shard (ttl 3 s, renew 0.5 s); B stays standby while A leads and `leader` names hostA; after A stops, B goes Live within 15 s, the checkpoint does not regress, both exit 0. (b) Fenced demotion: `DEL leader` + acquire as `intruder` → exactly one `demoted` event, then a second `leader` term, Live again after the intruder's lease expires, exit 0.
- Gate: `make build` exit 0; `make test` exit 0, **433/433** (432 − 1 refusal + 2 lease tests).

### 5-D3 gate bridge host tests: done (2026-10-27)
- Tests only (no production change). [host_test.cpp](../../evm_listener/tests/host_test.cpp) gains `NodeState` / `tunable_node` (tags and failure switchable between `run_until` calls; counts `eth_getLogs`) and `RunningHost::status()` (last `DriverStatus` under a mutex).
  1. **Finalized regression:** finalized tag drops 250 → 100 blocks above start → `gate` logs `finalized anomaly`, `status().frozen`, reason contains `finalized regressed`, cursors do not move, and a rising safe gate causes no further `eth_getLogs`.
  2. **Safe regression:** safe tag 300 → 290 → `eth_getLogs` calls resume with no other trigger (quiet baseline checked first), `reverify_pending` clears, Live again, `safe_reorgs == 0` (the mock returns identical logs), not frozen.
  3. **Failover to lower tags:** all env providers on node A, `base_public` (4th in `getlogs_order`) on node B with lower tags. A dies → gates move to B's values (g_s 280, g_f 230), not frozen, no reverify, still Live, exit 0.
- **Finding (bucket 3, my test premise):** the first version of test 3 assumed `infura` leads. With WS unreachable the pool marks providers Down in order and the getLogs provider ends up `ankr` (a safe regression in test 2 was also logged for `ankr`). Fixed by using the last provider with an HTTP-only URL (`base_public`) as the lagging node.
- **Observation, not changed:** `FinalityGate` tracks regressions per provider, but `latest_safe_` / `latest_finalized_` are global, so after a failover the gates (and `set_gates`) move **down** to the new provider's tags. The released window and the sink's `ckpt.finalized` guard keep that harmless for emitted data; the cursors simply wait. Not a bug under the plan, recorded for the 5-E alert design.
- Gate: `make build` exit 0; `make test` exit 0, **436/436** (433 + 3).

### 5-D4 heads → ReorgDetector: done (2026-10-27)
- [host.cpp](../../evm_listener/src/app/host.cpp): each term's `Runtime` owns a `ReorgDetector ring` (`[chain].hash_ring_size`), declared before `PoolLogSource`, which now gets `&ring` instead of `nullptr`. `PoolHandlers::on_head` feeds it with `BlockRef{number, hash, parent_hash, timestamp}`; **the timestamp is filled** (the 5-C gap), so `block(n)` is served from the ring and asks a provider only on a miss.
- Flow (all on the io thread): `Seeded/Extended/Duplicate` → nothing. `NeedWalk` → `walk_fork` coroutine fetches missing ancestors with `eth_getBlockByNumber` via `call_to` on the provider that sent the head, until the walk reaches the ring, then `ring.apply()`. If `depth > 0` and `fork_point < C_s` → `driver.on_safe_reorg_signal`. Heads that arrive during a walk are deferred (only the newest is kept), because `apply()` throws if the ring changed.
- `on_ws_active_changed` → `ring.reset()` (plan 3.6, re-seed from the new WS provider) and a `head_epoch_` bump that makes any walk in flight exit without touching state. The epoch is also bumped at term start and in `end_term()` before `rt_.reset()`, so a walk waiting on a provider can never touch a destroyed runtime.
- **Deviation (limits, not in the plan):** a walk fetches at most `kMaxWalkBlocks = 16` blocks, because `call_to` is paced at `reconcile_rps` and the same bucket serves `block()`. A head more than 16 blocks above the ring top is not walked. Any case where the ring cannot decide (gap > 16, fork deeper than the walk or the ring, provider error / null block / bad JSON, `Inconsistent` three times) **resyncs the ring from the new head and signals a re-verification**: a missed reorg is never silent, at the cost of one `eth_getLogs` pass over (C_f, C_s].
- Tests ([host_test.cpp](../../evm_listener/tests/host_test.cpp); new `HeadRig` = host + mock WS node with queued `newHeads` + tunable HTTP node with alternate-branch blocks): (1) fork off c0+298 below C_s=c0+300 → exactly blocks 309..299 are fetched, `eth_getLogs` resumes, reverify clears; (2) fork off c0+308 above C_s → only block 309 fetched, no re-verification; (3) a 17-block head gap → no walk, ring resync, re-verification.
- **Not covered by a test:** `block()` actually being served from the ring with a real log (covered at unit level in 5-C, `pool_log_source_test`); walk failure paths other than the gap limit; ring reset on a WS failover inside the host.
- Gate: `make build` exit 0; `make test` exit 0, **439/439** (436 + 3).

### 5-D5 WS cross-check (G3): done (2026-10-27)
- **Deviation (user-approved, option A1):** the sweeper kept each fetched page private, so plan 90's "stop and report" rule applied. I stopped, the user approved one additive hook: `SweeperOptions::on_page(Finality kind, const BlockRange&, const std::vector<rpc::Log>&)` in [sweeper.hpp](../../evm_listener/src/pipeline/sweeper.hpp). Default empty = no behaviour change. It is called once per `Advanced` page, after emissions, checkpoint and cursor update, in `safe_step` and `final_step`. Not called for Retry/Paused/NoWork/Stalled or `reverify_released`. No other sweeper line changed; `LogSource` is untouched.
- **Class scoping:** the sweeper passes only in-scope logs: safe-class for a safe page, finalized-class for a final page. The host applies the same class to the staged side (`cfg.route(...)->finality`). Without this, the final sweep would count already-reconciled safe logs as `ws_missed`.
- [host.cpp](../../evm_listener/src/app/host.cpp): `Runtime` owns `PendingBuffer pending(cfg.chain.pending_max_logs)` (per term). `PoolHandlers::on_log` → `on_ws_log` stages logs (io thread; logs matching no configured route are warned and dropped). `on_sweep_page` calls `pending.reconcile(from, to, logs, in_scope)`, adds to `ws_missed_total_` / `ws_extra_total_`, and logs a warn on any mismatch. A resolved head fork (`depth > 0`) calls `purge_from(fork_point + 1)`; a ring resync calls `purge_from(0)`. A WS provider change keeps staged logs.
- `HostOptions::on_cross_check(missed_total, extra_total)` (io thread, per page) exposes the totals to tests. Prometheus export (`ws_missed_logs_total`, `ws_extra_logs_total`, evictions) and alerts stay in 5-F / 5-E.
- Tests: [sweeper_test.cpp](../../evm_listener/tests/sweeper_test.cpp) (hook fires once per advanced page with the in-scope logs; not on NoWork/Paused/Retry); [host_test.cpp](../../evm_listener/tests/host_test.cpp) with a WS log at c0+320 and gates moved past it: WS + sweep agree → 0/0; sweep only → `ws_missed` 1; WS only → `ws_extra` 1. The mock node now serves `eth_getLogs` by block range.
- **Not covered by a test:** fork purge and resync purge at host level; eviction at the 200k cap; staged logs surviving a WS failover.
- Gate: `make build` exit 0; `make test` exit 0, **443/443** (439 + 4).

### 5-D6 Heartbeat payload: done (2026-10-27)
- [host.cpp](../../evm_listener/src/app/host.cpp): `RedisSinkOptions` now gets `host_id = [shard].host_id`, `heartbeat_ms = [ops].heartbeat_s * 1000` and `heartbeat_ttl_s = max(30, 3 * heartbeat_s)` (the sink rejects a TTL <= the interval, so a large `heartbeat_s` would otherwise fail startup). `Host::publish_heartbeat` runs from the io beat, throttled to `heartbeat_s`, builds the plan 4.7 JSON and calls `sink.set_heartbeat`; the sink thread writes `SET hb:<host_id> <payload> EX ttl` on the lease connection.
- Payload: `ts` (wall ms), `role` (`"leader"`), `state`, `head` (top of the head ring), `G_s`, `G_f`, `C_s`, `C_f`, `active_provider` (current getLogs provider), `version`. A value that is not known yet is JSON `null`, never 0.
- **Deviation:** `epoch_n` (plan 4.7) is **left out**: the sink has no epoch read, and a placeholder would be a made-up value. It comes with the 5-E epoch read.
- **Limits:** only a leader writes a heartbeat (a standby has no sink, so no `hb:` key); the first key appears up to `heartbeat_s` after the term starts if the sink loop passed its first deadline before the first payload existed; with `heartbeat_s = 10` the payload can be up to ~10 s old when written.
- Test: [host_test.cpp](../../evm_listener/tests/host_test.cpp) reads `hb:test-host` from Redis (role, state, head = c0+310, G/C values, provider, version, no `epoch_n`, fresh `ts`, TTL in (0, 30]). The shared test config now uses `heartbeat_s = 1` so the key shows up quickly.
- Gate: `make build` exit 0; `make test` exit 0, **444/444** (443 + 1).

### 5-E1 Alert rule engine: done (2026-10-27)
- New [alerts.hpp](../../evm_listener/src/ops/alerts.hpp) / [alerts.cpp](../../evm_listener/src/ops/alerts.cpp), pure (no I/O, no clock, no threads). The host will fill an `OpsSnapshot` every 10 s and call `AlertEngine::evaluate()`; the result is the list of alerts to send (E2 sends them, E5-wiring comes with the host integration).
- **Conditions:** one fire, a re-notify every 15 min while true, one `RESOLVED: <title>` (severity info) on clear. **Events:** one alert per occurrence, no resolve; events that name a provider or group (Provider Inconsistent, Consumer Entries Trimmed) are limited to one per 15 min per name; events without a name (Provider Failover, Safe Reorg, Sink Invariant, WS Missed Logs) are not limited, so every Safe Reorg pages.
- **Windows are tracked inside the engine** from successive snapshots (gate unchanged for N s, condition true for N s, late-log samples in a sliding hour), so the host only reports current values. All 25 rows of §5.3.3 are implemented; thresholds come from `AlertThresholds::from_config` ([watchdog] `head_stall_ms`, `safe_gate_stuck_s`, `progress_stall_ms`; [sink] `consumer_lag_*`) plus the fixed numbers of the table (60 min finalized gate, 300 / 2000 blocks for 15 min, 30 blocks for 2 min, 5 min spill, 60 s Redis, more than 3 late logs per hour, 10 min missing group).
- **D7 ownership:** pipeline, provider, sink and consumer rows are leader-only. Redis Unreachable, Redis Misconfigured, Sink Fenced and Leader Changed work in any role. A host that stops leading drops its leader conditions **without** a resolve message (the new leader owns the state) and restarts their trackers. A consumer-lag snapshot older than 120 s (2 x the 60 s probe) is "unknown": the rule neither fires nor resolves.
- **Decisions / deviations to know:**
  - `Pipeline Stalled` fires when a cursor stays behind its ceiling for `3 x progress_stall_ms`, or when the host reports a `Stalled` sweep (`stalled_detail`).
  - "Rate-limited per group" for the four consumer rows is the per-rule 15 min re-notify (each group has its own rule instances); there is no extra cross-rule limiter, so a lagging-to-near-trim escalation inside 15 min is **not** suppressed.
  - Provider Quota Exhausted does not re-notify (once per incident); it is critical when `any_healthy_provider` is false.
  - The finalized-gate alert carries only `reason` in `details` (the gate keeps no block/hash fields); the plan lists `provider, block, expected_hash, got_hash`.
  - `Consumer Entries Trimmed` is derived by the engine as an edge (false to true) of `trimmed_unread`.
- Not done here (next slices): building the snapshot from the live host (E-wiring), sending (E2), dead-man (E3), the `epoch` read for Leader Changed, and the plan-4.7 `epoch_n` heartbeat field.
- Tests: 16 in [alerts_test.cpp](../../evm_listener/tests/alerts_test.cpp): healthy leader silent; fire / no repeat / re-notify at 900 s / resolve; standby vs leader-only and no stray resolve; each condition row with its boundary (exactly the limit does not fire); events (once, per-name limit, severities); thresholds from config.
- **Test fix (bucket 3, harness race):** the full run once failed `redis sink integration 18` at [redis_sink_integration_test.cpp:553](../../evm_listener/tests/redis_sink_integration_test.cpp#L553): it sampled `mode()` once after `wait_idle` and saw `Replaying`; the sink log shows "back to connected" 9 ms later. It passes alone. The check now waits up to 5 s for `Connected` (still required, not weakened). No production code changed.
- Gate: `make build` exit 0; `make test` exit 0, **460/460** (444 + 16).

### 5-E2 Alert sender: done (2026-10-27)
- New [alert_sender.hpp](../../evm_listener/src/ops/alert_sender.hpp) / [alert_sender.cpp](../../evm_listener/src/ops/alert_sender.cpp). `AlertSender::send(const Alert&)` is fire-and-forget: each alert is delivered by its own detached coroutine on the host executor, so the io thread never blocks. State (options, `HttpClient`, atomic counters) lives in a `shared_ptr` held by every coroutine.
- **Payload** (§5.3.1): `{severity, service, shard, host, title, message, ts, details}`; `ts` is UTC `YYYY-MM-DDTHH:MM:SSZ`; `details` is passed through. Headers: `Content-Type: application/json`, `X-API-Key: <token>`. A resolve goes out as severity `info` with the `RESOLVED:` title the engine already sets.
- **Retry:** a transport error or a non-2xx answer is retried `retries = 3` times (so at most **4 attempts**, the literal reading of "retried 3x") with full-jitter `rpc::Backoff` (default 1 s base, 30 s cap). Then one ERROR is logged and `send_failures_total()` is incremented. Delivery failure never throws out of `send()` and never touches `/readyz`.
- **Secrets:** URL, token and body are never logged. Log fields are the alert title, attempt number, and either `http status N` or the `TransportError` text (redacted URL only). Constructor throws `std::invalid_argument` on an empty URL or token (fail fast).
- **Decisions:** optional injectable `spdlog::logger` (null = global logger) so a test can capture logs and prove no secret appears; counters are exposed as `sent_total()` / `send_failures_total()`. Exporting `evml_alert_send_failures_total` as a Prometheus metric is 5-F.
- **Limits:** no queue and no ordering guarantee between alerts (each retries independently); an alert in flight during shutdown is lost; the TLS success path is still only covered by the manual alert test in 5-I. Env resolution (`N8N_ALERT_WEBHOOK_URL` / `_TOKEN`) is host wiring in E4.
- Tests: 6 in [alert_sender_test.cpp](../../evm_listener/tests/alert_sender_test.cpp): schema + headers + method + ts format; resolve as info; 500, 500, 200 gives 3 requests and 0 failures; always 503 gives 4 requests, 1 failure, 1 ERROR, no token / path / URL in logs; connection refused is counted and leaks nothing; constructor validation.
- Gate: `make build` exit 0; `make test` exit 0, **466/466** (460 + 6).

### 5-E3 Dead-man pinger: done (2026-10-27)
- New [deadman.hpp](../../evm_listener/src/ops/deadman.hpp) / [deadman.cpp](../../evm_listener/src/ops/deadman.cpp). The host calls `Deadman::tick(DeadmanFacts)` at the ping cadence (`[ops] deadman_ping_s`); it returns true when it started a ping. The ping is an async `GET` on the io thread through `HttpClient` (10 s timeout, the configured User-Agent).
- **Healthy** (§5.3.2) = `leader` AND `state_ok` (CatchingUp or Live, computed by the host) AND NOT `sink_saturated` AND NOT (C_s unchanged for more than 10 min AND G_s changed within the last 10 min). So a quiet chain (neither moved) keeps pinging; a stalled checkpoint while the gate moves stops it. The two windows are tracked inside `Deadman` from the facts of each tick; a standby never pings and loses its windows, so a new leader starts with fresh ones.
- **Failure** (transport error or non-2xx) increments `ping_failures_total()` (`evml_deadman_ping_failures_total`, exported in 5-F) and logs one WARN with the HTTP status or the redacted `TransportError` text. The URL is never logged. Never fatal.
- **Empty URL:** `enabled()` is false, `status()` is `"disabled"` (for the `/readyz` JSON in 5-F), a WARN is logged at construction (startup), and `tick()` never pings.
- **Decisions:** at most one ping in flight (a tick while one runs returns false; the 10 s timeout bounds it); "advanced" is implemented as "changed" (cursors only move forward); optional injectable logger as in E2.
- **Limits:** the windows only see values at tick cadence, so the 10 min boundary is accurate to one `deadman_ping_s`; env resolution of `DEADMAN_PING_URL`, the `state_ok` / `sink_saturated` facts, and the timer are host wiring (E4).
- Tests: 9 in [deadman_test.cpp](../../evm_listener/tests/deadman_test.cpp): healthy leader sends GET with User-Agent and path; standby / bad state / saturated send nothing; C_s stalled 11 min while G_s moves stops pings, exactly 600 s still pings, a quiet chain pings; C_s advancing restores pings; losing leadership restarts the windows; non-2xx and connection refused counted with no URL in logs; one ping in flight; empty URL disabled with WARN.
- Gate: `make build` exit 0; `make test` exit 0, **475/475** (466 + 9).

### 5-E4a Host wiring of alerts and dead-man: done (2026-10-28)
- **Split:** plan 90 E4 is split. E4a (this slice) wires what has a source today; E4b adds the missing facts and the per-row tests (see Limits).
- New [snapshot.hpp](../../evm_listener/src/ops/snapshot.hpp) / [snapshot.cpp](../../evm_listener/src/ops/snapshot.cpp): `SnapshotBuilder::build(SnapshotInputs)` is pure (host passes copies). It copies driver cursors, gates, `frozen`, `stalled_detail` (only while `stalled`), head, sink saturation, spill bytes and cap, late-log total, and converts consumer-lag probes (`sampled_at` = now minus the probe's age; never-probed groups skipped; `lag` nil becomes -1). Counter increases since the previous leader build become events: `safe_reorgs` → Safe Reorg (with `fork_point`), `below_finalized_total` → Sink Invariant, `ws_missed_total` → WS Missed Logs. The first leader build only sets the baseline, and the baseline is dropped while not leader (counters restart per term). `push_event()` queues host-local events for the next build.
- **Host** ([host.cpp](../../evm_listener/src/app/host.cpp)): owns `AlertEngine` (thresholds from config), `SnapshotBuilder`, optional `AlertSender`, `Deadman`. `ops_tick()` runs from `on_beat()` on the io thread: every 10 s (`kOpsPeriod`) build, evaluate, log each alert at WARN (`alert raised`: severity, title, message, no secrets) and send it; every `[ops] deadman_ping_s` feed the dead-man (leader, state CatchingUp or Live, sink not saturated, `C_s`, `G_s`). `demote()` queues the Sink Fenced event (message = why, `discarded_records`).
- **Env (no secrets in code):** `N8N_ALERT_WEBHOOK_URL` / `N8N_ALERT_WEBHOOK_TOKEN` and `DEADMAN_PING_URL` are read through the config's env-name fields. Decision: the webhook is **optional**: if either is empty the host logs a WARN at startup and alerts are only logged (local dev works). The dead-man warns by itself when its URL is empty. Both are already in `.env.example`.
- **Limits (E4b):** these facts have no source yet, so their rules stay silent: provider auth / quota facts and `any_healthy_provider` (default true), Provider Failover and Provider Inconsistent events, `ms_since_head`, `getlogs_safe` / `ws_safe`, `oldest_record_age_s`, Redis unreachable / misconfigured, Leader Changed (needs the `epoch` read). The first evaluation runs at the first beat of a term, so a counter that is already non-zero then is not reported. A partly set webhook config (URL without token) also falls back to log-only. `evml_alerts_fired_total` and the failure counters are exported in 5-F.
- Tests: 7 in [snapshot_test.cpp](../../evm_listener/tests/snapshot_test.cpp) (copy of leader facts, stalled detail, standby with queued event delivered once, counter-to-event rules with baseline, term restart, consumer conversion, builder plus engine end to end); 1 host test in [host_test.cpp](../../evm_listener/tests/host_test.cpp): a Live leader pings the mock dead-man URL (GET, right path), a healthy leader posts nothing, then a finalized regression freezes release and the mock webhook receives one critical POST (`Gate Regressed`, shard, host, `X-API-Key`) at the next 10 s evaluation. The test takes about 10 s because of that cadence.
- Gate: `make build` exit 0; `make test` exit 0, **483/483** (475 + 7 + 1).

### 5-E4b Remaining alert facts, atomic LeaseGrant, StaleDetector split, and switch_back: done (2026-10-28)
- **Provider facts and `Provider Failover` / `switch_back`:**
  - `ProviderPool` exposes `health()` (per provider: state, the last quota / auth fault with its HTTP status or RPC code, time left until the re-probe), `ms_since_head()`, and the cached `safe` tag per provider (`cached_tag(provider, Tag::Safe)`, used for `ws_safe` / `getlogs_safe`). They are fed into `SnapshotBuilder` (`providers`, `ms_since_head`, `getlogs_safe`, `ws_safe`, and the `inconsistency_total` counter-delta → `Provider Inconsistent`); `any_healthy_provider` is computed there (any provider `Up`). `WsSession` propagates HTTP status on upgrade rejection so 401/403 triggers `Provider Auth Failure`.
  - [provider_book.hpp](../../evm_listener/src/provider/provider_book.hpp) / [provider_book.cpp](../../evm_listener/src/provider/provider_book.cpp): `ProviderBook::WsDecision` carries `std::optional<std::string> from` across all 5 decision paths in `select_ws()`, and `PoolHandlers::on_ws_active_changed(from, to, reason)` passes the complete transition edge without host-side state tracking.
  - [host.cpp](../../evm_listener/src/app/host.cpp) logs `INFO` (`ws active provider changed`) on every active WS change (`from`, `to`, `reason`) and queues `ops::provider_failover_event(from, *to, reason)` on both `reason == "failover"` (`"the WS provider failed over from <from> to <to>"`) and `reason == "switch_back"` (`"the WS provider switched back from <from> to <to>"`).
- **Head-silence vs session-stall split (`StaleDetector`):**
  - [stale_detector.hpp](../../evm_listener/src/provider/stale_detector.hpp) / [stale_detector.cpp](../../evm_listener/src/provider/stale_detector.cpp) splits `session_baseline_` (reset by `reset_ws(now)` and `on_ws_head()`, used by `ws_head_stalled(now)` for the 15 s per-session stall timeout) from `last_head_at_` (initialized at start, updated **only** in `on_ws_head()`, never reset by `reset_ws(now)`, used by `ws_head_silence(now)` for the 30 s pool-wide `WebSocket Stalled` alert).
  - `ProviderPool::ms_since_head()` stays active when `active_ws()` is empty (`provider = "none"` in the snapshot), so all WS providers going `Down` or reconnecting without delivering heads never resets or falsely resolves `WebSocket Stalled`.
- **Sink `oldest_record_age_s` and Redis unreachable / misconfigured facts:**
  - [redis_sink.cpp](../../evm_listener/src/sink/redis_sink.cpp) stamps every accepted write (`steady_now()`) in queue/spill/held-batch order and exposes `SinkStatus::oldest_record_age_s` for `Sink Degraded`.
  - [redis_startup.cpp](../../evm_listener/src/sink/redis_startup.cpp) / [host.cpp](../../evm_listener/src/app/host.cpp) track Redis unreachable duration (`redis_unreachable_s`, `redis_addr_family`) during startup wait and leader outage, and feed startup `RedisMisconfigured` (`maxmemory_policy`, `appendonly`, `redis_version`) into the snapshot.
- **Atomic `LeaseGrant` (`lease.lua` + `sink/lease`):**
  - [lease.lua](../../evm_listener/src/sink/lua/lease.lua) `acquire` returns `token .. '\n' .. (e or '')` atomically. [lease.hpp](../../evm_listener/src/sink/lease.hpp) / [lease.cpp](../../evm_listener/src/sink/lease.cpp) `try_acquire_lease` and `acquire_lease_blocking` return `std::optional<LeaseGrant>` (`{token, epoch_n, old_host}`, with `old_host` populated only when the previous epoch's host differs from `host_id`).
  - Closes the 5-D2 and 5-D6 deferrals: `start_runtime` takes `const sink::LeaseGrant&`, pushes `leader_changed_event(*grant.old_host, host_id)` on takeover, and writes `epoch_n` into the Redis heartbeat payload (`null` when lease is disabled).
- Gate: `make build` exit 0; `make test` exit 0, **504/504** (483 + 21).

### 5-E4c §5.3.3 contract tests and Stage-1 review fixes: done (2026-10-28)
- **What was already covered:** the transport (schema, `X-API-Key`, 1 + 3 attempts, failure counter, no secret in logs: [alert_sender_test.cpp](../../evm_listener/tests/alert_sender_test.cpp)), the dead-man leader / standby / saturated cases ([deadman_test.cpp](../../evm_listener/tests/deadman_test.cpp)), and one test per rule family in [alerts_test.cpp](../../evm_listener/tests/alerts_test.cpp). E4c adds the missing piece: the whole §5.3.3 table checked row by row.
- New [alert_contract_test.cpp](../../evm_listener/tests/alert_contract_test.cpp): one table row per §5.3.3 row (25). Every row (condition and event alike) is driven through `SnapshotBuilder::build(SnapshotInputs)` and `AlertEngine::evaluate` (plus the event helpers for host-queued events) and must give exactly one alert with the table's title and severity, then on the next snapshot: a **condition** resolves exactly once (`RESOLVED: <title>`), an **event** neither repeats nor resolves. Every row asserts 100% of its §5.3.3 `details` keys directly with non-empty values.
- **Stage-1 review fixes (N-1, N-2, P1-3):**
  - **N-1 (`SnapshotBuilder::build` missing condition / `Consumer Entries Trimmed` details):** `sink::ConsumerLag` ([consumer_lag.hpp](../../evm_listener/src/sink/consumer_lag.hpp) / [consumer_lag.cpp](../../evm_listener/src/sink/consumer_lag.cpp)) now records `last_delivered_id` and `max_deleted_id`; `SweepStats` ([sweeper.hpp](../../evm_listener/src/pipeline/sweeper.hpp) / [sweeper.cpp](../../evm_listener/src/pipeline/sweeper.cpp)) records `last_late_block` in `final_step()`; `SnapshotBuilder::build()` populates `s.stalled_provider`, `s.safe_late_provider`, `s.safe_late_last_block`, and `f.last_delivered_id` / `f.max_deleted_id` from `SnapshotInputs`.
  - **N-2 (`Redis Unreachable` in standby + `addr_family` on leader outage):** `StartupReport` ([redis_startup.hpp](../../evm_listener/src/sink/redis_startup.hpp) / [redis_startup.cpp](../../evm_listener/src/sink/redis_startup.cpp)) always populates `address_family` on first connection; `AcquireOptions` ([lease.hpp](../../evm_listener/src/sink/lease.hpp) / [lease.cpp](../../evm_listener/src/sink/lease.cpp)) adds `on_waiting(error, waited)` and `on_reachable()` callbacks wired into `Host::launch_acquire()` so standby Redis outages update `redis_wait_since_ns_` and leader outages always report the resolved `addr_family`.
  - **P1-3 (`WS Missed Logs` rate limit):** `SnapshotBuilder::build()` sets `e.subject = in.ws_provider.empty() ? "ws" : in.ws_provider` on `WsMissedLogs` so `AlertEngine` rate-limits it to once per 15 min per provider.
- **Refactor (call sites updated):** the Sink Fenced event is built by `ops::sink_fenced_event()` in [snapshot.cpp](../../evm_listener/src/ops/snapshot.cpp) (the only caller is `Host::demote`), so it can be tested without a host.
- **Spec alignment:** `SnapshotBuilder::build()` includes `provider` (`in.ws_provider`) in `WS Missed Logs` details alongside `new_missed` and `total`, and the §5.3.3 `details` column for the 6 counter-delta / summary rows (`Safe Reorg`, `Sink Fenced`, `Sink Invariant`, `WS Missed Logs`, `Provider Inconsistency / Gate Regressed (finalized)`, `Provider Inconsistent`) is aligned with the counter-delta design.
- **Host check (one occurrence, one POST):** the switch_back host test now also counts the Provider Failover POSTs with reason `failover` before the switch back arrives, about 60 s and six evaluations later: exactly 1.
- **Not changed:** after every WS provider has gone `Down`, the pool reports the next active provider with reason `initial`, so that recovery raises no Provider Failover alert.
- Gate: `make build` exit 0; `make test` exit 0, **505/505** (504 + 1).

### 5-F Metrics and `/readyz` wiring: done (2026-10-28)
- **Metrics ([host.hpp](../../evm_listener/src/app/host.hpp) / [host.cpp](../../evm_listener/src/app/host.cpp), [pipeline_driver.hpp](../../evm_listener/src/pipeline/pipeline_driver.hpp) / [pipeline_driver.cpp](../../evm_listener/src/pipeline/pipeline_driver.cpp)):**
  - `Host::init_metrics()` pre-registers all §5.4 metrics (`evml_role{role="leader"|"standby"}`, `evml_lease_renew_failures_total`, `evml_provider_active{provider}`, `evml_provider_failovers_total`, `evml_alerts_fired_total{alert}`, `evml_provider_inconsistency_total{provider}`, `evml_safe_missed_total`, `evml_reverify_total{result="clean"|"reorg"}`, `evml_getlogs_throttled_total{provider}`, `evml_consumer_undelivered_age_seconds{stream,group}`, `evml_consumer_pending_age_seconds{stream,group}`, `evml_consumer_lag_entries{stream,group}`, `evml_spill_discarded_records_total`) plus `evml_alert_send_failures_total`, `evml_deadman_ping_failures_total`, WS cross-check counters (`evml_ws_missed_logs_total`, `evml_ws_extra_logs_total`, `evml_pending_evicted_logs_total`), and existing sink, sweeper, and pipeline (`evml_head_block`, `evml_gate_block`, `evml_checkpoint_block`) metrics.
  - `PipelineDriver` tracks `reverify_clean` alongside `safe_reorgs` so `evml_reverify_total{result="clean"|"reorg"}` is exact.
  - `Host::update_metrics()` runs every 10 s (`ops_tick`), on demotion (`demote` and `end_term`), and on every `/metrics` scrape via `Metrics::counter_total` / `Metrics::gauge`, flushing monotonic counters before runtime teardown and resetting leader-only gauges to `0` on standby so per-leadership runtime restarts never drop or double-count monotonic totals or leave stale gauges.
- **`/readyz` and `/healthz` ([host.cpp](../../evm_listener/src/app/host.cpp)):**
  - `/healthz` returns 200 (`ok\n`) iff `Watchdog::stale_heartbeat()` is empty, else 503.
  - `/readyz` returns 200 iff `(leader && state == "live" && !saturated && gate_age_ok) || (phase == Standby && redis_wait_since_ns == 0)`, where `gate_age_ok` enforces D6 (`G_s` advanced within `watchdog.safe_gate_stuck_s`). JSON body carries `state`, `role`, `lease` (`"enabled"` / `"disabled"`), `deadman` (`"enabled"` / `"disabled"`), `consumer_lag` (`"enabled"` / `"disabled"`), `saturated`, `gates.{safe,finalized}`, and `cursors.{safe,finalized}` (with JSON `null` when unknown / standby).
  - `HostOptions::on_ops_port` exposes the bound ephemeral port to tests.
- **Tests ([host_test.cpp](../../evm_listener/tests/host_test.cpp)):**
  - `host: /metrics exposes every §5.4 and sink/sweeper metric and updates on scrape`: verifies HTTP 200 and `# TYPE <name>` for all 41 registered metric families plus `evml_role` and `evml_checkpoint_block` values.
  - `host: /readyz returns 200 for leader-live, standby and lease-disabled, and 503 when saturated or gate is stuck`: verifies 200 and JSON schema for leader-live (`lease: "disabled"`), 503 after `safe_gate_stuck_s` elapses without `G_s` advancing and recovery to 200 when `G_s` advances (D6), 200 for a healthy standby (`role: "standby"`, `lease: "enabled"`), and 503 (`saturated: true`) when Redis is disabled via `ToxiRedis` and queue+spill saturate.
- Gate: `make build` exit 0 (`-Werror`); `make test` exit 0, **507/507** (505 + 2).

### 5-G Operator tools (`--check-redis` and `--backfill-contract <name>`): done (2026-10-28)
- **`--check-redis` ([tools.hpp](../../evm_listener/src/app/tools.hpp) / [tools.cpp](../../evm_listener/src/app/tools.cpp)):**
  - `check_redis(cfg)` runs a single-shot `sink::await_redis_ready()` probe (`stop` set on first `on_waiting`), prints `ok    redis version=<v> address_family=<ipv6|ipv4> maxmemory_policy=<p> appendonly=<a>` (plus any `warn` lines) and exits 0; fails fast with exit 1 on unreachable Redis or `StartupFatal`.
- **`--backfill-contract <name>` ([tools.hpp](../../evm_listener/src/app/tools.hpp) / [tools.cpp](../../evm_listener/src/app/tools.cpp), §5.6a + D8):**
  - Looks up `<name>` in `cfg.contracts` (throws `ConfigError` → exit 2 if unknown) and validates provider env vars via `make_pool_config()` before connecting to Redis or acquiring the lease.
  - Runs `await_redis_ready()` and, when `[lease] enabled = true`, calls `sink::acquire_lease_blocking()` bounded by `lease_timeout` (default 60 s). Exits 1 without writing anything if another host holds the lease.
  - Starts a fenced `RedisSink` (or unfenced token `"0"` when lease is disabled), loads the stored Redis checkpoint `{safe=C_s, finalized=C_f, contracts}`, and sweeps only the target contract via `Sweeper` + `PoolLogSource`:
    - `[start_block, C_f]` with the contract's route finality set to `Finality::Finalized` (`final_step(C_f)`, not tracked in `released`).
    - For safe-class contracts when `C_s > C_f`, `(max(start_block - 1, C_f), C_s]` at `Finality::Safe` (`safe_step(C_s)`, tracked in `released` and `released_data`).
  - Waits for `sink.wait_idle()` (all page emissions acked), merges the contract's `address:start_block:class` entry into `ckpt.contracts` via `sink.set_contracts()`, and calls `sink.drain(25 s)` to flush the manifest and release the lease.
- **Tests ([host_test.cpp](../../evm_listener/tests/host_test.cpp)):**
  - `tools: --check-redis prints address_family and startup check results, and exits 1 when unreachable`.
  - `tools: --backfill-contract emits finalized below C_f and safe/released in C_f..C_s, updates manifest, is idempotent, and enforces D8 lease fencing`: verifies host exit 2 before backfill, exit 1 with 0 writes when lease is held by another host (D8), lease acquire + release when free, exact `finalized` vs `safe` emissions and `released` ZSET tracking, 0 new entries on second run (idempotency via dedup), and subsequent host startup reaching `Live`.
- Gate: `make build` exit 0 (`-Werror`); `make test` exit 0, **509/509** (507 + 2).

### 5-H Production Docker image, `railway.json` and local compose (`listeners` profile): done (2026-10-28)
- **[Dockerfile.prod](../../docker/Dockerfile.prod) & [mk/60-prod.mk](../../mk/60-prod.mk):**
  - Stage 1 (`builder` on `mcr.microsoft.com/devcontainers/cpp:1-bookworm`): installs toolchain + Conan 2, links `build/Release/generators/CMakePresets.json` to `CMakeUserPresets.json` so `conan install` stays cached across source edits, builds `evm_listener` in `Release` (`-DEVML_BUILD_TESTS=OFF -DEVML_SANITIZE=OFF`, with `-Wno-restrict -Wno-maybe-uninitialized` for GCC 12 `-O3` false positives in `basic_string.h` and `std::variant`), and runs `strip --strip-unneeded`.
  - Stage 2 (`runtime` on `debian:bookworm-slim`): installs only `ca-certificates`, `libstdc++6` and `tini` (`curl`, `tzdata` and `libssl3` dropped since Conan links OpenSSL 3.5.9 statically); copies `/usr/local/bin/evm_listener` and `evm_listener/config/ -> /app/config/`; sets `STOPSIGNAL SIGTERM`, `HEALTHCHECK` (using `/bin/bash` `/dev/tcp/127.0.0.1/${PORT:-9464}` against `/healthz`), `ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/evm_listener"]`, and `CMD ["--config", "/app/config/base.toml"]`.
  - **Image size report:** compressed layer total is **~35 MB** (`28.1 MB` `debian:bookworm-slim` + `~3 MB` `ca-certificates`/`libstdc++6`/`tini` + `~4 MB` compressed binary); unpacked filesystem is **129.9 MB** (`108 MB` unpacked `debian:bookworm-slim` base + `10.3 MB` runtime packages + `11.5 MB` stripped static-Conan binary + `20.5 kB` config; Docker Desktop's containerd store reports `166 MB` combined unpacked + compressed blobs). `ldd` shows only `libstdc++.so.6`, `libm.so.6`, `libgcc_s.so.1`, and `libc.so.6`.
- **[railway.json](../../railway.json):**
  - Created at repo root per §5.6 / D10 (`dockerfilePath: "docker/Dockerfile.prod"`, `healthcheckPath: "/healthz"`, `healthcheckTimeout: 120`, `restartPolicyType: "ALWAYS"`, no `startCommand`).
- **[.devcontainer/docker-compose.yml](../../.devcontainer/docker-compose.yml):**
  - Added `listener-a` (`EVML_HOST_ID: a`) and `listener-b` (`EVML_HOST_ID: b`) under `profiles: ["listeners"]` with `restart: always`, `REDIS_URL: redis://redis:6379/0`, and optional `../evm_listener/.env`. Default `docker compose up` (`dev`, `redis`, `toxiproxy`) is unchanged.
- **Test-harness fix (bucket 3):** in `host: a finalized regression on the current provider freezes release and stops the sweeps` ([host_test.cpp](../../evm_listener/tests/host_test.cpp)), added a 1 s settle wait after `h.live()` so the three dead WS providers (`quicknode`, `infura`, `alchemy` → `127.0.0.1:9`) finish failing over to `ankr` and `ankr` records `finalized = c0 + 250` before the test lowers `ns.finalized = c0 + 100`.
- Gate: `make prod-image` exit 0; `--version` and `--check-config` inside `evm-listener:prod` exit 0; `make build && make test` exit 0, **509/509**.
