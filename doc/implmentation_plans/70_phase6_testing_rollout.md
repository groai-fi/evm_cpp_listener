# Phase 6: Testing, live smoke, soak and rollout, rev 1

**Goal:** prove the listener is correct against the real chain and real providers, deploy it to Railway as two services, and run it for **72 h with a clean soak**. Ship the consumer contract (README) and the ops runbook.

**Gate (master plan):** 72 h soak clean, as defined in §6.7.

**Entry criteria:**
- Phase 5 acceptance is met (chaos tests green; in tests every condition alert fires and resolves and every event alert fires, plan 60 rev 10 §5.3.1). Every Phase 5 slice is committed and merged to `main`.
- `make test` exits 0 in Debug (ASan/UBSan). `make prod-image` builds.

**Section numbers are referenced elsewhere. Do not renumber:**
- §6.4 is the provider quota measurement. Plan 40 §2 and job 71 depend on it.
- §6.5 is the Railway smoke, which uses `--check-redis` (plan 50 §4.3).

---

## 6.1 Test completeness audit (repo, no network)
**Why:** each phase added tests under its own scenario numbers. Before trusting the stack on a real chain, check that every scenario a plan promised actually exists and runs.

- [ ] Write a **traceability table** in this plan's results section. Each row is a promised scenario from Phase 3 (§Tests), Phase 4 (scenarios 1–19 plus the unit list) and Phase 5 (§Tests), mapped to the exact Catch2 test name and file. A missing test is a blocker. Fix it, or get the user's approval to drop it.
- [ ] Run the full suite in **Release** as well (`make build BUILD_TYPE=Release` + ctest). Optimisation can expose races and UB that Debug hides.
- [ ] Repeat the timing-sensitive tagged tests 20× (`[redis_serial]`, the lease, takeover and watchdog tests) and record any flake. A flaky test counts as a failure and must be root-caused (unit-test SOP, bucket 3).
- [ ] Confirm that no `.skip`, `[!mayfail]` or `[.]` tags hide a test (`grep`).

**Accept:** a complete table, Debug and Release both exit 0, 20 × 0 flakes.

## 6.2 Stream auditor (new tool, needed by §6.3, §6.6 and §6.7)
**Why:** "gap-free and duplicate-free" is the core acceptance claim, and nothing measures it today. The auditor compares what we wrote with an independent re-read of the chain.

`evm_listener --config <file> --audit-stream <from_block> <to_block>` (read-only, no lease):
1. Read the `events` and `registry` streams (`XRANGE`) and fold them per `event_id`. The final state is the last `type` (`log` or `revert`).
2. Re-read the canonical logs for `[from, to]` with `eth_getLogs` at **finalized** depth, from a provider other than the one most entries name in `first_source`. `to` must be ≤ `ckpt.finalized`.
3. Report:
   - **missing**: canonical log with no live entry;
   - **phantom**: live entry not canonical (it should have been reverted);
   - **dup_live**: more than one net-live entry for one `event_id`;
   - **field_mismatch**: same `event_id`, different topics or data;
   - **order_violation**: on the normal path, a safe or finalized class that is not increasing by `(block, log_index)`. Informational: corrections are allowed out of order (plan 40 I4).
4. Exit 0 iff missing = phantom = dup_live = field_mismatch = 0. Otherwise exit 1 and print the first 20 of each.

- It reuses `ProviderPool`, the codec and `RedisppConn`. No new dependencies. Paged with the same AIMD pager.
- Retention: entries older than 48 h are trimmed (`MINID`), so the audit window must lie inside the retention window. The soak audits rolling 24 h windows (§6.7).
- **Tests:** a fixture stream with one injected example of each defect, so each counter is proven to fire. A clean fixture exits 0.

## 6.3 Local live run (Docker compose, real Base mainnet providers)
**Why:** this is the first time the real providers, real reorgs and real event volume meet the full stack. It runs locally because it is cheap to restart and inspect.

- [ ] `.devcontainer/docker-compose.yml` with profile `listeners` brings up `listener-a` and `listener-b`, plus `redis:8.4` (`appendonly yes`, `noeviction`), using the real `evm_listener/.env` providers.
- [ ] **Cold start from `contract_start`** (block 52060000): catch-up reaches Live. Record:
  - catch-up duration;
  - getLogs pages and range shrinks per provider;
  - peak spill and queue size.
- [ ] **Registry topic0 live confirmation.** `base.toml` says the factory and Voter topic0s are "not yet live-confirmed". Find at least one real `PoolCreated` or `GaugeCreated` in history, or confirm on a block explorer that the topic0 matches. Record the tx hash.
- [ ] **F3 User-Agent check:** the public `mainnet.base.org` endpoint answers getLogs with our UA (no Cloudflare 1010).
- [ ] **Takeover:** stop `listener-a`. `b` leads within 20 s, and Leader Changed fires (to a local mock or a test n8n workflow).
- [ ] Run for **≥ 6 h**, then run `--audit-stream` over the finalized part of the window. **Accept:** exit 0.

## 6.4 Provider quota measurement (referenced by plan 40 §2 and job 71)
**Why:** the master plan F5 budget is unverified. Going over a monthly limit does not lose data (the pool fails over), but it degrades to slower or public providers. Plan limits and per-method credit weights were never confirmed (master F5, plan 60 old §5.1a).

During the §6.3 run (≥ 6 h, extrapolate) **and** again during the first 24 h of §6.7:
- [ ] From our side: per-provider and per-method call counts. Use `evml_rpc_requests_total{provider,method}`. If Phase 5 did not add it, add it here as a counter in `ProviderPool`.
- [ ] From the provider dashboards: QuickNode credits/day, Infura requests/day, and Ankr / Alchemy if active. Screenshot or copy the numbers into the results section.
- [ ] Compare against each plan's monthly limit, as calls/day × 30.

| Provider | Expected (master F5) | Measured/day | Plan limit/month | Headroom |
|---|---|---|---|---|
| QuickNode (WS `heads` + logs) | ≈ 43k notifications + ≈ 9.7k logs | | | |
| Infura (tags + getLogs) | ≈ 29k tag polls + ≈ 7k getLogs | | | |
| Ankr / public / Alchemy | fallback only | | | |

**Decision output:**
- If any provider is above 70% of its monthly limit, report it to the user **before** §6.6, with options:
  - lengthen `safe_poll_ms`;
  - change the getLogs order;
  - upgrade the plan;
  - or open job 71 (WS `ping` mode, [71_job_ws_ping_liveness.md](71_job_ws_ping_liveness.md)).
- These numbers are the job 71 §1 decision gate.

## 6.5 Railway smoke (one service, before the real rollout)
- [ ] **Railway project setup** (user side, see §6.11): a Redis service with `appendonly yes` and `maxmemory-policy noeviction` verified by `CONFIG GET`. Create service `evml-base-a` from the repo with `railway.json` (no `startCommand`; plan 60 §5.6 rev 9).
- [ ] **`--check-redis`** as a one-off run on Railway: it reaches `redis.railway.internal` over IPv6 and prints the address family.
  - If it fails, use the plan 50 §4.3 fallback: resolve with `getaddrinfo(AF_UNSPEC)` ourselves and pass the literal address. That is a code change: report it first.
- [ ] Deploy healthcheck: `/healthz` passes within `healthcheckTimeout` 120 s, **including** a start where Redis is slow (the healthcheck is served during the Redis wait).
- [ ] **Signals:** redeploy and check the logs for drain, then lease release, then exit 0. This confirms `tini` forwards SIGTERM and `RAILWAY_DEPLOYMENT_DRAINING_SECONDS=30` is honoured.
- [ ] **Structured logs:** Railway's log explorer parses our JSON lines (`level` and `message` filterable). Plan 60 §5.6a says "verify on first deploy".
- [ ] **Secrets:** grep 1 h of Railway logs for every secret env value (provider URLs with keys, `N8N_ALERT_WEBHOOK_TOKEN`, `DEADMAN_PING_URL`) → 0 hits.
- [ ] **Alerts:** send one `severity=info` test alert to the real n8n webhook, and it appears in `#strategy-production` (plan 60 §Tests "Live, manual, once").
- [ ] **Dead-man:** the healthchecks.io check turns green within 2 min of Live. Pause the service → the check pages within about 3 min.
- [ ] Private networking: `/metrics` and `/readyz` are reachable from another service in the project, and there is **no public domain** on the service.

## 6.6 Rollout (plan 60 §5.9)
1. `evml-base-a`: Live, with `checkpoint_lag` < 1 page.
2. Deploy `evml-base-b` (`EVML_HOST_ID=b`). It becomes standby, and its `/readyz` is 200 as a healthy standby.
3. **Forced takeover:** restart `a`. `b` leads within 20 s, Leader Changed fires once, and `a` comes back as standby.
4. Run `--audit-stream` over the takeover window once it is finalized (≈ 20 min later). **Accept:** exit 0.
5. Bot B (`strat:aero_eurcusdc:botB_v1`) creates its consumer group and starts reading. Consumer Group Missing must clear within 10 min of creation.

Rollback: redeploy the previous image tag on both services. The stream schema and keys are unchanged across v1 builds, so a rollback needs no data migration.

## 6.7 72 h soak (the Phase 6 gate)
Start the clock after §6.6 step 5. **Pass only if every row holds:**

| Check | Pass condition | Source |
|---|---|---|
| Stream exactness | `--audit-stream` exits 0 on three consecutive 24 h windows (each audited once finalized) | §6.2 |
| Unplanned restarts | 0 watchdog aborts (exit 134) and 0 crash restarts. Deliberate restarts are logged in the results | Railway deploy log |
| Critical alerts | every critical alert has a written root cause. 0 unexplained | Slack / n8n |
| Freshness | safe-class: `G_s − C_s` ≤ 1 page for > 99% of 1-min samples. finalized-class: `G_f − C_f` ≤ 1 page for > 99% | `/metrics` |
| Sink | spill empty > 99% of the time; no Sink Saturated | `/metrics` |
| Memory | RSS growth over 72 h < 10% after the first hour (no leak) | Railway metrics |
| Quota | §6.4 headroom stays ≥ 30% on every provider | dashboards |
| Consumer | Bot B `undelivered_age_s` < `consumer_lag_warn_s` (12 h) throughout | consumer lag metrics |
| Dead-man | 0 healthchecks.io pages | healthchecks.io |
| Takeover drill | one forced takeover at about 36 h, then audited exit 0 | §6.6 step 3–4 |

A failed row restarts the 72 h clock **after** the root-cause fix.

## 6.8 README: consumer contract (repo root `README.md`; it does not exist yet)
**Why:** downstream bots must code against a written contract, not against the listener's internals. Plan 40 §3.7–3.8, plan 50 §4.1 / P6 and the master plan refer to this README; plan 50 §4.1 calls it "README §5".

Required sections, in this order (§5 must be the consumer contract):
1. What it is, in one paragraph: Base 8453, the shard, the contracts, and "raw logs to Redis Streams, no signing".
2. Quick start: build, test, run locally (`make` targets) and config pointers.
3. Configuration: `base.toml` sections; env variables (names only, from `.env.example`); the provider-enable rule.
4. Operations: a pointer to the runbook (§6.9).
5. **Consumer contract.** Each sub-point is short, with one concrete example:
   - Streams: `evml:{base_eurc_usdc}:events` and `:registry`; schema v=1 with every field (plan 40 §3.8).
   - **The 3 finality states a consumer sees:**
     - `type=log, finality=safe`: emitted about 20–30 s after the block. Rarely followed by a `revert`.
     - `type=log, finality=finalized`: permanent. This covers finalized-class contracts and late safe-class logs.
     - `type=revert`: retracts an earlier safe log; carries its full before-image.
     - Every event with `block_number ≤ ckpt.finalized` is final.
     - Example: a Swap at block 100 is emitted `safe`. If a reorg drops it, a `revert` with the same `event_id` follows. Once `ckpt.finalized ≥ 100` nothing about block 100 changes again.
   - `event_id = chain_id:blockHash:logIndex`. Consumers **must** be idempotent on `event_id` and order by `(block_number, log_index)`. Stream order is not chain order across hosts or corrections.
   - Duplicates are possible (at-least-once delivery). `log → revert → log` for one `event_id` is legal (a reorg flip).
   - **48 h hard read deadline:** entries older than `retention_ms` are trimmed whether read or not. Register your consumer group in `[sink] consumer_groups` so lag is alerted.
   - Liveness: `hb:<host>` (TTL 30 s) tells "quiet market" from "listener dead". **Never** infer a dead listener from event silence.
   - Accepted risks (plan 50): a single Redis node; the AOF 1 s window; the 7 d dedup TTL on re-backfill.
6. Architecture: a pointer to `doc/System_Modules_and_Architecture.md`.

**Accept:** the user reviews §5. Bot B's owner confirms the contract matches its consumer code.

## 6.9 Ops runbook (`doc/runbook.md`)
One entry per alert in plan 60 §5.3.3. Each entry gives: what it means, the first 3 checks, the fix, and when to escalate. Plus these procedures:
- deploy, rollback, forced takeover;
- the **partial checkpoint** exit (plan 40: restore all keys or delete all);
- **uncovered contract** exit 2 → `--backfill-contract <name>` (with both services stopped, D8);
- **Redis data loss** → re-backfill from `start_block` (accepted SPOF, plan 60 §5.5);
- **Consumer Entries Trimmed** recovery (plan 60 §5.3.3);
- adding a provider (env var only) and rotating a provider key;
- the healthchecks.io and n8n setup, so the configuration is reproducible.

## 6.10 Go / no-go checklist (end of Phase 6)
- [ ] §6.1–6.9 accepted; §6.7 soak passed.
- [ ] Traceability table complete; Debug and Release green.
- [ ] README §5 approved by the user and Bot B's owner.
- [ ] Runbook reviewed.
- [ ] §6.4 numbers recorded. Job 71 opened or closed with those numbers (job 71 §1).
- [ ] Master plan status updated.

## 6.11 Prerequisites on the user side (not code)
| Item | Needed by | Status |
|---|---|---|
| `BASE_WSS_URL` (Infura WS) set; otherwise the WS chain is QuickNode only (master F4) | §6.3 | open |
| W4: correct `doc/Dynamic_Discovery.md` (master §2) | §6.8 | open |
| Railway: a project, the Redis service (`appendonly yes`, `noeviction`), and services `evml-base-a` / `-b` (no volume, no public domain, `RAILWAY_DEPLOYMENT_DRAINING_SECONDS=30`, ideally different regions) | §6.5 | open |
| Railway variables per plan 60 §5.6 (providers, `REDIS_URL`, `EVML_HOST_ID`, N8N, DEADMAN) | §6.5 | open |
| healthchecks.io check: period 1 min, grace 2 min, routed to Slack | §6.5 | open |
| n8n "EVM Listener Alert Webhook" workflow active, with the token issued | §6.5 | open (exists per plan 60) |
| Provider plan limits per month (QuickNode, Infura) for the §6.4 headroom table | §6.4 | open |
| Bot B ready to create `strat:aero_eurcusdc:botB_v1` and read | §6.6 | open |

## 6.12 Open decisions (recommended default first)
| # | Decision | Recommended |
|---|---|---|
| P6-1 | Auditor form | **C++ subcommand `--audit-stream`.** It reuses the pool, codec and Redis code, needs no new runtime, and ships in the image. Alternative: a Python script under `tools/`, which is faster to write but adds a dependency and duplicates the decoding. |
| P6-2 | CI (no `.github/workflows` today) | **Add a minimal GitHub Actions job:** Debug build + unit tests in the devcontainer image, with integration tests against service containers (redis, toxiproxy). It is not a Phase 6 gate, but it prevents regressions after go-live. Alternative: stay local-only (`make test`). |
| P6-3 | Soak length if a row fails late | **Restart the full 72 h** after the fix (the master gate). Alternative: restart only the failed check's window. Faster, but weaker. |

## Tests (new code in this phase)
- `--audit-stream`: one fixture per defect counter, plus a clean fixture (§6.2).
- `evml_rpc_requests_total`, if added in §6.4: a unit test that counts per provider and method.

## Acceptance
§6.10 all checked. The 72 h soak passes every row of §6.7.
