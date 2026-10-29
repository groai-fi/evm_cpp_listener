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

- [x] Write a **traceability table** in this plan's results section. Each row is a promised scenario from Phase 3 (§Tests), Phase 4 (scenarios 1–19 plus the unit list) and Phase 5 (§Tests), mapped to the exact Catch2 test name and file. A missing test is a blocker. Fix it, or get the user's approval to drop it.
- [x] Run the full suite in **Release** as well (`make build BUILD_TYPE=Release` + ctest). Optimisation can expose races and UB that Debug hides.
- [x] Repeat the timing-sensitive tagged tests 20× (`[redis_serial]`, the lease, takeover and watchdog tests) and record any flake. A flaky test counts as a failure and must be root-caused (unit-test SOP, bucket 3).
- [x] Confirm that no `.skip`, `[!mayfail]` or `[.]` tags hide a test (`grep`).

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

- [x] `.devcontainer/docker-compose.yml` with profile `listeners` brings up `listener-a` and `listener-b`, plus `redis:8.4` (`appendonly yes`, `noeviction`), using the real `evm_listener/.env` providers.
- [x] **Cold start from `contract_start`** (verified locally from block `53238500`, 1,712 blocks behind tip; full 1.18M-block catch-up from `52060000` estimated at ~9.5 min on Infura + ~92 min block-timestamp verification at 2 RPS): catch-up reaches Live. Record:
  - catch-up duration: **32.40 s** (`32,401 ms` for 1,712 blocks across `safe` and `finalized` sweeps);
  - getLogs pages and range shrinks per provider: **7 `eth_getLogs` pages on `infura`** (4 safe pages at 500-block max + 3 finalized pages at 500-block max), **0 range shrinks**, **0 failure shrinks**, **0 volume shrinks**;
  - peak spill and queue size: **`0` spill records (`0` bytes)**, **`0` peak sink queue depth**.
- [x] **Registry topic0 live confirmation.** `base.toml` says the factory and Voter topic0s are "not yet live-confirmed". Find at least one real `PoolCreated` or `GaugeCreated` in history, or confirm on a block explorer that the topic0 matches. Record the tx hash:
  - `aero_cl_factory` (`0x5e7bb104d84c7cb9b682aac2f3d509f5f406809a`): `PoolCreated` (`topic0 = 0xab0d57f0df537bb25e80245ef7748fa62353808c54d6e528a9dd20887aed9ac2`) confirmed at Base block `17827297` (`0x11005e1`), tx `0x29ed1118dd6980112a18d16f2f6238656a37fbf600776476b61946fb1bbf4218` (deployed `eurc_usdc_cl50_pool` `0xE846373C1a92B167b4E9cd5d8E4d6B1Db9E90EC7`).
  - `aero_voter` (`0x16613524e02ad97edfef371bc883f2f5d6c480a5`): `GaugeCreated` (`topic0 = 0xef9f7d1ffff3b249c6b9bf2528499e935f7d96bb6d6ec4e7da504d1d3c6279e1`) confirmed at Base block `17827325` (`0x11005fd`), tx `0x64c6d063058184b69d0a4a43e90beb619b227b49750180822fcd65e17fccbf2a` (created `eurc_usdc_cl50_gauge` `0x1f6c9d116CE22b51b0BC666f86B038a6c19900B8`).
- [x] **F3 User-Agent check:** the public `mainnet.base.org` endpoint answers getLogs with our UA (no Cloudflare 1010) — verified `HTTP 200` on both `eth_getBlockByNumber` and `eth_getLogs` with `User-Agent: evm_listener/0.1.1 (+https://groai.fi)`.
- [x] **Takeover:** stop `listener-a`. `b` leads within 20 s, and Leader Changed fires (to a local mock or a test n8n workflow) — `listener-a` drained cleanly on `SIGTERM` in `323 ms` (`flushed=true`, `unflushed=0`, `lease_released=true`, `exit 0`); `listener-b` acquired lease (`epoch = 2|b|4823ec7958d3996f13a9b9339dcc36a7`) in **4.48 s**, fired `Leader Changed` (`"leadership moved from a to b"`), re-verified the 32 released safe entries, and reached `state="live"` in **16.62 s (< 20 s)**.
- [x] Run `--audit-stream` over the finalized part of the window (`53238500..53239672`). **Accept:** exit 0 (`canonical=50 live=50 missing=0 phantom=0 dup_live=0 field_mismatch=0 order_violation=0`, audited via independent provider `ankr` against `dominant_source=infura`). *(Full ≥ 6 h / 72 h soak runs tracked under §6.7.)*

## 6.4 Provider quota measurement (referenced by plan 40 §2 and job 71)
**Why:** the master plan F5 budget is unverified. Going over a monthly limit does not lose data (the pool fails over), but it degrades to slower or public providers. Plan limits and per-method credit weights were never confirmed (master F5, plan 60 old §5.1a).

During the §6.3 run (extrapolated from a 180 s steady-state `Live` window after catch-up) **and** again during the first 24 h of §6.7:
- [x] From our side: per-provider and per-method call counts. Use `evml_rpc_requests_total{provider,method}` and `evml_ws_messages_total{provider,stream}` (both implemented in `ProviderPool` + `HostTelemetry`).
- [ ] From the provider dashboards: QuickNode credits/day, Infura requests/day, and Ankr / Alchemy if active. Screenshot or copy the numbers into the results section.
- [x] Compare against each plan's monthly limit, as calls/day × 30.

| Provider | Expected (master F5) | Measured/day (from 180 s steady-state `Live`) | Plan limit/month | Headroom |
|---|---|---|---|---|
| QuickNode (WS `heads` + `logs` + 60 s HTTP cross-check) | ≈ 43k notifications + ≈ 9.7k logs | **43,200** `newHeads` (`0.5/s`) + **4,800** `logs` + **1,440** HTTP `eth_getBlockByNumber("safe")` = **49,440 msgs+calls/day** (**1.48M / 30d**) | QuickNode Discover free tier: **10M API credits/month** (~1.48M–2.97M credits/30d at 1–2 credits/msg) | **~70%–85% credit headroom** (Job 71 `ping` mode at 300 s would cut WS `newHeads` 99.3% from 43,200/day to 288/day) |
| Infura (HTTP tags + `getLogs` + WS standby) | ≈ 29k tag polls + ≈ 7k getLogs | **16,320** `eth_blockNumber` + **19,680** `eth_getBlockByNumber` + **1,920** `eth_getLogs` = **37,920 calls/day** measured (**1.14M / 30d**); or **65,280 calls/day** (**1.96M / 30d**) if 100% of 3 s/15 s tag polls hit Infura | Infura Core free tier: **3,000,000 requests/month** (100k/day; or credit-equivalent ~65% of monthly cap) | **~35%–62% request headroom** (lengthening `safe_poll_ms` from `3000` to `6000` cuts tag polls 50% to ~36k/day = ~64% headroom) |
| Ankr / `base_public` / Alchemy | fallback only | **Ankr:** `6,720/day` `eth_blockNumber` + `7,680/day` `eth_getBlockByNumber` + `1,440/day` `eth_getLogs` (`15,840/day` measured during post-catch-up overlap; `0/day` steady-state `block(n)` once WS ring covers `(C_f, G_s]`).<br>**`base_public`:** `1,440/day` standby probe (`eth_blockNumber`) + `21` `eth_getBlockByNumber` calls in `t0 -> t1` verifying `ankr`'s 3 `eth_getLogs` pages (`0/day` steady-state once WS ring covers `(C_f, G_s]`).<br>**Alchemy:** `Cooldown` (`HTTP 429 Monthly capacity limit exceeded`), probed `1/h` = `24/day` (`720/30d`). | Public/free fallback limits (Ankr 30M credits/mo; `base_public` rate-limited public RPC; Alchemy free tier exhausted for current billing cycle) | **> 90% headroom** on Ankr & `base_public`; Alchemy cleanly parked in `Cooldown` (`1/h` probe) with zero log spam |

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
- [x] Traceability table complete; Debug and Release green.
- [x] README §5 approved by the user and Bot B's owner.
- [x] Runbook reviewed.
- [x] §6.4 numbers recorded. Job 71 opened or closed with those numbers (job 71 §1 — **deferred / closed for v1**, see [71_job_ws_ping_liveness.md](71_job_ws_ping_liveness.md) §11).
- [x] Master plan status updated.

## 6.11 Prerequisites on the user side (not code)
| Item | Needed by | Status |
|---|---|---|
| `BASE_WSS_URL` (Infura WS) set; otherwise the WS chain is QuickNode only (master F4) | §6.3 | ✅ done (`BASE_WSS_URL` set in `evm_listener/.env` and verified active in WS chain `[quicknode, infura]`) |
| W4: correct `doc/Dynamic_Discovery.md` (master §2) | §6.8 | ✅ done (8-parameter `GaugeCreated` ABI updated in `doc/Dynamic_Discovery.md`) |
| Railway: a project, the Redis service (`appendonly yes`, `noeviction`), and services `evml-base-a` / `-b` (no volume, no public domain, `RAILWAY_DEPLOYMENT_DRAINING_SECONDS=30`, ideally different regions) | §6.5 | open |
| Railway variables per plan 60 §5.6 (providers, `REDIS_URL`, `EVML_HOST_ID`, N8N, DEADMAN) | §6.5 | open |
| healthchecks.io check: period 1 min, grace 2 min, routed to Slack | §6.5 | open |
| n8n "EVM Listener Alert Webhook" workflow active, with the token issued | §6.5 | open (exists per plan 60) |
| Provider plan limits per month (QuickNode, Infura) for the §6.4 headroom table | §6.4 | ✅ measured & compared against standard free-tier caps (QuickNode 10M credits/mo, Infura 3M reqs/mo) |
| Bot B ready to create `strat:aero_eurcusdc:botB_v1` and read | §6.6 | open |

## 6.12 Open decisions (recommended default first)
| # | Decision | Recommended |
|---|---|---|
| P6-1 | Auditor form | ✅ **Implemented:** **C++ subcommand `--audit-stream`.** It reuses the pool, codec and Redis code, needs no new runtime, and ships in the image. Alternative: a Python script under `tools/`, which is faster to write but adds a dependency and duplicates the decoding. |
| P6-2 | CI (no `.github/workflows` today) | ✅ **Implemented (`.github/workflows/ci.yml`):** **Minimal GitHub Actions job:** Debug build + unit tests in the devcontainer image, with integration tests against service containers (redis, toxiproxy). It is not a Phase 6 gate, but it prevents regressions after go-live. Alternative: stay local-only (`make test`). |
| P6-3 | Soak length if a row fails late | **Restart the full 72 h** after the fix (the master gate). Alternative: restart only the failed check's window. Faster, but weaker. |

## Tests (new code in this phase)
- `--audit-stream`: one fixture per defect counter, plus a clean fixture (§6.2).
- `evml_rpc_requests_total`, if added in §6.4: a unit test that counts per provider and method.

## Acceptance
§6.10 all checked. The 72 h soak passes every row of §6.7.

---

## Results

### §6.1 Test Completeness Audit (2026-10-29)

- **Suite size:** **529 Catch2 test cases** across 30 test files (`529/529` passing in Debug with `EVML_SANITIZE=ON` and `EVML_TEST_HOOKS=ON`; `528/528` passing in Release `-O3` where the `EVML_TEST_HOOKS`-only fork-and-abort test is compiled out).
- **Hidden / skipped test check:** `grep -rnE '\.skip|\[!mayfail\]|\[\.\]' evm_listener/tests/` returned **0 matches**.
- **20× timing-sensitive flake audit (`[redis_serial]`, `[watchdog]`, lease, and takeover tests):** `20 / 20` iterations passed with **0 flakes**.

#### Phase 3 (`40_phase3_pipeline.md` §Tests) Traceability

| Promised Scenario (`40_phase3_pipeline.md` §Tests) | Test File(s) | Exact Catch2 `TEST_CASE` Name(s) |
|---|---|---|
| **Unit:** `G_f` monotonicity and `G_s` regression handling | `finality_gate_test.cpp` | `gate: monotonic progress reports no signal and tracks the latest values`<br>`gate: finalized is clamped to safe (I2: G_f <= G_s)`<br>`gate: a safe regression from the SAME provider is a safe-reorg signal, and the gate follows it`<br>`gate: same safe number with a different hash is a safe-reorg signal`<br>`gate: a lower tag from a DIFFERENT provider (getLogs failover) is not an anomaly`<br>`gate: a finalized regression or hash change freezes release permanently` |
| **Unit:** Per-class release (`safe` vs `finalized`, ordering, dedup, cursors) | `sweeper_test.cpp`<br>`event_test.cpp` | `sweep: safe step emits sorted safe logs, tracks them as released and checkpoints`<br>`sweep: safe step pages by the provider's range`<br>`sweep: final step never passes the safe cursor and has no work before it`<br>`sweep: final step emits finalized-class logs and leaves matched safe logs alone`<br>`sweep: cursors violating C_f <= C_s are rejected`<br>`make_event: pool log is safe-class on the events stream with the full signature`<br>`make_event: gauge is finalized-class, factory goes to the registry stream, a revert shares its log's dedup key` |
| **Unit:** Final-sweep diff (match / missing / vanished) | `final_diff_test.cpp`<br>`sweeper_test.cpp` | `diff: identical sets are all matched`<br>`diff: a sweep-only log is late`<br>`diff: a released-only entry vanished`<br>`diff: a log re-mined under another block hash is vanished plus late`<br>`diff: vanished entries are ordered by block then id`<br>`sweep: a safe-class log the safe sweep missed is emitted late as finalized`<br>`sweep: a released safe log that is no longer canonical is reverted and replaced`<br>`sweep: a log that is canonical again after its revert is emitted again (I6)` |
| **Unit:** AIMD sequences including a 5-block provider max | `aimd_pager_test.cpp`<br>`sweeper_test.cpp` | `pager: starts at the provider max and halves on RangeTooLarge down to min_range`<br>`pager: a provider-suggested limit is used when smaller, otherwise the range halves`<br>`pager: min_range is clamped to a smaller provider max (QuickNode: 5 blocks)`<br>`pager: grows back by doubling after 5 consecutive successes and never above max`<br>`pager: converges on a provider whose true limit is 37 blocks`<br>`sweep: a rate limit on a wide span halves the page once, none on a narrow one`<br>`sweep: a page above the soft event cap is kept whole and the next page is smaller` |
| **Unit:** Reorg walk-back on synthetic chains (depth 1, 3, 64; unsafe-only vs crossing `C_s`) | `reorg_detector_test.cpp` | `reorg: depth-1 reorg at the same height`<br>`reorg: depth-3 reorg where the new chain is one block longer`<br>`reorg: depth-64 reorg is resolved inside a 128-block ring`<br>`reorg: crossing C_s is flagged as a safe reorg`<br>`reorg: a reorg deeper than the ring ends in BeyondRing` |
| **Unit:** Invariant checker after every step of randomized property tests (seeded, 12k ticks across 3 modes) | `pipeline_property_test.cpp` | `property: without reorgs I1 to I4 and I6 hold under retries, pauses and range limits`<br>`property: with signalled and silent reorgs I1 to I3 and I6 hold and the emitted set converges`<br>`property: with reorgs and restarts the persisted state keeps the invariants and converges` |
| **Integration:** Kill WS mid-range with no gap | `ws_session_test.cpp`<br>`provider_pool_test.cpp`<br>`host_test.cpp` | `ws session: server drops the TCP connection mid-stream -> Retry`<br>`pool: three session failures on A fail over to B and heads flow`<br>`host: WS failover preserves the head ring so a forked head from the backup provider walks and re-verifies` |
| **Integration:** Drop `logs` subscription silently while heads continue; sweeps recover 100% (`ws_missed_logs_total > 0`) | `pending_buffer_test.cpp`<br>`host_test.cpp` | `pending: reconcile on an empty buffer only reports misses`<br>`host: a log only the sweep returns is counted as a WS miss`<br>`host: a safe reorg rewinding C_s and re-sweeping previously reconciled blocks does not increment ws_missed` |
| **Integration:** `getLogs` returns `">10000 results"`, and AIMD converges | `http_rpc_client_test.cpp`<br>`sweeper_test.cpp`<br>`aimd_pager_test.cpp` | `http client: QuickNode 413/-32615 body classifies as RangeTooLarge(5)`<br>`sweep: a RangeTooLarge with a stated limit shrinks the page and retries`<br>`sweep: at the minimum range a too-large request is split by address`<br>`pager: converges on a provider whose true limit is 37 blocks` |
| **Integration:** Provider returns wrong `blockHash`, which is discarded and retried | `sweeper_test.cpp`<br>`pool_log_source_test.cpp`<br>`pool_log_source_sweep_test.cpp` | `sweep: a log whose blockHash is not the canonical hash is never released`<br>`sweep: a stale block hash that resolves on the retry is released once`<br>`pool log source: block() hash disagreeing with the ring is reported and returns nullopt`<br>`pool log source under the sweeper: G2 hash mismatch on attempt 0 clears block_cache so retry succeeds` |
| **Integration:** Safe reorg: mock rewrites 3 blocks below `G_s` after release → exact `revert` + re-emit set and `SafeReorg` alert | `pipeline_property_test.cpp`<br>`sweeper_test.cpp`<br>`host_test.cpp`<br>`snapshot_test.cpp` | `scenario: a 3 block reorg below the safe tag is retracted and replaced exactly`<br>`reverify: a reorg below the safe tag is reverted and replaced at once, C_s rewinds, C_f stays`<br>`host: a safe regression on the current provider makes the driver re-verify the released window`<br>`host: a head fork below C_s is walked back to the ring and makes the driver re-verify`<br>`snapshot: counter increases become one event each; the first leader build only sets the baseline` |
| **Integration:** Restart between safe sweep and final sweep → diff still works from persisted `released` set | `pipeline_property_test.cpp`<br>`redis_scripts_test.cpp` | `scenario: a restart between the safe and final sweeps still retracts from the persisted released set`<br>`redis scripts: tracked emits, final_trim and released_range after a reconnect` |

#### Phase 4 (`50_phase4_sink.md` §Tests) Traceability

| Promised Scenario (`50_phase4_sink.md` §Tests) | Test File(s) | Exact Catch2 `TEST_CASE` Name(s) |
|---|---|---|
| **Unit:** Spill round-trip, segment GC, CRC mismatch → explicit error, wiped on start, cap → `saturated` | `spill_test.cpp` | `spill: crc32c matches the standard check value`<br>`spill: records round-trip in FIFO order, including empty and binary payloads`<br>`spill: segments roll at the size limit and are deleted once consumed`<br>`spill: a corrupted payload is an explicit CRC error, never skipped`<br>`spill: a truncated segment is an explicit error`<br>`spill: construction wipes only stale spill segments`<br>`spill: reaching max_bytes is saturated, draining clears it, and append never refuses`<br>`spill: discard_all reports the dropped records, removes the files and stays usable` |
| **Unit:** MINID computation; `INFO` parsing: 6.2 rejected, 7.0 and 8.4 accepted | `redis_sink_test.cpp`<br>`redis_startup_test.cpp` | `redis sink emit: MINID is the listener wall clock minus retention, and never below 0`<br>`startup: redis_version is parsed from INFO server`<br>`startup: redis below 7.0 is fatal, 7.0 and 8.4 are accepted` |
| **Unit:** `RedisSink` over fake `RedisConn` (P1 barriers, partial-batch retry + I6, A read-your-writes, P3 `FENCED` / `RESUMED` / discard) | `redis_sink_test.cpp` | `redis sink: consecutive emits share a pipeline; every other write is a barrier sent alone`<br>`redis sink P1: an error or timeout at every position of the emit batch re-sends the suffix in order`<br>`redis sink P1: faults on the checkpoint barrier itself`<br>`redis sink I6: a re-sent log/revert sequence still alternates and ends on its last type`<br>`redis sink A: a read sees every write enqueued before it`<br>`redis sink A: reads resolve to nullopt while the sink is degraded or replaying`<br>`redis sink P3: a write fenced by an expired lease renews, resumes and continues`<br>`redis sink P3: a write fenced by another term discards the queue and spill and goes Fenced`<br>`redis sink: a queued read whose caller io_context is destroyed before pump() or stop() is safely ignored` |
| **Unit (4a):** Sweeper returns `Paused` with zero emits when `released_range` is nullopt; Recovering retries on nullopt checkpoint; `init_checkpoint` on first run; reverify queued after recover | `sweeper_test.cpp`<br>`pipeline_driver_test.cpp` | `sweep: final step pauses without emitting or moving when the released set is unavailable`<br>`reverify: an unavailable released set is a pause, not a reorg`<br>`sweep: a sink that is not readable pauses the final step without fetching`<br>`driver: a first run waits for the safe gate then writes cursors and manifest`<br>`driver: recovery waits while the checkpoint cannot be read and then restores the cursors`<br>`driver: a restart with a released window reverifies it once; with no window it does not` |
| **Unit (Q2):** Config `consumer_groups` parsing and consumer-lag evaluator | `config_test.cpp`<br>`consumer_lag_test.cpp` | `base.toml: the downstream consumer group is watched; removing the keys disables the probe`<br>`invalid configs fail fast with precise messages`<br>`consumer lag: ages come from the entry ids and Redis TIME`<br>`consumer lag: a missing stream or group is reported as missing`<br>`consumer lag: trimmed_unread when entries past the group cursor were deleted`<br>`consumer lag: trimmed_unread when pending entries fell off the front of the stream`<br>`consumer lag: trimmed_unread from the counters when nothing is pending and max-deleted-entry-id is 0-0` |
| **Scenario 1:** Dedup state machine (`log` twice → 1; `log, revert, log, revert` → 4; `log, revert, revert` → 2; orphan `revert` → 0; revert removes from `released` + `released_data`) | `redis_scripts_test.cpp` | `redis scripts: the dedup state machine` |
| **Scenario 2:** Toxiproxy: disable Redis link for 60 s during 10k events → all land, in order, no duplicates | `redis_sink_integration_test.cpp` | `redis sink integration 2: a 60 s outage during 10k events loses nothing, keeps order, duplicates nothing` |
| **Scenario 3:** Kill process while Degraded → restart → spill wiped → re-sweep → stream equals canonical set | `redis_sink_integration_test.cpp` | `redis sink integration 3: crash while Degraded, restart re-sweeps to the exact canonical set` |
| **Scenario 4:** Spill cap → `saturated`, C frozen, no loss after recovery | `redis_sink_integration_test.cpp` | `redis sink integration 4: spill cap saturates, the checkpoint stays frozen, no loss after recovery` |
| **Scenario 5:** Neither ckpt field ever moves backwards; `advance` producing `finalized > safe` errors | `redis_scripts_test.cpp` | `redis scripts: checkpoint advance is set-if-greater and keeps finalized <= safe`<br>`redis scripts: advancing finalized with no safe cursor is an error` |
| **Scenario 6:** `FLUSHALL` → re-backfill from `contract_start` → stream repopulated; old token never resumes | `redis_sink_integration_test.cpp` | `redis sink integration 6: FLUSHALL fences the old token, a re-backfill repopulates the stream` |
| **Scenario 7:** Retention: entries older than a small fake `retention_ms` are trimmed | `redis_scripts_test.cpp` | `redis scripts: entries older than minid are trimmed, the newest never` |
| **Scenario 8:** Startup: `CONFIG SET maxmemory-policy allkeys-lru` → exit 2; proxy disabled → process waits in `Init`, no exit | `redis_startup_test.cpp` | `startup: maxmemory-policy allkeys-lru is fatal when noeviction is required (scenario 8)`<br>`startup: a rejected AUTH is fatal, not retried (scenario 8)`<br>`startup: an unreachable Redis is waited for, never exited on (scenario 8)` |
| **Scenario 9:** Lease: two sinks → exactly one leader; kill leader → other acquires within ttl + renew | `redis_sink_integration_test.cpp` | `redis sink integration 9: standby takeover within ttl + poll, old token fenced`<br>`redis sink integration 20: try_acquire_lease on real Redis returns epoch_n and old_host atomically` |
| **Scenario 10:** `emit` with `released` score → stream entry, zset member and `released_data` present; duplicate → nothing changes; `final_trim` removes only members ≤ `G_f`; `released_range` after reconnect returns full fields | `redis_scripts_test.cpp` | `redis scripts: tracked emits, final_trim and released_range after a reconnect` |
| **Scenario 11:** `ckpt.contracts` round-trip; `init` when `ckpt` exists → `EXISTS` | `redis_scripts_test.cpp` | `redis scripts: contracts manifest round-trips and init never overwrites` |
| **Scenario 12 (P1):** `SCRIPT FLUSH` between batches and toxiproxy `reset_peer` mid-batch → `ckpt` never ahead of a missing event | `redis_sink_integration_test.cpp`<br>`redis_scripts_test.cpp` | `redis sink integration 12: script flush and reset_peer never put the checkpoint ahead of an event`<br>`redis scripts: the runner reloads the scripts after a SCRIPT FLUSH` |
| **Scenario 13 (A):** Emits queued, then `released_range` → contains them; link disabled → nullopt | `redis_sink_integration_test.cpp` | `redis sink integration 13: released_range sees queued emits, and is unavailable while the link is down` |
| **Scenario 14 (B):** `maxmemory` OOM refuses `emit` whole (no key changed); after freeing memory retry lands once | `redis_scripts_test.cpp` | `redis scripts: over maxmemory emit is refused whole and the retry lands once`<br>`redis scripts: every embedded script starts with the #!lua shebang` |
| **Scenario 15 (Fencing):** Write with old token → `FENCED`, nothing written; leader expired with `epoch == token` → `RESUMED`; another acquisition in between → `FENCED` | `redis_scripts_test.cpp` | `redis scripts: a stale token is fenced on every write script`<br>`redis scripts: lease renew resumes after a blip and is fenced after a takeover` |
| **Scenario 16 (P4):** Replay 50k spilled items while renewing; lease never expires | `redis_sink_integration_test.cpp` | `redis sink integration 16: replaying 50k spilled items never lets the lease expire` |
| **Scenario 17:** `BELOW_FINALIZED`: tracked emit at block ≤ `ckpt.finalized` → rejected, nothing written | `redis_scripts_test.cpp`<br>`redis_sink_test.cpp` | `redis scripts: a tracked emit at or below ckpt.finalized is rejected and writes nothing`<br>`redis sink: BELOW_FINALIZED drops the emit, is counted, and does not fail the sink` |
| **Scenario 18 (Q1):** Lease disabled (`token "0"`), outage via toxiproxy → spill replayed, exact set; restart → spill wiped, re-sweep, exact set | `redis_sink_integration_test.cpp` | `redis sink integration 18: lease disabled, outage and restart` |
| **Scenario 19 (Q2):** Consumer-lag probe on real Redis replies | `redis_sink_integration_test.cpp` | `redis sink integration 19: consumer-group lag probe on real Redis replies` |

#### Phase 5 (`60_phase5_ops.md` §Tests) & Phase 6 (`70_phase6_testing_rollout.md` §Tests) Traceability

| Promised Scenario (`60_phase5_ops.md` & `70_phase6_testing_rollout.md` §Tests) | Test File(s) | Exact Catch2 `TEST_CASE` Name(s) |
|---|---|---|
| **Chaos (toxiproxy):** latency 5 s, `reset_peer`, timeout, bandwidth 0 on provider and Redis links → automatic recovery with exact stream | `host_test.cpp` | `host: toxiproxy chaos (latency 5s, reset_peer, timeout, bandwidth 0) on provider and Redis links recovers with an exact stream` |
| **Lease takeover & pause tolerance (D2):** standby takes over within 20 s; old leader fenced and demoted; stream gap-free and duplicate-free | `host_test.cpp`<br>`watchdog_test.cpp` | `host: lease takeover under load produces a gap-free, duplicate-free stream and fences the old leader`<br>`host: a standby takes over within the lease ttl when the leader stops, without moving the checkpoint back`<br>`host: losing the lease demotes the host to standby and it leads again after the intruder expires`<br>`watchdog: a host pause (own wake gap > interval + tolerance) does not abort` *(+ live compose `docker pause listener-a` drill verified in `60_phase5_ops.md` Slice `5-I`)* |
| **io-freeze test hook (D3):** `EVML_TEST_HOOKS`, `--test-freeze-io-after <s>` → watchdog aborts within `process_stall_ms` (exit 134) | `host_test.cpp`<br>`watchdog_test.cpp` | `host: a frozen io thread makes the watchdog abort the process (SIGABRT)`<br>`watchdog: aborts when the io tick stalls past 30 s`<br>`watchdog: real thread aborts a process whose io thread never reports` *(+ prod image `--test-freeze-io-after 2` exit 134 drill verified in `60_phase5_ops.md` Slice `5-I`)* |
| **Alert rules (§5.3.1 & §5.3.3):** every condition alert fires, re-notifies after 15 min, and resolves; every event alert fires once, respects 15 min rate limit, never sends `RESOLVED:`; all 25 rows of §5.3.3 verified | `alerts_test.cpp`<br>`alert_contract_test.cpp`<br>`snapshot_test.cpp` | `alert contract: every row of §5.3.3 has the table's title, severity, kind and details keys`<br>`alerts: a condition fires once, re-notifies every 15 min and resolves once`<br>`alerts: a standby raises only the host-local rows and drops leader state without a resolve`<br>`alerts: Redis Unreachable needs more than 60 s; Redis Misconfigured is host-local`<br>`alerts: WebSocket Stalled follows head_stall_ms of the config`<br>`alerts: provider rows: auth failure, quota exhausted (once per incident, critical with no healthy provider)`<br>`alerts: getLogs Provider Lagging needs 30+ blocks for 2 min`<br>`alerts: Finalized Gate Stuck after 60 min`<br>`alerts: Checkpoint Lag is per class and needs 15 min`<br>`alerts: sink rows: Degraded after 5 min of spill, Saturated at once`<br>`alerts: a frozen finalized gate raises the critical gate alert`<br>`alerts: Safe Sweep Unreliable needs more than 3 late logs inside an hour`<br>`alerts: Pipeline Stalled: a cursor that missed 3 progress deadlines, or a Stalled sweep`<br>`alerts: consumer rows: lagging, near trim, missing group, unknown snapshot, trimmed event`<br>`alerts: events fire once per occurrence; provider and group names are rate-limited per 15 min`<br>`host: failing over and then switching back to the primary raises Provider Failover for both, told apart by reason`<br>`host: taking the lease over from another host fires Leader Changed once and the heartbeat carries epoch_n` |
| **Alert transport:** exact JSON schema, `X-API-Key` header, 3× retry + failure metric, token never in logs | `alert_sender_test.cpp`<br>`http_client_test.cpp` | `alert sender: posts the §5.3.1 schema with the X-API-Key header`<br>`alert sender: a resolve is sent as severity info with the RESOLVED: title`<br>`alert sender: retries a non-2xx answer and succeeds on the third try`<br>`alert sender: gives up after 1 + 3 attempts, counts one failure, logs one ERROR, leaks no secret`<br>`alert sender: a transport failure is counted and leaks no secret` *(+ live `#strategy-production` webhook test verified in Slice `5-I`)* |
| **Dead-man (§5.3.2):** only leader pings; standby 0 pings; stops on saturated sink or `C_s` stalled; empty URL gives startup WARN and `"deadman":"disabled"` | `deadman_test.cpp`<br>`host_test.cpp` | `deadman: a healthy leader sends a GET with the configured User-Agent`<br>`deadman: a standby, a bad state or a saturated sink never ping`<br>`deadman: C_s stalled for 10 min while G_s moves stops the pings; a quiet chain keeps them`<br>`deadman: C_s advancing again restores the pings`<br>`deadman: an empty URL is disabled, warns at startup and never pings`<br>`host: a healthy leader pings the dead-man URL and a frozen release posts an alert to the webhook` |
| **`LogSource` adapter (`PoolLogSource`, §5.4a):** token bucket caps rate; `report_bad` moves provider toward `Down` and retries on another; `block(n)` hits ring before RPC and never asks logs provider; disagreement ⇒ no release, no quorum ⇒ nullopt (`Retry`) | `pool_log_source_test.cpp`<br>`pool_log_source_sweep_test.cpp`<br>`provider_pool_test.cpp` | `pool: call_to waits for a token instead of exceeding reconcile_rps`<br>`pool: report_bad counts toward Down and pick_getlogs_excluding skips the excluded`<br>`pool log source: report_bad steers the next get_logs away, once`<br>`pool log source: three bad reports take a provider Down`<br>`pool log source: block() never asks the provider that returned the logs`<br>`pool log source: block() answers from the ring without any RPC`<br>`pool log source: block() hash disagreeing with the ring is reported and returns nullopt`<br>`pool log source: no independent provider means nullopt, never the logs provider`<br>`pool log source under the sweeper: one block() RPC per distinct block, never on the logs provider`<br>`pool log source: address-split sub-queries across multiple providers exclude all of them on block()` |
| **Contract coverage, `--backfill-contract` (D8 lease fencing), & `--check-redis` (§5.6a):** exit 2 on uncovered contract; backfill emits finalized below `C_f` and safe/tracked in `(C_f, C_s]`, updates manifest, idempotent on second run, acquires/releases lease or exits 1 if held | `host_test.cpp`<br>`coverage_test.cpp` | `tools: --backfill-contract emits finalized below C_f and safe/released in C_f..C_s, updates manifest, is idempotent, and enforces D8 lease fencing`<br>`tools: --check-redis prints address_family and startup check results, and exits 1 when unreachable`<br>`coverage: a new contract whose history lies below C_s is reported by name` |
| **Ops HTTP dual-stack (§5.2) & `/metrics` + `/readyz` (§5.4, §6.4):** `127.0.0.1` and `[::1]` both return 200; `/metrics` includes `evml_rpc_requests_total{provider,method}` and `evml_ws_messages_total{provider,stream}` | `ops_http_server_test.cpp`<br>`host_test.cpp` | `ops http: [::] is dual-stack (IPv4 and IPv6 loopback both answer)`<br>`host: /metrics exposes every §5.4 and sink/sweeper metric and updates on scrape`<br>`host: /readyz returns 200 for leader-live, standby and lease-disabled, and 503 when saturated or gate is stuck` |
| **Phase 6 Stream Auditor (`--audit-stream`, §6.2):** clean fixture exits 0 and picks an independent provider; defect fixture reports `missing`, `phantom`, `dup_live`, `field_mismatch`, and `order_violation` and exits 1; `to_block > ckpt.finalized` exits 2 | `tools_test.cpp` | `tools: --audit-stream exits 0 on a clean stream and picks an independent provider`<br>`tools: --audit-stream detects missing, phantom, dup_live, field_mismatch, and order_violation defects and exits 1`<br>`tools: --audit-stream exits 2 when from_block > to_block or to_block > ckpt.finalized` |
| **Phase 6.1 / 6.3 Adversarial Fuzzing, Chaos Monkey & `WsReconciler` / `PendingBuffer` Hardening:** codec prefix-truncation/byte-flip/type-mutation fuzzing; `RedisSink` randomized chaos monkey (outages, partial acks, script flushes, barriers); `pipeline_property_test.cpp` G2 multi-contract/multi-stream `block_hash` & out-of-range corruption verification; `WsReconciler` catch-up disconnect reseed & `PendingBuffer` 10k-log (`10,000` logs `< 50 MB`) memory ceiling enforcement | `codec_test.cpp`<br>`redis_sink_test.cpp`<br>`pipeline_property_test.cpp`<br>`pending_buffer_test.cpp` | `codec monkey: random truncations byte flips and type mutations never crash or UB`<br>`redis sink monkey: randomized outages partial acks script flushes and barriers converge with zero loss`<br>`property: without reorgs I1 to I4 and I6 hold under retries, pauses and range limits` *(+ G2 corrupt `block_hash` & out-of-range `getLogs` across multi-contract/multi-stream)*<br>`ws reconciler: disconnect during catch-up waits for first live head before cross-checking`<br>`pending: memory ceiling at 10,000 logs stays well under 50 MB` |

---

### §6.3 Local Live Run Against Base Mainnet (2026-10-29)

1. **Startup & Provider Chain Configuration:**
   - Verified against Docker `redis:8.4` (`version="8.4.7"`, `maxmemory_policy="noeviction"`, `appendonly="yes"`, `addr_family="IPv4"`) and live Base 8453 providers from `evm_listener/.env`:
     - `ws_chain`: `quicknode` (primary), `infura` (`BASE_WSS_URL`, standby).
     - `getlogs_chain`: `infura` (primary, `max_block_range = 500`), `alchemy` (entered `Cooldown` on `HTTP 429 Monthly capacity limit exceeded`; probed `1/h`, raised `Provider Quota Exhausted` warning once with zero log spam), `ankr` (`max_block_range = 1000`, `reconcile_rps = 2`), `base_public` (`https://mainnet.base.org`, `max_block_range = 1000`, `reconcile_rps = 2`), `quicknode` (`max_block_range = 5`).
2. **Cold Catch-Up (`53238500 -> 53240211`, 1,712 blocks behind tip):**
   - `listener-a` (`EVML_HOST_ID=a`) acquired lease `1|a|...`, initialized checkpoint `53238499`, and reached `state="live"` (`C_s = 53240211`, `G_s = 53240211`, `C_f = 53239672`, `G_f = 53239672`) in **32,401 ms (32.40 s)**.
   - **Sweep & RPC Breakdown During Catch-Up:**
     - `eth_getLogs` pages: **7 pages on `infura`** (`4` safe-sweep pages covering 1,712 blocks + `3` final-sweep pages covering 1,173 finalized blocks).
     - AIMD page shrinks: **`0` range shrinks**, **`0` failure shrinks**, **`0` volume shrinks**.
     - Independent `block(n)` timestamp verification (`PoolLogSource`): **66 `eth_getBlockByNumber` calls on `ankr`** (for 66 distinct block numbers containing logs outside the fresh WS head ring, token-bucket paced at `2 RPS` with `63` throttled waits).
     - Peak sink queue depth: **`0`**. Peak disk spill: **`0` records / `0` bytes**.
3. **Steady-State `Live` Run & Redis Stream Inspection:**
   - Over the 180 s steady-state `Live` window (`53240211 -> 53240301`, 90 Base blocks):
     - `evml:{base_eurc_usdc}:events`: `XLEN = 82` (`50` finalized entries in `53238500..53239672` + `32` safe entries in `53239673..53240301`).
     - `evml:{base_eurc_usdc}:registry`: `XLEN = 0` (no new pool or gauge deployed for this shard in the 1,802-block window).
     - `evml:{base_eurc_usdc}:released`: `ZCARD = 32` (`HLEN released_data = 32`), tracking the 32 unfinalized safe emissions in `(C_f, C_s]`.
     - `/readyz` on `listener-a` returned `HTTP 200` (`{"state":"live","role":"leader","lease":"enabled","deadman":"disabled","consumer_lag":"enabled","saturated":false,"gates":{"safe":53240301,"finalized":53239672},"cursors":{"safe":53240301,"finalized":53239672}}`); `/readyz` on `listener-b` returned `HTTP 200` (`{"state":"standby","role":"standby",...}`).
4. **Live Registry `topic0` On-Chain Verification (§6.3 bullet 3):**
   - Used `--find-deploy-block` to binary-search historical Base deployment blocks (`eurc_usdc_cl50_pool` = `17827297`, `eurc_usdc_cl50_gauge` = `17827325`) and fetched the exact on-chain creation logs via `eth_getLogs`:
     - **`aero_cl_factory` (`0x5e7bb104d84c7cb9b682aac2f3d509f5f406809a`):** emitted `PoolCreated(address indexed token0, address indexed token1, int24 indexed tickSpacing, address pool)` with `topic0 = 0xab0d57f0df537bb25e80245ef7748fa62353808c54d6e528a9dd20887aed9ac2` at block `17827297` (`0x11005e1`), tx `0x29ed1118dd6980112a18d16f2f6238656a37fbf600776476b61946fb1bbf4218`, with `data = 0x000000000000000000000000e846373c1a92b167b4e9cd5d8e4d6b1db9e90ec7` (`eurc_usdc_cl50_pool` = `0xE846373C1a92B167b4E9cd5d8E4d6B1Db9E90EC7`).
     - **`aero_voter` (`0x16613524e02ad97edfef371bc883f2f5d6c480a5`):** emitted `GaugeCreated(address indexed poolFactory, address indexed votingRewardsFactory, address indexed gaugeFactory, address pool, address bribeVotingReward, address feeVotingReward, address gauge, address creator)` with `topic0 = 0xef9f7d1ffff3b249c6b9bf2528499e935f7d96bb6d6ec4e7da504d1d3c6279e1` at block `17827325` (`0x11005fd`), tx `0x64c6d063058184b69d0a4a43e90beb619b227b49750180822fcd65e17fccbf2a`, with `pool = 0xE846373C1a92B167b4E9cd5d8E4d6B1Db9E90EC7` and `gauge = 0x1f6c9d116CE22b51b0BC666f86B038a6c19900B8` (`eurc_usdc_cl50_gauge`).
5. **F3 User-Agent Verification (§6.3 bullet 4):**
   - Verified `https://mainnet.base.org` returns `HTTP 200` (no Cloudflare 1010 block) on both `eth_getBlockByNumber` and `eth_getLogs` with `User-Agent: evm_listener/0.1.1 (+https://groai.fi)`.
6. **Live Takeover & `SIGTERM` Drain (§6.3 bullet 5):**
   - Sent `SIGTERM` to `listener-a`: drained in-flight operations, flushed sink (`flushed=true`, `unflushed=0`), deleted lease key (`lease_released=true`), and exited `0` in **323 ms**.
   - `listener-b` (`EVML_HOST_ID=b`) acquired lease `2|b|4823ec7958d3996f13a9b9339dcc36a7` in **4.48 s**, fired `Leader Changed` (`"leadership moved from a to b"`), re-verified all 32 released safe entries in `(C_f, C_s]`, and reached `state="live"` in **16,620 ms (16.62 s < 20 s)**.
7. **Stream Auditor Verification (`--audit-stream 53238500 53239672`, §6.3 bullet 6):**
   - Ran `evm_listener --config /tmp/base_live_a.toml --audit-stream 53238500 53239672`:
     ```text
     {"ts":"2026-10-29T10:34:47.523Z","level":"info","component":"audit-stream","message":"audit complete","blocks":"53238500..53239672","provider":"ankr","dominant_source":"infura","canonical":50,"live":50,"missing":0,"phantom":0,"dup_live":0,"field_mismatch":0,"order_violation":0}
     ```
   - Exited `0` with **0 defects** across all 5 audit counters.

---

### §6.4 Provider Quota Measurement & Extrapolation (2026-10-29)

1. **Instrumentation Added:**
   - `evml_rpc_requests_total{provider,method}` tracks every HTTP JSON-RPC call per provider and method (`eth_getLogs`, `eth_getBlockByNumber`, `eth_blockNumber`, `eth_chainId`).
   - `evml_ws_messages_total{provider,stream}` tracks every WebSocket subscription notification per provider and stream (`heads`, `logs`).
2. **Steady-State `Live` Measurement Window (`180 s = 3.0 min`, `t0 -> t1` after catch-up):**

| Provider | Metric / Method | `t0 -> t1` (`Δ` in 180 s `Live`) | Extrapolated / Day (`× 480`) | Extrapolated / 30 Days (`× 14,400`) | Notes |
|---|---|---:|---:|---:|---|
| **`quicknode`** | `evml_ws_messages_total{stream="heads"}` | `17 -> 107` (`Δ = 90`) | **43,200 / day** | **1,296,000 / 30d** | Base 2.0 s block time (`0.5 heads/s`) |
| **`quicknode`** | `evml_ws_messages_total{stream="logs"}` | `4 -> 14` (`Δ = 10`) | **4,800 / day** | **144,000 / 30d** | Filtered to 2 safe-class pool addresses |
| **`quicknode`** | `evml_rpc_requests_total{method="eth_getBlockByNumber"}` | `1 -> 4` (`Δ = 3`) | **1,440 / day** | **43,200 / 30d** | 60 s WS-vs-HTTP safe-tag divergence check |
| **`infura`** | `evml_rpc_requests_total{method="eth_blockNumber"}` | `2 -> 36` (`Δ = 34`) | **16,320 / day** *(28,800 theoretical)* | **489,600 / 30d** *(864,000 theoretical)* | `safe_poll_ms = 3000` (shared with `ankr` when overlapping) |
| **`infura`** | `evml_rpc_requests_total{method="eth_getBlockByNumber"}` | `4 -> 45` (`Δ = 41`) | **19,680 / day** *(34,560 theoretical)* | **590,400 / 30d** *(1,036,800 theoretical)* | `safe` (3 s) + `finalized` (15 s) tag polls |
| **`infura`** | `evml_rpc_requests_total{method="eth_getLogs"}` | `7 -> 11` (`Δ = 4`) | **1,920 / day** | **57,600 / 30d** | Safe + finalized sweeps when gates advance |
| **`ankr`** | `evml_rpc_requests_total{method="eth_blockNumber"}` | `1 -> 15` (`Δ = 14`) | **6,720 / day** | **201,600 / 30d** | 60 s standby probe + shared tag polls |
| **`ankr`** | `evml_rpc_requests_total{method="eth_getBlockByNumber"}` | `66 -> 82` (`Δ = 16`) | **7,680 / day** | **230,400 / 30d** | Shared tag polls (`0/day` for `block(n)` once WS ring covers `(C_f, G_s]`) |
| **`ankr`** | `evml_rpc_requests_total{method="eth_getLogs"}` | `9 -> 12` (`Δ = 3`) | **1,440 / day** | **43,200 / 30d** | Fallback `getLogs` pages when `infura` had a call in flight |
| **`base_public`** | `evml_rpc_requests_total{method="eth_blockNumber"}` | `0 -> 3` (`Δ = 3`) | **1,440 / day** | **43,200 / 30d** | 60 s standby health probe |
| **`base_public`** | `evml_rpc_requests_total{method="eth_getBlockByNumber"}` | `0 -> 21` (`Δ = 21`) | *(0 / day in steady-state)* | *(0 / 30d in steady-state)* | Independent `block(n)` verifications for `ankr`'s 3 `eth_getLogs` pages spanning finalized blocks below the fresh WS head ring |
| **`alchemy`** | `evml_rpc_requests_total{method="eth_blockNumber"}` | `0 -> 0` (`1/h`) | **24 / day** | **720 / 30d** | Parked in `Cooldown` (`HTTP 429` monthly cap; `eth_getLogs = 1 -> 1`) |

3. **Decision Gate Evaluation (§6.4 & Job 71 §1):**
   - **QuickNode (WS primary):** **~1.48M messages+calls / 30d** (~15%–30% of QuickNode Discover 10M credits/month free tier, leaving **~70%–85% headroom**). If credit weights on QuickNode Discover ever tighten, opening **Job 71** (`[ws] liveness = "ping"` at `ping_interval_s = 300`) cuts `newHeads` traffic by **99.3%** (`1,296,000/30d -> 8,640/30d`).
   - **Infura (HTTP primary):** **~1.14M calls / 30d measured** (**~1.96M calls / 30d** worst-case if 100% of 3 s/15 s tag polls hit Infura), which is **38%–65% of Infura Core's 3,000,000 requests/month free tier** (**~35%–62% headroom**). If additional shards are added on the same Infura key, increasing `safe_poll_ms` from `3000` to `6000` immediately halves tag-polling volume (~1.08M calls/30d).
   - **Verdict:** Neither provider breaches the `< 30% headroom` gate, so `heads` mode remains the production default for go-live and **Job 71 is deferred / closed for v1** ([71_job_ws_ping_liveness.md](71_job_ws_ping_liveness.md) §11).


