# EVM Listener v1 (Base): Master Plan, rev 6

> Generic, config-driven, 24/7 C++20 EVM log listener → Redis Streams. No transaction signing.
> **Prod: Railway · Local: Docker devcontainer + Docker Redis 8.4 · Chain: Base 8453 · Output: raw logs · Finality per contract: pools at `safe`, everything else `finalized`.**
> Shard: `base_eurc_usdc` (Aerodrome Slipstream EURC/USDC), config `config/base.toml`.

## Plan files (execute in order)
| # | File | Scope | Gate to next |
|---|---|---|---|
| 00 | this file | Decisions, warnings, architecture | ✅ all questions answered → **approve Phase 0** |
| 01 | [01_doc_review_findings.md](01_doc_review_findings.md) | Errors in Design_v1 / Sample_features / tech stack / repo | — |
| 02 | [02_dynamic_discovery_review.md](02_dynamic_discovery_review.md) | On-chain verification; final v1 contract set | ✅ |
| 10 | [10_phase0_repo_toolchain.md](10_phase0_repo_toolchain.md) | Env files, Conan, CMake, devcontainer + Docker Redis 8.4 | `--version` builds in container |
| 20 | [20_phase1_core.md](20_phase1_core.md) | Config (per-contract finality, presence-enabled providers), env, codec, logging | Unit green |
| 30 | [30_phase2_transport.md](30_phase2_transport.md) | WS/HTTP, backoff, provider pool (WS + getLogs chains), head tracker | Mock tests green |
| 40 | [40_phase3_pipeline.md](40_phase3_pipeline.md) | State machine, two gates, sweeps, safe-reorg revert, dedup, checkpoint | Scenario tests green |
| 50 | [50_phase4_sink.md](50_phase4_sink.md) | Redis Lua sink (fenced), barriers, spill (process-scoped), lease fencing | Redis-outage tests green |
| 60 | [60_phase5_ops.md](60_phase5_ops.md) | Railway: watchdog, n8n alerts, **healthchecks.io dead-man**, active-passive | Chaos tests green |
| 70 | [70_phase6_testing_rollout.md](70_phase6_testing_rollout.md) | Tests, live smoke, soak, README with 3 finality states | 72 h soak clean |

### Companion Jobs, Reviews & Task Plans
| # | File | Scope |
|---|---|---|
| 51 | [51_lua_extraction_plan.md](51_lua_extraction_plan.md) | Phase 4 subtask: extract Redis Lua scripts to real `.lua` files |
| 71 | [71_job_ws_ping_liveness.md](71_job_ws_ping_liveness.md) | Post-Phase 6 job: opt-in WebSocket `ping` liveness *(deferred / closed for v1 after §6.4 gate)* |
| 80 | [80_phase3d_review.md](80_phase3d_review.md) | Phase 3d code & architecture review |
| 81 | [81_phase4_review.md](81_phase4_review.md) | Phase 4 Redis sink design review |
| 82 | [82_phase4_impl_review.md](82_phase4_impl_review.md) | Phase 4 implementation review (steps 0–4e) |
| 90 | [90_phase5_readiness_and_tasks.md](90_phase5_readiness_and_tasks.md) | Phase 5 readiness assessment and worker implementation slices |


## 1. Decisions log
| Topic | Decision | Plan impact |
|---|---|---|
| Finality | **Pools (Swap/Mint/Burn/Collect) at `safe`**; gauge, factory, Voter at `finalized` | Two gates and sweeps, `released` zset, `revert` path for the safe class |
| Cross-host | **Active-passive via the Redis lease** `evml:{base_eurc_usdc}:leader` (accepted) | 1× RPC cost; standby takes over in ≤ 20 s; epoch fenced (r6, P3) |
| Alerts | n8n webhook → Slack `#strategy-production` | `N8N_ALERT_WEBHOOK_URL` / `N8N_ALERT_WEBHOOK_TOKEN` (`X-API-Key`) |
| Dead-man's switch | **healthchecks.io**, `DEADMAN_PING_URL` (already in `.env`) | Ping every 60 s, **leader-only and only when healthy**. Check configured as period 1 min, grace 2 min → page after ~3 min |
| WS providers | **QuickNode → Infura** | Alchemy joins only if `BASE_WSS_URL_ALCHEMY` is set |
| getLogs providers | **Infura → Ankr → public `mainnet.base.org` → QuickNode** (Alchemy slots in after Infura when its quota resets) | A provider is **enabled iff its env var is set**; quota-exhausted providers cool down and re-probe automatically |
| Redis | Railway 8.4.0, local 8.6.2; compose pins `redis:8.4`; **minimum 7.0 (Lua shebang flags)**; `noeviction` on | AOF checked at startup; OOM refused before script execution; ephemeral disk spill replaces durable WAL (r6, D1) |
| Contracts | ts=1 + ts=50 pools, ts=50 gauge, CLFactory + Voter (`registry`) | ≈ 9.7k events/day |
| Build | Devcontainer bookworm; apt dev libs removed | Conan 2 is the only library source |
| Naming | `base_eurc_usdc` (accepted) | Keys `evml:{base_eurc_usdc}:{events,registry,ckpt,dedup:<id>,released,released_data,leader,epoch,hb:<host>}` |

## 2. ⚠️ Findings from today's live probes (need no decision unless noted)

**F1. Provider capability matrix (measured 2026-10-05, updated 2026-10-29; URLs redacted):**
| Provider | Env | WSS | getLogs max range | safe lag (sample) | Role |
|---|---|---|---|---|---|
| QuickNode | `BASE_*_QUICKNODE` | ✅ | **5** (`-32615`, Discover plan) | −9 | WS #1, getLogs last resort |
| Infura | `BASE_RPC_URL`, `BASE_WSS_URL` | ✅ (`BASE_WSS_URL` set & verified 2026-10-29, see F4) | 500 (2000 → 429) | −13 | WS #2, getLogs #1 |
| Ankr | `BASE_RPC_URL_ANKR` | not configured | **≥ 10,000** (2,331 logs in 2.4 s) | **−54** | getLogs #2 |
| Public Base | literal `https://mainnet.base.org` (no secret) | — | 500 (`-32614` above) | — | getLogs #3, best effort |
| Alchemy | `BASE_RPC_URL_ALCHEMY` | not configured | 2000 | — | quota exhausted; auto-rejoins |

**F2. A lagging getLogs provider would silently drop events. Fixed in design.** Ankr's `safe` tag trailed the others by ~45 blocks. Suppose the sweep asks a lagging node for `(C_s, G_s]` with `G_s` taken from QuickNode. The node returns `[]` for blocks it hasn't seen yet, and that empty answer is indistinguishable from "no events".
- Fix (30 §2.6, 40 §3.5): finality tags are polled **on the getLogs provider itself**, and a sweep's `toBlock` can never exceed that provider's own tag. The WS provider's tags become the 60 s cross-check.
- Side effect: tag polling moves from QuickNode to Infura. QuickNode then only carries the WS (≈ 43k newHeads notifications/day, the number you quoted).

**F3. The public endpoint is Cloudflare-fronted.** A default Python `User-Agent` got HTTP 403 "error code 1010", while curl worked. The HTTP client sends an explicit `User-Agent: evm-listener/<version>`, and the live smoke test checks it.

**F4. Your env file, as it is today (updated 2026-10-29):**
- `BASE_WSS_URL` (Infura WS) is **now set and verified** (`ws_chain = [quicknode, infura]`, confirmed in [70_phase6_testing_rollout.md](70_phase6_testing_rollout.md) §6.3/§6.11).
- `BASE_RPC_URL_ALCHEMY` is **already set**, so under the "enabled iff env set" rule Alchemy is active from day 1. It immediately hits `QuotaExhausted`, sits in a 1 h cooldown, re-probes hourly, and **sends one `warning` per incident with no 15-min re-notify spam** (verified in §6.3 live run). When the quota resets it rejoins on its own.

**F5. Quota budget (measured 2026-10-29 in [70_phase6_testing_rollout.md](70_phase6_testing_rollout.md) §6.4).** Expected vs. measured steady-state `Live` load:
- **QuickNode (WS primary):** `43,200` `newHeads`/day + `4,800` `logs`/day + `1,440` HTTP `eth_getBlockByNumber("safe")`/day = **`49,440` msgs+calls/day (`1.48M / 30d`)**, leaving **~70%–85% headroom** on the 10M credits/month Discover plan.
- **Infura (HTTP primary):** **`37,920` calls/day measured (`1.14M / 30d`; `1.96M / 30d` worst-case** if 100% of tag polls hit Infura), leaving **~35%–62% headroom** on the 3M requests/month Core plan.

Neither provider breaches the `< 30% headroom` gate, so Job 71 ([71_job_ws_ping_liveness.md](71_job_ws_ping_liveness.md)) is **deferred / closed for v1**. Exceeding a limit degrades into failover; it does not lose data.

**Updated status on previously open items (2026-10-29):**
- **W4 (`doc/Dynamic_Discovery.md`):** ✅ **done & live-confirmed on Base mainnet** — updated with the 8-parameter `GaugeCreated` ABI and canonical `topic0`s, verified on-chain at Base block `17827297` (`PoolCreated`, tx `0x29ed1118dd6980112a18d16f2f6238656a37fbf600776476b61946fb1bbf4218`) and block `17827325` (`GaugeCreated`, tx `0x64c6d063058184b69d0a4a43e90beb619b227b49750180822fcd65e17fccbf2a`).
- **Still open on your side for §6.5–§6.7:** Railway project + Redis `appendonly yes` / `noeviction` setup (startup check warns if `appendonly` is off and exits 2 if `noeviction` is off).

## 3. Architecture (rev 6)
```
   QuickNode WSS (active) ──X──► Infura WSS (failover)                  [1 WS at a time]
            │ newHeads + logs → liveness, hash ring (independent canonical hashes), WS-vs-sweep cross-check
   getLogs provider (Infura → Ankr → public → QuickNode, ≠ active WS when possible)
            │ its OWN safe (4 s) / finalized (12 s) tags  ── cross-checked vs WS provider every 60 s
            ▼                                   G_s                 G_f
   Safe sweep  : getLogs (C_s, G_s]  pools       ──► emit finality=safe + ZADD released + HSET released_data
   Final sweep : getLogs (C_f, G_f]  all addrs   ──► emit finality=finalized; diff released → revert(before image)/late-emit
   Re-verify   : getLogs (C_f, C_s]  pools       ──► immediately on safe-reorg signal; same diff
            ▼
   last-type dedup ─► sink thread (barriers P1) ─► Redis 8.4, min 7.0 (Railway, private IPv6)
                  │ Lua: shebang flags + atomic dedup (emit iff type changed) + XADD MINID~48h (+ZADD/HSET released) · ckpt HSET-if-greater {safe, finalized, contracts}
                  └─ Redis down → spill on ephemeral disk → ordered replay (same process only; r6, D1)
   Lease evml:{base_eurc_usdc}:{leader,epoch} ── evml-base-a (leader) / evml-base-b (standby, ~0 RPC; fenced P3)
   Watchdog → self-abort · n8n alerts (X-API-Key) · healthchecks.io ping (leader ∧ healthy, 60 s) · consumer-group lag probe (Q2)
```

## 4. Mandatory feature map
| Requirement | Mechanism | Phase |
|---|---|---|
| State machine | process: Init→Recovering→CatchingUp→Live→Draining; session: Connecting→Subscribing→Live→Closed; role: Leader/Standby | 2/3/5 |
| Full-jitter reconnect backoff | base 250 ms, cap 30 s; `Retry-After`; QuotaExhausted 1 h cooldown | 2 |
| Paged backfill | AIMD `getLogs`, per-provider max (Ankr 10k, Infura/public 500, QuickNode 5) | 3 |
| Checkpoint | Redis hash `{safe, finalized}`, set-if-greater; resume from each; atomic `init_checkpoint`; `contracts` manifest (sorted list) → exit 2 + `--backfill-contract` on an uncovered new contract | 3/4/5 |
| Multi-source dedup | per-event last-type state (in-process + atomic Lua), key `dedup:{event_id}`; emit only when type changes | 3/4 |
| Real-time data | QuickNode WSS; safe-class output ≈ 20–30 s | 2/3 |
| Watchdog | session stall · pipeline progress · process self-abort · external dead-man | 2/5 |
| Reorg handling | two gates, hash ring, immediate re-verify of `(C_f, C_s]`, final-sweep diff → full-field revert | 3 |
| Downstream fallback | bounded queue → process-scoped ephemeral spill buffer → replay → backpressure | 4 |
| Head-lag alert | WS vs getLogs-provider head, gate age per class → n8n | 2/5 |
| Cross-provider redundancy | 2 WS providers, 4–5 getLogs providers, independent reconcile; bad responses penalise the provider and retry elsewhere; canonical hash from a second source | 2/3/5 |
| Cross-host redundancy | 2 Railway services, Redis lease, active-passive, epoch-fenced | 4/5 |

## 5. Status
- Phases 0–2 done and committed. Phase 3a–3d done and committed (`105d151`).
- Phase 3e done and committed (`7036249`): N1 dedup, `report_bad`, wide-range and volume shrink, `reverify_released()`, contract-coverage manifest, `PipelineDriver`, and seeded I1–I6 property tests.
- **Phase 4 (Redis sink) rev 6 done and committed** ([50_phase4_sink.md](50_phase4_sink.md), review [81_phase4_review.md](81_phase4_review.md), impl review [82_phase4_impl_review.md](82_phase4_impl_review.md)). Steps 0–4e complete; 359/359 via `make test` at the end of 4e. Later commits on `feat/lua-seperation` embed the Lua scripts from `.lua` files ([51_lua_extraction_plan.md](51_lua_extraction_plan.md)) and remove the legacy `common/` library.
- **Phase 5 (ops) complete (2026-10-29)** ([60_phase5_ops.md](60_phase5_ops.md) rev 9, task list [90_phase5_readiness_and_tasks.md](90_phase5_readiness_and_tasks.md)). Slices `5-0` through `5-I` plus `Stage 4` pre-Phase-6 hardening & C++ refactoring complete; **521/521** tests green via `make test`.
- **Phase 6 local verification & readiness complete (2026-10-29)** ([70_phase6_testing_rollout.md](70_phase6_testing_rollout.md)):
  - **§6.1 Test completeness audit:** `529/529` Catch2 tests passing in Debug (`EVML_SANITIZE=ON`, `EVML_TEST_HOOKS=ON`), `528/528` passing in Release (`-O3`), and `20×` timing-sensitive flake audit (`[redis_serial]`, `[watchdog]`, lease, and takeover tests) = **0 flakes**; full traceability table recorded in `70_phase6_testing_rollout.md`.
  - **§6.2 Stream auditor (`--audit-stream`):** C++ subcommand implemented (`evm_listener --config <file> --audit-stream <from_block> <to_block>`) and verified against Base mainnet and unit defect fixtures (`missing`, `phantom`, `dup_live`, `field_mismatch`, `order_violation`).
  - **§6.3 Local live run (Base mainnet):** `listener-a` + `listener-b` cold catch-up (`32.40 s` for 1,712 blocks, `0` shrinks, `0` spill), on-chain `PoolCreated` (block `17827297`) & `GaugeCreated` (block `17827325`) `topic0` live-confirmed, F3 Cloudflare `User-Agent` verified (`HTTP 200` on `mainnet.base.org`), clean `SIGTERM` drain (`323 ms`), standby takeover to `state="live"` in **`16.62 s (< 20 s)`**, and `--audit-stream 53238500 53239672` exited `0` (`50/50` canonical entries, `0` defects).
  - **§6.4 Provider quota measurement & Job 71 decision gate:** `evml_rpc_requests_total{provider,method}` + `evml_ws_messages_total{provider,stream}` measured in steady-state `Live` (`QuickNode` `1.48M / 30d` = ~70%–85% headroom; `Infura` `1.14M / 30d` measured, `1.96M / 30d` worst-case = ~35%–62% headroom). Job 71 ([71_job_ws_ping_liveness.md](71_job_ws_ping_liveness.md)) evaluated and **deferred / closed for v1**.
  - **§6.8 Consumer contract & §6.9 Ops runbook:** repo-root [README.md](../../README.md) (§5 consumer contract) and [doc/runbook.md](../runbook.md) shipped and reviewed.
  - **Remaining for Phase 6 closure:** §6.5 Railway smoke, §6.6 production rollout, and §6.7 72 h soak. Nothing is committed without your instruction.
