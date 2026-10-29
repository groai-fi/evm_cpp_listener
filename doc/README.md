# Documentation Index (`doc/`)

This directory contains the canonical v1 architecture, operational runbooks, deployment checklists, phase specifications, future scaling research, and archived pre-implementation brainstorming drafts for `evm_listener`.

---

## 1. Canonical v1 Production Documentation

| Document | Scope |
|---|---|
| [`System_Modules_and_Architecture.md`](System_Modules_and_Architecture.md) | Canonical v1 C++20 module architecture (`core`, `rpc`, `provider`, `chain`, `pipeline`, `sink`, `ops`, `app`), threading model, state machines, and Redis Lua contract |
| [`Dynamic_Discovery.md`](Dynamic_Discovery.md) | On-chain verified Base 8453 Aerodrome Slipstream EURC/USDC contract addresses, event ABI signatures, canonical `topic0` hashes, and static-shard discovery design |
| [`runbook.md`](runbook.md) | Production operations runbook covering all 25 alerts (`60_phase5_ops.md` §5.3.3) and 7 standard operational procedures (deploy, rollback, takeover, checkpoint recovery, `--backfill-contract`, key rotation, webhook/dead-man setup) |

---

## 2. Deployment & Rollout Checklists (`doc/ops/`)

| Document | Scope |
|---|---|
| [`ops/staging_deployment_checklist.md`](ops/staging_deployment_checklist.md) | Pre-production staging verification checklist (`make staging-audit`, `make staging-deploy`, isolation guardrails, and 6 verification drills) |
| [`ops/railway_deployment_checklist.md`](ops/railway_deployment_checklist.md) | Phase 6 (§6.5–§6.7) Railway production smoke test, dual-service active-passive rollout (`evml-base-a` / `evml-base-b`), and 72-hour soak scorecard |

---

## 3. Phase Implementation Plans & Reviews (`doc/implmentation_plans/`)

| Document | Scope |
|---|---|
| [`implmentation_plans/00_master_plan.md`](implmentation_plans/00_master_plan.md) | Master architectural decisions, live provider probe matrix (`F1`–`F5`), and Phase 0–6 status |
| [`implmentation_plans/01_doc_review_findings.md`](implmentation_plans/01_doc_review_findings.md) | Pre-Phase-0 audit of early brainstorming drafts (`Design_v1.md`, `Sample_features.md`, `Preferred_tech_stack.md`) |
| [`implmentation_plans/02_dynamic_discovery_review.md`](implmentation_plans/02_dynamic_discovery_review.md) | On-chain verification of Base contract set (`PoolCreated` & `GaugeCreated`) |
| [`implmentation_plans/10_phase0_repo_toolchain.md`](implmentation_plans/10_phase0_repo_toolchain.md) | Phase 0: Devcontainer, Conan 2, CMake presets, and Docker Redis 8.4 |
| [`implmentation_plans/20_phase1_core.md`](implmentation_plans/20_phase1_core.md) | Phase 1: Config loader, presence-enabled providers, `.env` resolution, Keccak/codec, structured JSON logging |
| [`implmentation_plans/30_phase2_transport.md`](implmentation_plans/30_phase2_transport.md) | Phase 2: Boost.Beast WS/HTTP transport, full-jitter backoff, `ProviderPool`, `StaleDetector` |
| [`implmentation_plans/40_phase3_pipeline.md`](implmentation_plans/40_phase3_pipeline.md) | Phase 3: `FinalityGate`, `ReorgDetector`, AIMD `Sweeper`, `PendingBuffer`, `PipelineDriver`, I1–I6 invariants |
| [`implmentation_plans/50_phase4_sink.md`](implmentation_plans/50_phase4_sink.md) | Phase 4: Fenced Redis Lua sink (`emit`, `ckpt`, `lease`), barriers (`P1`), ephemeral disk spill (`D1`), consumer lag probe (`Q2`) |
| [`implmentation_plans/51_lua_extraction_plan.md`](implmentation_plans/51_lua_extraction_plan.md) | Phase 4 subtask: Standalone `.lua` scripts + `luacheck`/`luac5.1` linter |
| [`implmentation_plans/60_phase5_ops.md`](implmentation_plans/60_phase5_ops.md) | Phase 5: Watchdog, `/healthz` + `/readyz` + `/metrics`, n8n alerts, healthchecks.io dead-man, `Host` active-passive runtime |
| [`implmentation_plans/70_phase6_testing_rollout.md`](implmentation_plans/70_phase6_testing_rollout.md) | Phase 6: 529-test traceability table, `--audit-stream`, Base mainnet live run, §6.4 quota measurement, rollout & 72h soak |
| [`implmentation_plans/71_job_ws_ping_liveness.md`](implmentation_plans/71_job_ws_ping_liveness.md) | Companion Job 71: Opt-in WebSocket `ping` liveness mode *(evaluated at §6.4 gate and deferred/closed for v1)* |

---

## 4. Future Scaling & Ecosystem Research (`doc/research/`)

| Document | Scope |
|---|---|
| [`research/Redis_Lua_Streaming_and_Kafka_Tooling.md`](research/Redis_Lua_Streaming_and_Kafka_Tooling.md) | Scaling Redis Streams + Lua toward Kafka-grade durability and analytics (Apache Kvrocks on NVMe, Redpanda Connect / Benthos ETL, embedded DuckDB Parquet archival, BullMQ / Asynq) |

---

## 5. Archived Pre-Implementation Brainstorming (`doc/archive/brainstorming/`)

> [!NOTE]
> The files in `doc/archive/brainstorming/` are early pre-Phase-0 conceptual drafts preserved for historical context. See [`implmentation_plans/01_doc_review_findings.md`](implmentation_plans/01_doc_review_findings.md) for the design review that superseded them.

| Document | Historical Purpose |
|---|---|
| [`archive/brainstorming/Design_v1.md`](archive/brainstorming/Design_v1.md) | Initial Chinese v1 sharded listener proposal (superseded by `00_master_plan.md` & `System_Modules_and_Architecture.md`) |
| [`archive/brainstorming/Sample_features.md`](archive/brainstorming/Sample_features.md) | Early state-machine and deduplication sketch (superseded by `40_phase3_pipeline.md` & `50_phase4_sink.md`) |
| [`archive/brainstorming/Preferred_tech_stack.md`](archive/brainstorming/Preferred_tech_stack.md) | Early C++20 DeFi library survey including downstream execution bot notes (`evmone`, `secp256k1`, `intx`) |
