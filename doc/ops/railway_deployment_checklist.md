# Phase 6 Railway Production Deployment, Rollout & 72h Soak Checklist (§6.5–§6.7)

> **Target Shard:** `base_eurc_usdc` (Base `chainId = 8453`)
> **Config-as-Code:** [`railway.json`](../../railway.json) · [`docker/Dockerfile.prod`](../../docker/Dockerfile.prod) · [`mk/60-prod.mk`](../../mk/60-prod.mk) · [`evm_listener/config/base.toml`](../../evm_listener/config/base.toml)
> **Ref Plans:** [`70_phase6_testing_rollout.md`](../implmentation_plans/70_phase6_testing_rollout.md) (§6.5–§6.7, §6.11) · [`60_phase5_ops.md`](../implmentation_plans/60_phase5_ops.md) (§5.0, §5.6, §5.9) · [`doc/runbook.md`](../runbook.md)
> **Pre-Prod Prerequisite:** Complete [`doc/ops/staging_deployment_checklist.md`](staging_deployment_checklist.md) first, and stop/pause the staging listeners before starting §6.5 if staging shares RPC API keys with production.

---

## 0. Pre-Deploy Git, CI & Image Gate

Before deploying to Railway production:
- [ ] All local Phase 6 changes are committed and pushed to GitHub.
- [ ] GitHub Actions CI ([`.github/workflows/ci.yml`](../../.github/workflows/ci.yml): `test` + `prod-image`) is green.
- [ ] Local production image audit passes:
  ```bash
  make prod-audit
  ```
- [ ] Staging verification ([`doc/ops/staging_deployment_checklist.md`](staging_deployment_checklist.md)) has passed and staging containers are stopped/paused (if sharing QuickNode/Infura keys) so production has 1× RPC quota.

---

## 1. Prerequisites & Railway Project Setup (§6.11 & Plan 60 §5.6)

### 1.1 Railway Redis 8.4 Service
- [ ] Provision a **Redis** service (`redis:8.4`) inside the Railway production project/environment.
- [ ] Ensure Redis starts with persistence and `noeviction` enabled (`--appendonly yes --maxmemory-policy noeviction`), or apply and persist via `redis-cli`:
  ```bash
  redis-cli -u "$REDIS_URL" CONFIG SET appendonly yes
  redis-cli -u "$REDIS_URL" CONFIG SET maxmemory-policy noeviction
  redis-cli -u "$REDIS_URL" CONFIG REWRITE
  ```
- [ ] Verify with `CONFIG GET`:
  ```bash
  redis-cli -u "$REDIS_URL" CONFIG GET appendonly maxmemory-policy
  # Expected:
  # 1) "appendonly"
  # 2) "yes"
  # 3) "maxmemory-policy"
  # 4) "noeviction"
  ```
  > [!IMPORTANT]
  > `evm_listener` enforces `require_noeviction = true` at startup ([`base.toml:142`](../../evm_listener/config/base.toml#L142)): it **exits 2 (`StartupFatal`)** if `maxmemory-policy != noeviction` and raises **`Redis Misconfigured` (`critical`)** if `appendonly != yes`.

### 1.2 External Alerting & Dead-Man Setup
- [ ] **n8n Webhook (`#strategy-production`):** Confirm workflow **"EVM Listener Alert Webhook"** is active and accepts `POST` with header `X-API-Key: <N8N_ALERT_WEBHOOK_TOKEN>`.
- [ ] **healthchecks.io Dead-Man Check:**
  - Create check **`evm-listener-base-eurc-usdc`**.
  - Set **Period = `1 min`** (`60 s`) and **Grace Time = `2 min`** (`120 s`) → pages after ~3 min of leader silence.
  - Route check notifications to Slack `#strategy-production`.
  - Copy the ping URL (`https://hc-ping.com/<UUID>`) for `DEADMAN_PING_URL`.

### 1.3 Service Creation & Environment Variables (`evml-base-a` and `evml-base-b`)
Create `evml-base-a` first for §6.5 smoke (and `evml-base-b` in §6.6 Step 2), pointing to the repo root ([`railway.json`](../../railway.json)):
- **No Custom Start Command:** Leave Railway's `startCommand` empty so the image's `ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/evm_listener"]` runs as PID 1.
- **No Volume Attached:** Spill uses ephemeral container disk (`/tmp/evml-spill`) and is wiped on start (`D1`).
- **No Public Domain:** Keep HTTP port `9464` (`/healthz`, `/readyz`, `/metrics`) strictly on `*.railway.internal`.
- **Regions:** Place `evml-base-a` and `evml-base-b` in different regions if supported by your Railway plan.

| Variable Name | `evml-base-a` Value | `evml-base-b` Value | Purpose |
|---|---|---|---|
| `EVML_HOST_ID` | `a` | `b` | Lease token identity & alert/heartbeat `host` tag |
| `REDIS_URL` | `${{Redis.REDIS_URL}}` | `${{Redis.REDIS_URL}}` | Private IPv6 URL (`redis://...@redis.railway.internal:6379`) |
| `RAILWAY_DEPLOYMENT_DRAINING_SECONDS` | `30` | `30` | Gives `tini` + `evm_listener` 30 s between `SIGTERM` and `SIGKILL` to drain & release lease |
| `BASE_WSS_URL_QUICKNODE` | `<from .env>` | `<from .env>` | WS #1 primary |
| `BASE_RPC_URL_QUICKNODE` | `<from .env>` | `<from .env>` | HTTP `getLogs` last resort (`max_getlogs_range = 5`) |
| `BASE_WSS_URL` | `<from .env>` | `<from .env>` | WS #2 failover (Infura WSS) |
| `BASE_RPC_URL` | `<from .env>` | `<from .env>` | HTTP `getLogs` #1 primary (Infura HTTP, `max_getlogs_range = 500`) |
| `BASE_RPC_URL_ANKR` | `<from .env>` | `<from .env>` | HTTP `getLogs` #2 (`max_getlogs_range = 10000`) |
| `BASE_RPC_URL_ALCHEMY` | `<from .env>` *(optional)* | `<from .env>` *(optional)* | Auto-cools down for 1h on 429 quota exhaustion and rejoins when reset |
| `N8N_ALERT_WEBHOOK_URL` | `<from .env>` | `<from .env>` | n8n webhook endpoint |
| `N8N_ALERT_WEBHOOK_TOKEN` | `<from .env>` | `<from .env>` | Sent in `X-API-Key` header (never logged) |
| `DEADMAN_PING_URL` | `https://hc-ping.com/<UUID>` | `https://hc-ping.com/<UUID>` | Same URL on both services; **only the active healthy leader pings** |

---

## 2. §6.5 Railway Smoke Checklist (`evml-base-a` Only)

Deploy `evml-base-a` (`make prod-deploy` or via Railway GitHub auto-deploy) and complete all 8 checks before deploying `evml-base-b`:

- [ ] **1. IPv6 Redis Connectivity (`--check-redis`):**
  Run as a one-off command in the `evml-base-a` container (`railway ssh --service evml-base-a`):
  ```bash
  /usr/local/bin/evm_listener --config /app/config/base.toml --check-redis
  ```
  - **Accept:** Exits `0`, reaches `redis.railway.internal`, prints `addr_family="IPv6"` (or `IPv4` if dual-stack), `redis_version="8.4..."`, `maxmemory_policy="noeviction"`, `appendonly="yes"`.
  - **Fallback if DNS/IPv6 fails:** Stop and report before changing code (Plan 50 §4.3 fallback: resolve with `getaddrinfo(AF_UNSPEC)`).

- [ ] **2. Deploy Healthcheck (`/healthz`):**
  - **Accept:** Railway deploy healthcheck (`GET /healthz`, timeout `120 s`) turns green and deployment transitions to **Active** even while `evml-base-a` is still in `Init`, `Recovering`, or `CatchingUp`.

- [ ] **3. `SIGTERM` Graceful Drain & Lease Release:**
  Trigger a redeploy/restart of `evml-base-a` and inspect the Shutdown logs:
  - **Accept:** Logs show signal receipt (`SIGTERM`), pipeline drain, sink flush (`"flushed":true`, `"unflushed":0`), lease release (`"lease_released":true`), and clean `exit 0` inside the 30 s window.

- [ ] **4. Structured JSON Logs in Railway Log Explorer:**
  - **Accept:** Railway Log Explorer parses stdout JSON lines cleanly, with `level` (`info`, `warn`, `error`, `critical`) and `message` filterable as structured attributes.

- [ ] **5. 1-Hour Secret Leak Audit:**
  After `evml-base-a` has run for 1 hour, export/grep the container logs against every secret token in `.env` (QuickNode token, Infura project ID, Ankr key, Alchemy key, `N8N_ALERT_WEBHOOK_TOKEN`, `DEADMAN_PING_URL` UUID, Redis password):
  ```bash
  railway logs --service evml-base-a | grep -F -e "<QUICKNODE_TOKEN>" -e "<INFURA_ID>" -e "<ANKR_KEY>" -e "<N8N_TOKEN>" -e "<HC_UUID>"
  ```
  - **Accept:** **0 matches.**

- [ ] **6. Live n8n Alert Delivery to `#strategy-production`:**
  Send a one-off `severity=info` test payload from inside the container to `$N8N_ALERT_WEBHOOK_URL`:
  ```bash
  curl -sS -X POST "$N8N_ALERT_WEBHOOK_URL" \
    -H "Content-Type: application/json" \
    -H "X-API-Key: $N8N_ALERT_WEBHOOK_TOKEN" \
    -d '{"severity":"info","service":"evm-listener","shard":"base_eurc_usdc","host":"a","title":"Railway Smoke Test","message":"Phase 6.5 manual webhook check","ts":"2026-10-29T13:00:00Z","details":{"check":"6.5"}}'
  ```
  - **Accept:** HTTP `2xx` response and message appears in `#strategy-production`.

- [ ] **7. Dead-Man's Switch (`healthchecks.io`) Fire & Page Drill:**
  1. Confirm `evm-listener-base-eurc-usdc` turns **green** within 2 minutes of `evml-base-a` entering `CatchingUp` / `Live`.
  2. Pause/stop `evml-base-a` for ~3.5 minutes → **Accept:** `healthchecks.io` transitions to **Down** and pages `#strategy-production` within ~3 minutes.
  3. Resume `evml-base-a` → **Accept:** check recovers to **Up** within 60–90 seconds.

- [ ] **8. Private Networking Verification (`[::]:9464`):**
  - Confirm no public domain is attached to `evml-base-a`.
  - From another service in the Railway project (e.g., `evml-base-b` or `railway ssh`), query:
    ```bash
    curl -sS "http://evml-base-a.railway.internal:9464/readyz"
    curl -sS "http://evml-base-a.railway.internal:9464/metrics" | head -n 25
    ```
  - **Accept:** Both return HTTP `200` over `.railway.internal`.

---

## 3. §6.6 Production Rollout (`evml-base-a` + `evml-base-b` + Bot B)

> [!NOTE]
> **Cold Catch-Up Note (`start_block = 52060000`):** On an empty Redis instance, `evml-base-a` catches up from block `52060000` (~1.18M blocks). Because historical blocks outside the 1,024-block WS ring require independent `eth_getBlockByNumber` timestamp verification paced at `reconcile_rps = 2`, initial catch-up takes ~60–95 minutes. During that initial backfill, `Checkpoint Lag` (`G_s - C_s > 300` for `> 15 min`) will fire once at the 15-minute mark and automatically send `RESOLVED: Checkpoint Lag` once `evml-base-a` reaches `Live`.

- [ ] **Step 1 — `evml-base-a` Reaches `Live`:**
  Wait for `evml-base-a` to complete catch-up:
  - `/readyz` returns HTTP `200` with `{"state":"live","role":"leader","saturated":false,...}`.
  - `G_s - C_s` and `G_f - C_f` are both `< 1 page` (`< 500` blocks).

- [ ] **Step 2 — Deploy `evml-base-b` (`EVML_HOST_ID=b`):**
  Deploy `evml-base-b` with the identical image and variables (`EVML_HOST_ID=b`, no public domain, no volume):
  - **Accept:** `evml-base-b` enters standby (`"state":"standby","role":"standby"`), `/healthz` and `/readyz` both return `200`, and `evml-base-b` opens **0 WebSocket connections** and sends **0 dead-man pings**.

- [ ] **Step 3 — Forced Takeover Drill (`a -> b`):**
  Restart `evml-base-a` in Railway:
  - **Accept:**
    1. `evml-base-b` acquires the lease and reaches `"state":"live","role":"leader"` within **≤ 20 s** (typically ~4–17 s).
    2. `Leader Changed` (`warning`, `"old_host":"a","new_host":"b"`) fires **once** to `#strategy-production`.
    3. `evml-base-a` comes back up and settles into `"state":"standby","role":"standby"`.
    4. Record the `takeover_block` (`C_s` at the moment of handover).

- [ ] **Step 4 — Audit the Takeover Window (`--audit-stream`):**
  Once `ckpt.finalized` advances past `takeover_block + 20` (~20 minutes on Base), run:
  ```bash
  /usr/local/bin/evm_listener --config /app/config/base.toml \
    --audit-stream $((TAKEOVER_BLOCK - 50)) $((TAKEOVER_BLOCK + 50))
  ```
  - **Accept:** Exits `0` with `missing=0 phantom=0 dup_live=0 field_mismatch=0 order_violation=0`.

- [ ] **Step 5 — Bot B Consumer Group Onboarding:**
  Start Bot B so it creates consumer group `strat:aero_eurcusdc:botB_v1` on `evml:{base_eurc_usdc}:events` and begins reading via `XREADGROUP`:
  - **Accept:** Any `Consumer Group Missing` warning resolves (`RESOLVED: Consumer Group Missing`) within **≤ 10 minutes** of group creation, and `evml_consumer_undelivered_age_seconds{group="strat:aero_eurcusdc:botB_v1"}` stays near `0`.

---

## 4. §6.7 72-Hour Production Soak Scorecard (Master Gate)

Start the 72-hour clock (`T = 0h`) after §6.6 Step 5 completes. **Every row below must pass; any failed row restarts the 72h clock after root-cause fix (`P6-3`).**

| # | Soak Check | Pass Condition | Verification Command / Source | Status (`0h..72h`) |
|---|---|---|---|---|
| 1 | **Stream exactness (Window 1: `0h..24h`)** | `--audit-stream <b_0h> <b_24h>` exits `0` (`missing=0 phantom=0 dup_live=0 field_mismatch=0`) | Run inside container once `ckpt.finalized ≥ b_24h` | [ ] Pending |
| 2 | **Mid-soak takeover drill (`~36h`)** | Restart current leader at `T ≈ 36h`; standby leads in `≤ 20 s`; `--audit-stream` over takeover window exits `0` once finalized | Railway deploy log + `--audit-stream` | [ ] Pending |
| 3 | **Stream exactness (Window 2: `24h..48h`)** | `--audit-stream <b_24h> <b_48h>` exits `0` | Run inside container once `ckpt.finalized ≥ b_48h` | [ ] Pending |
| 4 | **Stream exactness (Window 3: `48h..72h`)** | `--audit-stream <b_48h> <b_72h>` exits `0` | Run inside container once `ckpt.finalized ≥ b_72h` | [ ] Pending |
| 5 | **Unplanned restarts** | `0` watchdog aborts (exit `134`) and `0` crash restarts (`exit != 0`) across both `a` and `b` | Railway deploy / crash history | [ ] Pending |
| 6 | **Critical alerts** | `0` unexplained `critical` alerts in `#strategy-production` | Slack `#strategy-production` / n8n | [ ] Pending |
| 7 | **Freshness** | `G_s - C_s ≤ 500` and `G_f - C_f ≤ 500` for `> 99%` of 1-min samples | `/metrics` (`evml_gate_block - evml_checkpoint_block`) | [ ] Pending |
| 8 | **Sink health** | `evml_sink_spill_bytes == 0` for `> 99%` of samples; `0` `Sink Saturated` alerts | `/metrics` | [ ] Pending |
| 9 | **Memory stability** | Container RSS growth over 72 h `< 10%` after hour 1 (no memory leak) | Railway service metrics | [ ] Pending |
| 10 | **Provider quota headroom (§6.4 24h check)** | Record 24 h dashboard credits/requests on QuickNode & Infura; monthly headroom stays `≥ 30%` on every provider | QuickNode & Infura billing dashboards + `/metrics` | [ ] Pending |
| 11 | **Downstream consumer lag** | Bot B `evml_consumer_undelivered_age_seconds < 43200` (`12 h`) throughout | `/metrics` | [ ] Pending |
| 12 | **Dead-man's switch** | `0` unplanned `healthchecks.io` pages during the 72 h window | `healthchecks.io` event log | [ ] Pending |

