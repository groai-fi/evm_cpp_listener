# Staging Server Deployment & Pre-Production Verification Checklist

> **Target Shard:** `base_eurc_usdc` (Base `chainId = 8453`)
> **Staging Build Automation:** [`mk/50-staging.mk`](../../mk/50-staging.mk) · [`docker/Dockerfile.prod`](../../docker/Dockerfile.prod) · [`evm_listener/config/base.toml`](../../evm_listener/config/base.toml)
> **Next Step After Staging Passes:** [`doc/ops/railway_deployment_checklist.md`](railway_deployment_checklist.md) (§6.5–§6.7 Production Rollout & 72h Soak)

---

## 0. Staging Isolation Guardrails (Read Before Starting)

> [!CAUTION]
> **Never point a staging listener at the production Redis instance.** Because `base.toml` uses shard id `base_eurc_usdc`, connecting a staging process to production Redis will compete for `evml:{base_eurc_usdc}:leader`, fence the production leader (`Sink Fenced`), and write to the production stream.

1. **Dedicated Staging Redis (`REDIS_URL`):** Always use a dedicated staging Redis 8.4 instance (`--appendonly yes --maxmemory-policy noeviction`).
2. **Distinct Host Identifiers (`EVML_HOST_ID`):** Set `EVML_HOST_ID=staging-a` and `EVML_HOST_ID=staging-b` so every log line, Redis heartbeat (`evml:{base_eurc_usdc}:hb:staging-a`), lease token, and n8n alert payload is unambiguously tagged as `staging-*`.
3. **Dead-Man's Switch Isolation (`DEADMAN_PING_URL`):**
   - Either create a separate **staging** check in `healthchecks.io` (`evm-listener-base-staging`), **or** leave `DEADMAN_PING_URL=""` (`"deadman":"disabled"` in `/readyz`).
   - **Never** put the production `DEADMAN_PING_URL` on staging (otherwise a staging process could mask a dead production leader, or stopping staging would trigger false production pages).
4. **RPC Quota Hygiene:** Running staging and production simultaneously on the same QuickNode/Infura keys doubles `newHeads` (`43,200/day`) and HTTP tag-polling (`~38k/day`) traffic. **Stop or pause the staging containers once staging sign-off is complete** before starting the 72-hour production soak.

---

## 1. Local Pre-Flight Build & Image Audit (`mk/50-staging.mk`)

Run on your workstation or staging build host before deploying to the staging server:

- [ ] **1. Build & Test in `RelWithDebInfo` Mode (`-O2 -g`):**
  ```bash
  make staging-build
  make staging-test
  ```
  - **Accept:** Compiles cleanly with zero warnings (`-Werror`) and passes the full CTest suite (`0` failures).

- [ ] **2. Build & Audit Staging Container Image (`evm-listener:staging`):**
  ```bash
  make staging-audit
  ```
  - **Accept:**
    - Builds `evm_listener:staging-<VERSION>` and `evm-listener:staging`.
    - `docker run --rm evm_listener:staging-<VERSION> --version` prints the expected version and git commit.
    - `ldd` shows only standard glibc/libstdc++ shared libraries (`libstdc++.so.6`, `libm.so.6`, `libgcc_s.so.1`, `libc.so.6`).

---

## 2. Staging Server Setup (Railway `staging` + Local Docker Pre-Test)

### Option A: Railway `staging` Environment (`staging` Branch CI + Auto-Deploy)
> [!IMPORTANT]
> **One `staging` GitHub branch serves all staging instances.** You do **not** need two separate GitHub branches for `staging-a` and `staging-b`. Both Railway services in the `staging` environment connect to the **same `staging` branch** on GitHub; the only difference between the two services is the `EVML_HOST_ID` environment variable (`EVML_HOST_ID=staging-a` vs `EVML_HOST_ID=staging-b`). Pushing to `staging` triggers GitHub Actions CI and automatically deploys all connected services in the `staging` environment.

- [x] **1. Railway Environment & GitHub Branch:**
  - Project: `Strategy` (`5147b56c-266b-4104-a971-1232813c4d48`), Environment: `staging` (`9d4d1159-658a-4ae9-9d64-cc0416a54c8d`).
  - Connected to GitHub repo `GroAI-fi/evm_cpp_listener`, branch **`staging`** (enable **"Wait for CI"** so Railway only deploys after `.github/workflows/ci.yml` passes).
- [x] **2. Staging `Redis` (8.4.0) Service Configuration:**
  - Use a volume-safe start command (works whether or not a Railway volume is attached) with AOF and `noeviction` enforced:
    ```bash
    /bin/sh -c "rm -rf ${RAILWAY_VOLUME_MOUNT_PATH:-/data}/lost+found/ && exec docker-entrypoint.sh redis-server --requirepass $REDIS_PASSWORD --save 60 1 --appendonly yes --maxmemory-policy noeviction --dir ${RAILWAY_VOLUME_MOUNT_PATH:-/data}"
    ```
- [x] **3. Listener Service (`evm_cpp_listener` -> `staging-a`, and optional `staging-b`):**
  - Paste variables from gitignored `evm_listener/.env.staging` (`REDIS_URL=${{Redis.REDIS_URL}}`, `EVML_HOST_ID=staging-a`, `RAILWAY_DEPLOYMENT_DRAINING_SECONDS=30`).
  - For single-node live stream & price verification, only **1 listener service (`staging-a`)** is needed.
  - To test 2-host active-passive failover on Railway (Drills 3–5), add a second service (`evm_cpp_listener_b`) in the same `staging` environment pointing to the **same `staging` GitHub branch**, copy the variables from `evm_cpp_listener`, and change only `EVML_HOST_ID=staging-b`.
- [ ] **4. Push to `staging` Branch to Deploy:**
  ```bash
  git push origin HEAD:staging
  ```

### Option B: Local Docker Pre-Verification (Recommended Before Pushing to Railway)
> [!TIP]
> **Test in local Docker first, then push to Railway `staging`.** Rebuilding inside the local devcontainer (`make build`) takes ~3 seconds vs ~4 minutes for a full Conan/C++ image build on Railway, and running both local Docker and Railway `staging` simultaneously on the same Infura/QuickNode keys doubles RPC consumption. Pause/remove the Railway `staging` deployment while iterating locally, then push to `staging` once local verification passes.

- [ ] Run unit + integration tests against local Docker Redis (`evm_cpp_listener_redis`):
  ```bash
  make build
  docker exec evm_cpp_listener_dev bash -lc 'REDIS_URL=redis://redis:6379 ctest --test-dir build/Debug --output-on-failure'
  ```
- [ ] Or run the listener binary directly inside `evm_cpp_listener_dev` against local Docker Redis (`redis://redis:6379/0`):
  ```bash
  docker exec evm_cpp_listener_dev bash -lc '
    set -a && source evm_listener/.env && set +a
    REDIS_URL=redis://redis:6379/0 EVML_HOST_ID=local-dev ./build/Debug/evm_listener/evm_listener --config evm_listener/config/base.toml
  '
  ```

---

## 3. Staging Verification Drills (Step-by-Step)

### Drill 1: Config & Redis Pre-Flight Checks (`--check-config` & `--check-redis`)
Run one-off checks inside the local container or against Railway staging Redis:
```bash
# Local dev container against local Redis:
docker exec evm_cpp_listener_dev bash -lc '
  set -a && source evm_listener/.env && set +a
  REDIS_URL=redis://redis:6379/0 ./build/Debug/evm_listener/evm_listener --config evm_listener/config/base.toml --check-config
  REDIS_URL=redis://redis:6379/0 ./build/Debug/evm_listener/evm_listener --config evm_listener/config/base.toml --check-redis
'
```
- [x] **`--check-config` Accept:** Exits `0`; lists `ws providers: quicknode, infura` and `getlogs providers: infura, ..., ankr, base_public, quicknode`; verifies `chain_id = 8453` and contract bytecode on Base.
- [x] **`--check-redis` Accept:** Exits `0`; prints `redis_version="8.4..."`, `maxmemory_policy="noeviction"`, `appendonly="yes"`.

---

### Drill 2: Fast Staging Catch-Up Seed & Leader Startup (`staging-a`)
> [!TIP]
> **Fast-Forwarding `ckpt` in Staging Redis (Do NOT edit `start_block` in `base.toml`):**
> `start_block = 52060000` in [`evm_listener/config/base.toml`](../../evm_listener/config/base.toml) is asserted by 48 unit/integration tests. To start staging near the current Base head (`tip - 200`) without waiting ~60–95 minutes to backfill 1.18M blocks and without breaking unit tests, let `staging-a` initialize `evml:{base_eurc_usdc}:ckpt` once, then fast-forward `safe` and `finalized` in staging Redis:
> ```bash
> URL=$(railway variable list --service Redis --json | python3 -c 'import json,sys; print(json.load(sys.stdin)["REDIS_PUBLIC_URL"])')
> docker exec evm_cpp_listener_redis redis-cli -u "$URL" HSET "evml:{base_eurc_usdc}:ckpt" safe <TIP_MINUS_200> finalized <TIP_MINUS_200>
> docker exec evm_cpp_listener_redis redis-cli -u "$URL" DEL "evml:{base_eurc_usdc}:leader"
> ```
> `staging-a` will re-acquire the lease on its next tick, sweep the last 200 blocks in ~15 seconds, and transition to `"state":"live"`.

Start `staging-a`:
```bash
# Standalone Docker staging server example:
docker run -d \
  --name evml-staging-a \
  --network evml-staging-net \
  --restart always \
  --env-file evm_listener/.env \
  -e REDIS_URL="redis://evml-staging-redis:6379/0" \
  -e EVML_HOST_ID="staging-a" \
  -p 127.0.0.1:9464:9464 \
  evm-listener:staging
```
- [ ] **Health & Readiness Check:**
  ```bash
  curl -sS http://127.0.0.1:9464/healthz && echo "healthz OK"
  curl -sS http://127.0.0.1:9464/readyz | jq .
  ```
  - **Accept:** `/healthz` returns `200`; once caught up, `/readyz` returns `200` with `"state":"live","role":"leader","saturated":false`.
- [ ] **Redis Key & Heartbeat Inspection:**
  ```bash
  docker exec evml-staging-redis redis-cli GET "evml:{base_eurc_usdc}:leader"
  docker exec evml-staging-redis redis-cli HGETALL "evml:{base_eurc_usdc}:ckpt"
  docker exec evml-staging-redis redis-cli GET "evml:{base_eurc_usdc}:hb:staging-a"
  ```
  - **Accept:** `leader` holds `1|staging-a|<nonce>`, `ckpt` has `safe`, `finalized`, and `contracts`, and `hb:staging-a` has a ~30 s TTL and fresh `ts_ms`.

---

### Drill 3: Standby Deployment & Zero-Overhead Check (`staging-b`)
Start `staging-b` against the same staging Redis:
```bash
# Standalone Docker staging server example:
docker run -d \
  --name evml-staging-b \
  --network evml-staging-net \
  --restart always \
  --env-file evm_listener/.env \
  -e REDIS_URL="redis://evml-staging-redis:6379/0" \
  -e EVML_HOST_ID="staging-b" \
  -p 127.0.0.1:9465:9464 \
  evm-listener:staging
```
- [ ] **Standby Readiness & Metrics Check:**
  ```bash
  curl -sS http://127.0.0.1:9465/healthz && echo "standby healthz OK"
  curl -sS http://127.0.0.1:9465/readyz | jq .
  curl -sS http://127.0.0.1:9465/metrics | grep -E "evml_role|evml_ws_messages_total|evml_rpc_requests_total"
  ```
  - **Accept:**
    - `/readyz` returns `200` with `"state":"standby","role":"standby"`.
    - `evml_role{role="standby"} 1`.
    - Standby makes **0** WebSocket connections and **0** RPC calls while standing by.

---

### Drill 4: Graceful `SIGTERM` Drain & Active-Passive Takeover (`staging-a -> staging-b`)
1. Record the current `safe` cursor `C_s` before stopping `staging-a`:
   ```bash
   TAKEOVER_BLOCK=$(curl -sS http://127.0.0.1:9464/readyz | jq -r '.cursors.safe')
   echo "Takeover block: $TAKEOVER_BLOCK"
   ```
2. Stop `staging-a` with `SIGTERM` (`docker stop -t 30 evml-staging-a` or restart in Railway staging):
   - [ ] **Drain Check (`staging-a` logs):** Confirm `staging-a` logs `"flushed":true`, `"unflushed":0`, `"lease_released":true`, and exits `0` in `< 1 s`.
   - [ ] **Takeover Check (`staging-b`):** Poll `http://127.0.0.1:9465/readyz`:
     - `staging-b` acquires leadership (`2|staging-b|<nonce>`) within `≤ 5 s`.
     - `staging-b` re-verifies the unfinalized `(C_f, C_s]` safe window and reaches `"state":"live","role":"leader"` in **`< 20 s`**.
     - `Leader Changed` (`"old_host":"staging-a","new_host":"staging-b"`) fires once.
3. Start `staging-a` back up (`docker start evml-staging-a`):
   - [ ] **Demotion / Standby Return Check:** Confirm `staging-a` comes up in `"state":"standby","role":"standby"` on `http://127.0.0.1:9464/readyz` without disrupting `staging-b`.

---

### Drill 5: Pause / Network Freeze Fencing Drill (`docker pause`)
Test ungraceful leader freeze where `SIGTERM` is not sent:
1. Pause the current leader (`docker pause evml-staging-b`) for `20 seconds`:
   - [ ] **Standby Takeover:** Confirm `staging-a` acquires the expired 15 s lease (`3|staging-a|<nonce>`) and reaches `"state":"live","role":"leader"` within `≤ 20 s`.
2. Unpause the old leader (`docker unpause evml-staging-b`):
   - [ ] **Watchdog D2 Pause Tolerance & Fencing:** Confirm `staging-b` does **not** falsely `SIGABRT` on wake-up (D2 pause tolerance resets watchdog baseline when wake gap `> 6 s`), detects `FENCED` on its next lease renewal, logs `Sink Fenced`, and demotes cleanly to `"state":"standby","role":"standby"`.

---

### Drill 6: Independent Stream Exactness Audit (`--audit-stream`)
Once `ckpt.finalized` (`curl -sS http://127.0.0.1:9464/readyz | jq -r '.cursors.finalized'`) has advanced past the takeover window (`>= TAKEOVER_BLOCK + 20`, ~20 minutes on Base):
```bash
FROM_BLOCK=$(docker exec evml-staging-redis redis-cli XRANGE "evml:{base_eurc_usdc}:events" - + COUNT 1 | grep -A1 block_number | tail -n1)
FINAL_CKPT=$(docker exec evml-staging-redis redis-cli HGET "evml:{base_eurc_usdc}:ckpt" finalized)

docker run --rm --network evml-staging-net \
  --env-file evm_listener/.env \
  -e REDIS_URL="redis://evml-staging-redis:6379/0" \
  evm-listener:staging \
  --config /app/config/base.toml \
  --audit-stream "$FROM_BLOCK" "$FINAL_CKPT"
```
- [ ] **Accept:** Exits `0` with:
  `missing=0 phantom=0 dup_live=0 field_mismatch=0 order_violation=0` (verified via an independent RPC provider distinct from `dominant_source`).

---

## 4. Staging Sign-Off & Teardown Before Production Rollout

- [ ] All 6 Staging Verification Drills above passed with `0` stream defects.
- [ ] **Stop/Pause Staging Containers Before Starting Production Soak:**
  ```bash
  # Standalone Docker staging server:
  docker stop evml-staging-a evml-staging-b
  ```
  *(Ensures staging does not consume QuickNode/Infura quota during the 72-hour production soak in [`doc/ops/railway_deployment_checklist.md`](railway_deployment_checklist.md).)*

