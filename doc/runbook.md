# EVM C++ Listener Operations Runbook

This runbook covers alert triage and standard operational procedures for `evm_listener` (`shard = base_eurc_usdc`, Base `chainId = 8453`).

---

## Part 1: Alert Reference (`60_phase5_ops.md` §5.3.3)

Alerts are evaluated every 10 s by the host and delivered via `POST` to `$N8N_ALERT_WEBHOOK_URL` (`X-API-Key: $N8N_ALERT_WEBHOOK_TOKEN`) and logged at `WARN` (`message: "alert raised"`).
- **Condition alerts** fire once when the condition becomes true, re-notify every 15 minutes while still true, and emit `RESOLVED: <title>` (`severity = "info"`) once when the condition clears.
- **Event alerts** fire on occurrence (rate-limited per subject to once per 15 minutes where noted) and never emit `RESOLVED:`.

---

### 1. `WebSocket Stalled` (`critical`, condition)
- **What it means:** No `newHeads` notification has arrived from any WebSocket provider for `> 30 s` (`2 × head_stall_ms`; Base produces blocks every 2 s). Note: HTTP `getLogs` sweeps continue to advance the pipeline even when all WebSockets are down.
- **First 3 checks:**
  1. Inspect alert `details` (`provider`, `seconds_since_last_head`, `last_processed_block`).
  2. Query `GET /readyz` and `GET /metrics` (`evml_provider_active`, `evml_checkpoint_block{class="safe"}`) to confirm whether HTTP `getLogs` sweeps are still advancing `C_s`.
  3. Check `BASE_WSS_URL_QUICKNODE`, `BASE_WSS_URL`, and `BASE_WSS_URL_ALCHEMY` status on provider status pages or run `evm_listener --config /app/config/base.toml --probe-transport 10`.
- **Fix:** If `BASE_WSS_URL` (Infura WSS) is unset in Railway variables, set it so the pool has a backup WebSocket provider behind QuickNode. If a provider key is rate-limited or revoked, rotate the WSS variable and redeploy.
- **When to escalate:** Escalate if `WebSocket Stalled` persists `> 15 min` across all configured WSS providers or is accompanied by `Safe Gate Stuck` / `Pipeline Stalled`.

### 2. `Provider Failover` (`warning`, event)
- **What it means:** The active WebSocket provider switched (`reason = "failover"` after session errors/head stall, or `reason = "switch_back"` after the primary remained healthy for `prefer_primary_after_s = 600 s`).
- **First 3 checks:**
  1. Check `details` (`from`, `to`, `reason`).
  2. If `reason == "failover"`, inspect container logs immediately prior to the alert for `ws down` or `provider transition` reasons.
  3. Check `evml_provider_failovers_total` on `/metrics` to see if the provider is flapping.
- **Fix:** No immediate action required for a clean failover or `switch_back`. If a primary provider fails repeatedly every 10 minutes upon `switch_back`, temporarily unset its `ws_env` or increase `prefer_primary_after_s`.
- **When to escalate:** Escalate only if failovers occur more than 4 times per hour or lead to `WebSocket Stalled`.

### 3. `Provider Quota Exhausted` (`warning` or `critical`, condition)
- **What it means:** A provider returned HTTP `402` or a monthly capacity / billing quota error (`QuotaExhausted`) and entered a 1-hour cooldown (`quota_cooldown_s = 3600`). Severity is `warning` (once per incident, no 15-min repeat) if at least one healthy provider remains, or `critical` if no healthy provider remains.
- **First 3 checks:**
  1. Check `details` (`provider`, `chain`, `error_code`, `message`, `next_probe_ts`).
  2. Check `/metrics` (`evml_rpc_requests_total{provider,method}`) to see which RPC method consumed the provider's quota.
  3. Log in to the provider dashboard (QuickNode, Infura, Alchemy, Ankr) to check monthly credit usage.
- **Fix:** If severity is `warning` (e.g., Alchemy monthly free tier exhausted), the pool automatically cools down the provider and re-probes hourly until the quota resets. If severity is `critical` (all providers exhausted), top up credits or add a fresh provider key in Railway variables immediately.
- **When to escalate:** Escalate immediately if severity is `critical` or if primary providers (QuickNode or Infura) exceed 80% of monthly plan limits.

### 4. `getLogs Provider Lagging` (`warning`, condition)
- **What it means:** The current `getLogs` provider's `safe` block tag has trailed the active WebSocket provider's `safe` tag by `> 30 blocks` continuously for `> 2 minutes`, triggering an automatic `getLogs` provider failover.
- **First 3 checks:**
  1. Check `details` (`provider`, `its_safe`, `ws_safe`).
  2. Check container logs for `provider transition` to confirm the pool switched to the next `getlogs_order` provider.
  3. Verify `evml_gate_block{class="safe"}` on `/metrics` is advancing on the new provider.
- **Fix:** Self-healing via automatic provider rotation. If a specific provider (e.g., Ankr) chronically lags by > 30 blocks, keep it lower in `getlogs_order` in `base.toml`.
- **When to escalate:** Escalate if all `getLogs` providers lag simultaneously (`Safe Gate Stuck`).

### 5. `Provider Auth Failure` (`critical`, condition)
- **What it means:** A configured provider returned HTTP `401` or `403` (invalid API key, revoked token, IP/UA restriction, or disabled project) and was marked `Fatal` for the lifetime of the process.
- **First 3 checks:**
  1. Check `details` (`provider`, `http_status`).
  2. Verify the provider's env var (`BASE_RPC_URL*` / `BASE_WSS_URL*`) in Railway service variables.
  3. Test the endpoint with `evm_listener --config /app/config/base.toml --check-config`.
- **Fix:** Rotate or fix the broken API key in Railway variables (see [Procedure 6: Adding a Provider or Rotating a Provider Key](#6-adding-a-provider-or-rotating-a-provider-key)) so both services restart and clear the `Fatal` state.
- **When to escalate:** Escalate if more than one provider fails authentication or if key rotation does not resolve the `401`/`403`.

### 6. `Safe Gate Stuck` (`critical`, condition)
- **What it means:** The `safe` finality gate `G_s` has not advanced for `> safe_gate_stuck_s` (`600 s` = 10 min). `/readyz` also returns `503` on the leader while this condition holds.
- **First 3 checks:**
  1. Check `details` (`G_s`, `head`, `seconds_since_advance`).
  2. Check Base L1/L2 batcher status (`status.base.org`) to see if Base `safe` head progression is paused upstream on Ethereum L1.
  3. Check `/metrics` (`evml_rpc_requests_total{method="eth_getBlockByNumber"}`) and logs to verify tag polling is succeeding on at least one `getLogs` provider.
- **Fix:** If all `getLogs` providers are failing tag polls, fix provider connectivity/credentials. If Base L1 batch submission is delayed upstream, wait for the chain's `safe` tag to advance; the listener will catch up automatically once `G_s` moves.
- **When to escalate:** Escalate immediately if `head` is advancing normally on Base explorers, `safe` is advancing on public RPCs, and only `evm_listener` is stuck.

### 7. `Finalized Gate Stuck` (`critical`, condition)
- **What it means:** The `finalized` finality gate `G_f` has not advanced for `> 60 minutes` (`gate_lag_alert_s = 3600 s`).
- **First 3 checks:**
  1. Check `details` (`G_f`, `head`).
  2. Check Ethereum L1 finality and Base L2 state root finalization status (`status.base.org`).
  3. Run `evm_listener --config /app/config/base.toml --probe-transport 10` to inspect live `finalized` tag responses.
- **Fix:** If caused by an upstream L1/Base finalization delay, no operator intervention is needed (`safe` pool events continue streaming, and `released` entries remain buffered in Redis until `G_f` advances). If caused by a stuck provider, restart or failover the provider.
- **When to escalate:** Escalate if `G_f` is stuck `> 2 hours` while `finalized` is advancing on Base block explorers.

### 8. `Checkpoint Lag` (`critical`, condition)
- **What it means:** Sweep cursor lag exceeds threshold (`G_s - C_s > 300` blocks for safe, or `G_f - C_f > 2000` blocks for finalized) continuously for `> 15 minutes`.
- **First 3 checks:**
  1. Check `details` (`class`, `gate`, `ckpt`, `lag_blocks`).
  2. Check `/metrics` (`evml_getlogs_throttled_total`, `evml_provider_inconsistency_total`, `evml_sink_spill_bytes`) to see whether RPC throttling or Redis sink backpressure is slowing the sweep.
  3. Check which `getLogs` provider is active: if fallen back to `quicknode` (`max_getlogs_range = 5` at 2 RPS = 10 blocks/s), catch-up after a long outage is slower than on `infura` (`500`) or `ankr` (`10000`).
- **Fix:** Restore higher-range `getLogs` providers (`infura`, `ankr`, `alchemy`) if they are down or quota-exhausted.
- **When to escalate:** Escalate if `lag_blocks` is growing rather than shrinking (`Pipeline Stalled`).

### 9. `Safe Reorg` (`critical`, event)
- **What it means:** A safe-tag chain reorganization altered one or more blocks `≤ C_s` and retracted at least one previously emitted `finality=safe` event (emitting a `type=revert` entry to the stream).
- **First 3 checks:**
  1. Check `details` (`new_reorgs`, `total`, `fork_point`).
  2. Inspect the `evml:{base_eurc_usdc}:events` stream around `fork_point` to see the emitted `type=revert` and replacement `type=log` entries.
  3. Check downstream Bot B logs to confirm it processed the `type=revert` contra-entry cleanly.
- **Fix:** The listener handles `Safe Reorg` automatically (`reverify_released` diffs `(C_f, C_s]`, emits `type=revert` with full before-images, rewinds `C_s` to `fork_point`, and re-sweeps). Run `evm_listener --config /app/config/base.toml --audit-stream <fork_point - 10> <fork_point + 50>` once `ckpt.finalized` passes `fork_point + 50` to verify `0` defects.
- **When to escalate:** Always notify the strategy/quant team when `Safe Reorg` fires so Bot B's state at `fork_point` is verified.

### 10. `Sink Degraded` (`critical`, condition)
- **What it means:** The Redis sink has been spilling unacknowledged records to local ephemeral disk (`/tmp/evml-spill`) with `spill_bytes > 0` for `> 5 minutes` (`oldest_record_age_s > 300`).
- **First 3 checks:**
  1. Check `details` (`spill_bytes`, `oldest_record_age_s`).
  2. Run `evm_listener --config /app/config/base.toml --check-redis` to check Redis reachability, memory usage, and `maxmemory-policy`.
  3. Check Railway Redis metrics (CPU, memory vs `maxmemory`, connection count).
- **Fix:** **Do not restart the leader container while `spill_bytes > 0`** unless Redis is permanently lost — the disk spill is process-scoped (`D1`), and restarting wipes `/tmp/evml-spill` (forcing a re-sweep from `ckpt`). Restore Redis connectivity or increase Redis `maxmemory` if Redis is returning `OOM`; the leader will automatically replay the spill in order and resolve the alert.
- **When to escalate:** Escalate immediately if `spill_bytes` approaches 50% of `spill_max_bytes` (`256 MiB`) or if Redis cannot be restored within 15 minutes.

### 11. `Sink Saturated` (`critical`, condition)
- **What it means:** The sink queue (`queue_capacity = 100000`) or disk spill (`spill_max_bytes = 512 MiB`) is full. Sweeps are paused (`StepStatus::Paused`), checkpoints are frozen, `/readyz` returns `503`, and dead-man pings stop.
- **First 3 checks:**
  1. Check `details` (`spill_bytes`, `spill_max_bytes`) and `/readyz`.
  2. Check Redis status (`--check-redis`) and Railway Redis memory/OOM errors.
  3. Check whether a Redis network partition or `OOM` is blocking script execution.
- **Fix:** Restore Redis connectivity or raise Redis memory limit immediately. Once Redis accepts writes again, the sink drains the spill buffer, resumes sweeps from `C_s` / `C_f` with zero data loss, and resolves `Sink Saturated`.
- **When to escalate:** Escalate immediately upon firing.

### 12. `Redis Unreachable` (`critical`, condition)
- **What it means:** Redis connection or `PING` has been failing continuously for `> 60 seconds` (either during startup wait or during a leader/standby outage).
- **First 3 checks:**
  1. Check `details` (`since_s`, `addr_family` — e.g. `ipv6`, `ipv4`, or `unresolved`).
  2. Run `evm_listener --config /app/config/base.toml --check-redis` inside the container.
  3. Check the Railway Redis service status and private network (`redis.railway.internal`).
- **Fix:** If the Railway Redis container is down or restarting, wait for it to come back online. If `addr_family` is `unresolved`, check Railway private networking DNS.
- **When to escalate:** Escalate if Redis remains unreachable for `> 5 minutes`.

### 13. `Sink Fenced` (`warning`, event)
- **What it means:** A Redis write or lease renewal returned `FENCED` because another host acquired a newer leadership epoch. The fenced host discarded its in-memory queue and ephemeral spill (`discarded_records`) and demoted itself to standby.
- **First 3 checks:**
  1. Check `details` (`discarded_records`) and verify a matching `Leader Changed` alert fired from the new leader.
  2. Check `/readyz` on both `evml-base-a` and `evml-base-b` to confirm exactly one is `"role":"leader"` and the other is `"role":"standby"`.
  3. Check whether the old leader experienced a transient CPU/network pause or Redis blip that caused its 15 s lease to expire.
- **Fix:** Normal during a lease takeover: the new leader re-sweeps from the shared Redis `ckpt` and deduplicates any overlap via `emit.lua`. Once the takeover window is finalized (~20 min), run `--audit-stream` over the window to confirm 0 gaps and 0 duplicates.
- **When to escalate:** Escalate if `Sink Fenced` fires repeatedly without deliberate restarts (lease flapping).

### 14. `Sink Invariant` (`critical`, event)
- **What it means:** `emit.lua` returned `BELOW_FINALIZED` because a tracked safe-class emission (`ARGV[4] ~= ''`) had a block number `≤ ckpt.finalized`. The offending item was rejected by Redis.
- **First 3 checks:**
  1. Check `details` (`new_refusals`, `total`).
  2. Inspect `HGETALL evml:{base_eurc_usdc}:ckpt` (`safe` vs `finalized` cursors).
  3. Inspect container logs around the timestamp for cursor or checkpoint anomalies.
- **Fix:** Run `--audit-stream` over the affected finalized block range to verify stream integrity.
- **When to escalate:** Escalate immediately to engineering; `BELOW_FINALIZED` indicates a pipeline cursor invariant violation or external manual modification of `evml:{base_eurc_usdc}:ckpt`.

### 15. `WS Missed Logs` (`warning`, event)
- **What it means:** During live operation, an HTTP `getLogs` sweep discovered canonical logs that the active WebSocket `logs` subscription never delivered (rate-limited to once per 15 min per provider).
- **First 3 checks:**
  1. Check `details` (`new_missed`, `total`, `provider`).
  2. Check `/metrics` (`evml_ws_missed_logs_total`, `evml_ws_extra_logs_total`).
  3. Check whether the WS provider (`provider`) recently reconnected or lagged.
- **Fix:** **No data was lost** — `getLogs` sweeps are the single source of truth for release, and the missed WS logs were emitted normally by the sweep. If a WS provider drops logs chronically, consider demoting it in `ws_order`.
- **When to escalate:** Escalate only if accompanied by `WebSocket Stalled` or `Provider Inconsistent`.

### 16. `Provider Inconsistency / Gate Regressed (finalized)` (`critical`, event)
- **What it means:** `FinalityGate` detected a `finalized` block number regression from the **same** provider or a cross-provider `finalized` block hash mismatch at the same height. **Pipeline release is permanently frozen** to prevent corrupting finalized state.
- **First 3 checks:**
  1. Check `details` (`reason`), which names the provider and the conflicting finalized block numbers/hashes.
  2. Query `eth_getBlockByNumber("finalized", false)` across Infura, Alchemy, Ankr, and `mainnet.base.org` to identify which provider returned a bogus finalized block.
  3. Check `/readyz` (`"state"` / frozen status).
- **Fix:** Remove or disable the faulty provider's environment variable, verify the canonical finalized block on Base explorers, and restart the leader service to clear the frozen gate. Run `--audit-stream` over the preceding finalized window.
- **When to escalate:** Escalate immediately upon firing.

### 17. `Provider Inconsistent` (`warning`, event)
- **What it means:** `LogSource::report_bad()` penalized a provider (either `validate_logs` rejected an out-of-range/untracked/removed log in `eth_getLogs`, or `block(n)` saw a canonical block-hash disagreement against the head ring / independent provider). Rate-limited to once per 15 min per provider, accumulating `new_inconsistent` across cooldowns.
- **First 3 checks:**
  1. Check `details` (`provider`, `new_inconsistent`, `total`).
  2. Inspect container `WARN` logs for the exact validation or hash-mismatch message on `provider`.
  3. Check `/metrics` (`evml_provider_inconsistency_total{provider}`).
- **Fix:** The bad page was automatically discarded and retried on another provider; after 3 consecutive bad responses (`failover_after = 3`), the provider is marked `Down`. If a provider repeatedly serves corrupted responses, disable its env var.
- **When to escalate:** Escalate if multiple providers show rising `evml_provider_inconsistency_total` simultaneously (`Pipeline Stalled`).

### 18. `Safe Sweep Unreliable` (`warning`, condition)
- **What it means:** More than 3 safe-class logs in the last 1 hour were missed by the safe sweep (`(C_s, G_s]`) and had to be emitted late (`finality=finalized`) by the final sweep (`(C_f, G_f]`).
- **First 3 checks:**
  1. Check `details` (`count_1h`, `provider`, `last_block`) and `/metrics` (`evml_safe_missed_total`).
  2. Check which `getLogs` provider served the safe sweep around `last_block`.
  3. Run `--audit-stream <last_block - 100> <last_block>` once finalized to confirm all late logs were emitted by the final sweep.
- **Fix:** The final sweep already recovered and emitted the missed logs with `finality=finalized`. If a specific `getLogs` provider is serving incomplete log pages at the `safe` tag, move it lower in `getlogs_order`.
- **When to escalate:** Escalate if ` Safe Sweep Unreliable` persists for `> 1 hour` across provider failovers.

### 19. `Pipeline Stalled` (`critical`, condition)
- **What it means:** Either a sweep cursor (`safe` or `finalized`) missed 3 consecutive progress deadlines (`3 × progress_stall_ms = 6 min`) while its gate was ahead, or a sweep returned `StepStatus::Stalled` (single-address query still `RangeTooLarge` at `min_range = 10`, or `max_bad_attempts` exhausted with no cross-provider hash agreement).
- **First 3 checks:**
  1. Check `details` (`class`, `cursor`, `gate`, `provider`, `detail`).
  2. Inspect container logs for the exact `detail` string (`range still too large at the minimum size...` vs `block hash mismatch...` vs `getLogs failed...`).
  3. Check `/metrics` (`evml_provider_active`, `evml_provider_inconsistency_total`).
- **Fix:**
  - If `detail` indicates `range still too large at the minimum size`: lower `[backfill] min_range` (e.g. from `10` to `1`) in `base.toml` and redeploy.
  - If `detail` indicates all providers are failing or disagreeing on block hashes: verify provider health via `--check-config` and `--probe-transport 10`, disable any faulty provider, and ensure at least two independent HTTP providers are healthy.
- **When to escalate:** Escalate immediately upon firing.

### 20. `Leader Changed` (`warning`, event)
- **What it means:** Shard leadership in `evml:{base_eurc_usdc}:leader` changed from `old_host` to `new_host` (a new epoch token was minted).
- **First 3 checks:**
  1. Check `details` (`old_host`, `new_host`).
  2. Check whether a deploy, restart, or forced takeover was just executed on `old_host`.
  3. Query `/readyz` on `new_host` to confirm it reached `"state":"live","role":"leader"`.
- **Fix:** Expected during deploys and rollouts. If unexpected, inspect `old_host` logs and Railway metrics for crash/watchdog exit `134` or Redis connectivity blips.
- **When to escalate:** Escalate if unplanned `Leader Changed` events occur more than once per day.

### 21. `Redis Misconfigured` (`critical`, condition)
- **What it means:** Startup Redis inspection found `maxmemory_policy != "noeviction"`, `appendonly != "yes"`, or Redis version `< 7.0` (note: `maxmemory_policy != "noeviction"` with `require_noeviction = true` or Redis `< 7.0` is a `StartupFatal` exit; `appendonly = "no"` logs a startup warning and raises `Redis Misconfigured`).
- **First 3 checks:**
  1. Check `details` (`maxmemory_policy`, `appendonly`, `redis_version`).
  2. Run `evm_listener --config /app/config/base.toml --check-redis`.
  3. Inspect Redis configuration via `redis-cli -u $REDIS_URL CONFIG GET maxmemory-policy appendonly`.
- **Fix:** Configure Redis with `CONFIG SET maxmemory-policy noeviction` and `CONFIG SET appendonly yes` (and persist in the Railway Redis service start command: `--appendonly yes --maxmemory-policy noeviction`), then restart the listener so the startup check re-runs cleanly.
- **When to escalate:** Escalate immediately if Redis persistence or `noeviction` cannot be enabled on the production Redis instance.

### 22. `Consumer Lagging` (`warning`, condition)
- **What it means:** For a registered consumer group in `[sink] consumer_groups`, `max(undelivered_age_s, pending_age_s)` exceeded `consumer_lag_warn_s` (**12 hours** = 25% of the 48-hour stream retention window). Rate-limited to once per 15 min per group.
- **First 3 checks:**
  1. Check `details` (`stream`, `group`, `undelivered_age_s`, `pending_age_s`, `lag`, `retention_s`).
  2. If `undelivered_age_s` is high: the consumer (`group`) has stopped calling `XREADGROUP` or fallen behind.
  3. If `pending_age_s` is high while `undelivered_age_s` is low: a consumer read entries via `XREADGROUP` but crashed or stalled without calling `XACK` (leaving entries stuck in the PEL).
- **Fix:** Restart or scale the downstream consumer (`strat:aero_eurcusdc:botB_v1`), or ensure it runs `XAUTOCLAIM` / `XACK` on stuck pending entries.
- **When to escalate:** Notify the Bot B owner if lag is not cleared within 2 hours.

### 23. `Consumer Near Trim` (`critical`, condition)
- **What it means:** `max(undelivered_age_s, pending_age_s)` exceeded `consumer_lag_crit_s` (**24 hours** = 50% of the 48-hour retention window). Unread entries will be permanently trimmed by `XADD MINID ~` in less than 24 hours.
- **First 3 checks:**
  1. Check `details` (`stream`, `group`, `undelivered_age_s`, `pending_age_s`, `lag`, `retention_s`).
  2. Check whether the downstream consumer service (Bot B) is down or wedged.
  3. Check Redis memory headroom (`INFO memory`) in case `retention_ms` needs a temporary increase while Bot B is repaired.
- **Fix:** Immediately restore the downstream consumer so it drains and `XACK`s its backlog before the 48-hour retention deadline. If Bot B requires extended downtime and Redis has sufficient memory headroom, temporarily increase `[sink] retention_ms` in `base.toml` and redeploy.
- **When to escalate:** Escalate immediately to the Bot B owner and on-call engineer.

### 24. `Consumer Entries Trimmed` (`critical`, event)
- **What it means:** Stream retention (`XADD MINID ~`) or manual `XDEL` trimmed stream entries past a registered consumer group's cursor before the group read them (`trimmed_unread == true`). **Downstream data loss has occurred for that consumer group.**
- **First 3 checks:**
  1. Check `details` (`stream`, `group`, `last_delivered_id`, `max_deleted_id`).
  2. Inspect `XINFO STREAM evml:{base_eurc_usdc}:events` and `XINFO GROUPS evml:{base_eurc_usdc}:events`.
  3. Determine the block number of the first remaining entry in the stream (`XRANGE evml:{base_eurc_usdc}:events - + COUNT 1`).
- **Fix:** Follow [Procedure 6: `Consumer Entries Trimmed` Recovery](#6-consumer-entries-trimmed-recovery).
- **When to escalate:** Escalate immediately to the Bot B owner and on-call engineer.

### 25. `Consumer Group Missing` (`warning`, condition)
- **What it means:** A consumer group listed in `[sink] consumer_groups` (or its stream) has not existed in Redis for `> 10 minutes` after leader startup.
- **First 3 checks:**
  1. Check `details` (`stream`, `group`).
  2. Run `redis-cli -u $REDIS_URL XINFO GROUPS evml:{base_eurc_usdc}:events`.
  3. Check whether Bot B has been deployed and created its consumer group (`XGROUP CREATE evml:{base_eurc_usdc}:events strat:aero_eurcusdc:botB_v1 0 MKSTREAM`), or if the stream has simply had 0 events since a fresh Redis initialization.
- **Fix:** Start Bot B so it creates its consumer group, or create the group manually at `0` (`XGROUP CREATE evml:{base_eurc_usdc}:events strat:aero_eurcusdc:botB_v1 0 MKSTREAM`) so every entry from the beginning of the stream is retained in the group's backlog until Bot B starts reading.
- **When to escalate:** Escalate if Bot B is supposed to be live in production and its group disappeared (which indicates a Redis reset/flush).

---

## Part 2: Standard Operational Procedures (`70_phase6_testing_rollout.md` §6.9)

### 1. Deploy, Rollback & Forced Leader Takeover

#### Standard Rolling Deploy (Zero Data Loss, ≤ 20 s Takeover)
1. Deploy the new build to the **standby** service first (check `/readyz` on both services to see which has `"role":"standby"`).
2. Confirm the standby reaches `/healthz -> 200` and `/readyz -> 200` (`"role":"standby"`).
3. Deploy the new build to the **leader** service.
   - Railway sends `SIGTERM` via `tini` (`RAILWAY_DEPLOYMENT_DRAINING_SECONDS=30`).
   - The retiring leader drains its in-flight sink queue, persists checkpoints, and explicitly releases `evml:{base_eurc_usdc}:leader`.
   - The standby acquires the lease on its next poll (`≤ 5 s`), logs `leadership term started`, emits `Leader Changed`, and reaches `"state":"live","role":"leader"`.
   - The redeployed former leader starts up and settles into `"role":"standby"`.

#### Rollback
- Stream schema (`v=1`) and Redis key layouts are invariant across v1 builds.
- To roll back, redeploy the previous container image / commit on both `evml-base-a` and `evml-base-b` in Railway. No Redis data migration is required.

#### Forced Leader Takeover Drill
1. Identify the current leader (`evml-base-a` or `evml-base-b`) via `/readyz`.
2. Restart the leader service in Railway (or locally: `docker compose -f .devcontainer/docker-compose.yml restart listener-a`).
3. Verify the other service acquires the lease within 20 s (`/readyz` shows `"role":"leader"` and `Leader Changed` fires once).
4. Once `ckpt.finalized` advances past the takeover block (~20 minutes on Base), run:
   ```bash
   evm_listener --config /app/config/base.toml --audit-stream <takeover_block - 20> <takeover_block + 20>
   ```
   Confirm exit code `0` (`missing=0 phantom=0 dup_live=0 field_mismatch=0`).

---

### 2. Partial Checkpoint Exit (`ckpt` Missing `safe`, `finalized`, or `contracts`)

- **Symptom:** The listener logs a fatal error in `Recovering` (`partial checkpoint in Redis` or `ckpt.finalized > ckpt.safe`) and exits with code `1` (`kExitFailure`).
- **Why it happens:** `init_checkpoint` and `advance_checkpoint` are atomic Lua scripts (`ckpt.lua`), so a partial checkpoint only occurs if an operator manually ran `HDEL`/`HSET` on `evml:{base_eurc_usdc}:ckpt`.
- **Resolution (Restore All or Delete All — Never Guess Single Fields):**
  1. Stop both `evml-base-a` and `evml-base-b`.
  2. Inspect the hash:
     ```bash
     redis-cli -u "$REDIS_URL" HGETALL "evml:{base_eurc_usdc}:ckpt"
     ```
  3. **Option A (Restore from last known good values):** If you know the exact `safe`, `finalized`, and `contracts` values (e.g., from the last `hb:<host>` or logs), restore all three fields together.
  4. **Option B (Clean re-init & re-backfill):** Delete the shard's state keys and let the listener re-initialize atomically from `contract_start`:
     ```bash
     redis-cli -u "$REDIS_URL" DEL \
       "evml:{base_eurc_usdc}:ckpt" \
       "evml:{base_eurc_usdc}:released" \
       "evml:{base_eurc_usdc}:released_data" \
       "evml:{base_eurc_usdc}:leader"
     ```
     *(Keep `evml:{base_eurc_usdc}:epoch` intact so the epoch counter stays strictly monotonic!)*
  5. Start `evml-base-a`, wait for it to reach `Live`, then start `evml-base-b`.

---

### 3. Uncovered Contract Exit `2` → `--backfill-contract <name>` (`60_phase5_ops.md` §5.6a & D8)

- **Symptom:** After adding a new `[[contracts]]` entry to `base.toml` with `start_block ≤ C_s`, the listener exits at startup with **exit code `2`** and logs:
  `uncovered contract '<name>': run --backfill-contract <name>`.
- **Procedure:**
  1. **Ensure both `evml-base-a` and `evml-base-b` are stopped** (or exited `2`), so the shard lease `evml:{base_eurc_usdc}:leader` is free (or wait up to 15 s for its TTL to expire).
  2. Run the fenced backfill tool for the new contract:
     ```bash
     evm_listener --config /app/config/base.toml --backfill-contract <name>
     ```
     - The tool validates provider env vars, connects to Redis, acquires the shard lease (waiting up to 60 s; exits `1` without writing if another host holds the lease), sweeps `[start_block, C_f]` at `finality=finalized` and `(C_f, C_s]` at `finality=safe` (tracking safe emissions in `released`), updates `ckpt.contracts` only after all emissions are acknowledged, and releases the lease.
     - If interrupted or stalled (exit `1`), simply re-run the command; deduplication makes repeated runs idempotent.
  3. Once it prints `ok    backfilled <name>` (exit `0`), start `evml-base-a` and `evml-base-b`.

---

### 4. Redis Data Loss → Full Re-Backfill from `start_block` (`60_phase5_ops.md` §5.5)

- **Symptom:** The Railway Redis instance was wiped, recreated, or suffered unrecoverable volume loss.
- **Procedure:**
  1. Stop both `evml-base-a` and `evml-base-b` (so an old leader with an in-memory cursor cannot write stale state before re-initialization).
  2. Verify the new Redis instance passes startup checks:
     ```bash
     evm_listener --config /app/config/base.toml --check-redis
     ```
     Confirm `maxmemory_policy=noeviction` and `appendonly=yes`.
  3. Start `evml-base-a` alone.
     - Because `evml:{base_eurc_usdc}:ckpt` is absent and `start_policy = "contract_start"`, `evml-base-a` atomically initializes `ckpt` at `min(contract.start_block) - 1` (`52059999`) and sweeps all configured contracts up to the current `safe` and `finalized` gates.
  4. Recreate downstream consumer groups (e.g., `XGROUP CREATE evml:{base_eurc_usdc}:events strat:aero_eurcusdc:botB_v1 0 MKSTREAM`). Downstream consumers must deduplicate re-emitted historical events by `event_id`.
  5. Once `evml-base-a` reaches `"state":"live"` on `/readyz`, start `evml-base-b`.

---

### 5. `Consumer Entries Trimmed` Recovery

- **Symptom:** `Consumer Entries Trimmed` (`critical`) fired because a consumer group (`strat:aero_eurcusdc:botB_v1`) lagged beyond the 48-hour stream retention window (`retention_ms = 172800000`) and unread entries were trimmed.
- **Procedure:**
  1. Fix and restart the downstream consumer service (Bot B) so it is healthy.
  2. Determine the earliest block still present in the stream:
     ```bash
     redis-cli -u "$REDIS_URL" XRANGE "evml:{base_eurc_usdc}:events" - + COUNT 1
     ```
  3. **Option A (Consumer-side recovery):** If Bot B persists its own last processed `block_number` in its database and can replay/reconcile on-chain state directly up to the stream's first remaining entry, advance the Redis consumer group cursor past the trimmed gap:
     ```bash
     redis-cli -u "$REDIS_URL" XGROUP SETID "evml:{base_eurc_usdc}:events" "strat:aero_eurcusdc:botB_v1" <first_entry_id_minus_1>
     ```
  4. **Option B (Listener re-backfill):** If Bot B requires every missing log from the Redis stream:
     - Temporarily increase `[sink] retention_ms` in `base.toml` to cover the full window (provided Redis `maxmemory` has headroom).
     - Delete the contract's `dedup:*` keys (or flush and re-backfill per [Procedure 4](#4-redis-data-loss--full-re-backfill-from-start_block)) so the trimmed window is re-emitted into the stream, and let Bot B catch up and `XACK`.

---

### 6. Adding a Provider or Rotating a Provider Key

#### Adding an Already-Configured Provider (`base.toml`)
Because `base.toml` already declares `quicknode`, `infura`, `alchemy`, `ankr`, and `base_public` under the **presence-enabled rule**:
1. Set the corresponding environment variable (`BASE_WSS_URL`, `BASE_RPC_URL_ALCHEMY`, etc.) on both `evml-base-a` and `evml-base-b` in Railway.
2. Verify with `--check-config` after redeploy: the newly set provider appears under `ws providers:` and/or `getlogs providers:`.

#### Rotating an Existing Provider Key (Zero Downtime)
1. Update the provider's environment variable (`BASE_RPC_URL*` / `BASE_WSS_URL*`) on the **standby** service first and redeploy it.
2. Run `evm_listener --config /app/config/base.toml --check-config` in the standby container to confirm the new key passes `eth_chainId` and `eth_getCode` checks.
3. Update the environment variable on the **leader** service and redeploy it (triggering a clean lease handover to the already-updated standby).
4. Revoke the old API key in the provider's dashboard only after both services have restarted with the new key.

---

### 7. Reproducible `healthchecks.io` & `n8n` Webhook Setup

#### `healthchecks.io` Dead-Man's Switch (`DEADMAN_PING_URL`)
1. Create a check named **`evm-listener-base-eurc-usdc`** in healthchecks.io.
2. Set **Period = `1 minute`** (`60 s`, matching `[ops] deadman_ping_s = 60`) and **Grace Time = `2 minutes`** (`120 s`), so an unhealthy/frozen leader or total outage pages after ~3 minutes.
3. Attach the Slack `#strategy-production` integration to the check.
4. Set `DEADMAN_PING_URL=https://hc-ping.com/<uuid>` on **both** `evml-base-a` and `evml-base-b`. Only the healthy active leader sends `GET` pings; the standby stays silent so a leader freeze is never masked by a standby.

#### `n8n` Alert Webhook (`N8N_ALERT_WEBHOOK_URL` & `N8N_ALERT_WEBHOOK_TOKEN`)
1. In n8n, import/enable the **"EVM Listener Alert Webhook"** workflow:
   - **Trigger node:** Webhook (`POST`, header auth `X-API-Key` matching `$N8N_ALERT_WEBHOOK_TOKEN`, responds `200 OK`).
   - **Payload schema (`60_phase5_ops.md` §5.3.1):**
     ```json
     {
       "severity": "critical | warning | info",
       "service": "evm-listener",
       "shard": "base_eurc_usdc",
       "host": "a",
       "title": "Safe Reorg",
       "message": "safe reorg detected at fork_point=52060100",
       "ts": "2026-10-29T06:00:00Z",
       "details": { "new_reorgs": 1, "total": 1, "fork_point": 52060100 }
     }
     ```
   - **Routing:** Format `severity`, `shard`, `host`, `title`, `message`, `ts`, and `details` into a Slack message posted to `#strategy-production`.
2. Set `N8N_ALERT_WEBHOOK_URL` and `N8N_ALERT_WEBHOOK_TOKEN` on both `evml-base-a` and `evml-base-b`.

---

### 8. Dual-Plane Hot Signal Stream (`evml:{<shard>}:fast`)

For contracts configured with `fast_stream = true` (e.g., `eurc_usdc_cl1_pool` and `eurc_usdc_cl50_pool` in `base.toml`), the active leader emits non-removed WebSocket `eth_subscribe("logs")` notifications directly to `evml:{<shard>}:fast` (`finality = "latest"`, schema `v=1`) via `emit_fast.lua` in parallel with feeding `WsLogReconciler`:

- **Operational semantics:**
  - **Best-effort, non-authoritative hot signal plane:** Designed for sub-second strategy pre-computation and tick-drift estimation (`Page-Hinkley` / `EWMA`); never emits `type = "revert"` contra-entries.
  - **Zero interference with cold accounting (`evml:{<shard>}:events`):** `emit_fast.lua` fences on `evml:{<shard>}:leader` and executes `XADD KEYS[1] MINID ~ <minid_ms> * ...` (retention controlled by `[sink] fast_retention_ms = 3600000`, default 1 hour). It **never** touches `dedup:*`, `released`, `released_data`, or `ckpt`, so the subsequent `safe`/`finalized` HTTP `getLogs` sweep emits the authoritative event to `evml:{<shard>}:events` normally.
  - **Never spilled to disk:** If `RedisSink` is not `Connected`, is `saturated()`, or transitions to `Degraded`/`Fenced`, fast items (`FastEmitItem`) are dropped immediately rather than written to `/tmp/evml-spill`.
- **Prometheus telemetry (`/metrics`):**
  - `evml_fast_emitted_total`: Total fast-stream events acknowledged by `XADD` on `evml:{<shard>}:fast`.
  - `evml_fast_dropped_total`: Total fast-stream events dropped due to sink saturation, degraded/fenced mode, or pipeline error.


