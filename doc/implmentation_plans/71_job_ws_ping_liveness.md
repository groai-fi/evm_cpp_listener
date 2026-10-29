# Job 71: WS liveness mode `ping` (opt-in, after Phase 6)

**Status:** **Deferred / closed for v1 (2026-10-29)** after evaluating the Phase 6 §6.4 quota decision gate (see [§11](#11-phase-6-64-gate-decision-2026-10-29) below). Originally moved out of plan 60 (was §5.1a, rev 7, slice 5-J) by user decision on 2026-10-22.
**Runs after:** Phase 6 §6.4 quota measurement taken in `heads` mode ([70_phase6_testing_rollout.md](70_phase6_testing_rollout.md) §6.4).
**Default stays `heads`.** Nothing in Phase 5 or 6 depends on this job.

## 0. Why this is a separate job
- It touches `WsSession`, `ProviderPool` and `StaleDetector`, which Phase 5 slice 5-C and 5-D also change (new `ProviderPool` API, `on_tag` callback). Doing both at once makes review hard.
- Going live does not need it. A dead WS loses no data in either mode (release comes from getLogs).
- Whether it is worth doing depends on a measurement we do not have yet: the WS provider's real quota use in `heads` mode, measured in Phase 6.

## 1. Decision gate (start only if true)
Start this job when **either** holds after Phase 6:
- The Phase 6 quota measurement (provider dashboards, calls/day) shows `heads`-mode notifications (≈ 43,200/day) put the WS provider near or over its plan limit; **or**
- You decide the cost saving alone justifies it.

If neither holds, close this job without implementing. Record the Phase 6 numbers in the closing note.

## 2. Goal and why it is safe
**Why:** a `newHeads` subscription is billed per notification. Base makes a block every 2 s, which is about 43,200 notifications/day on the WS provider, whether or not any contract event happened. `ping` replaces that with one JSON-RPC request every few minutes. Example: at 300 s that is 288 requests/day.

**Why it is safe:** the WS is not on the release path. Release comes from getLogs plus the getLogs provider's own tags (Phase 3 §3.3/§3.5). A dead WS loses no data in either mode. It only delays the WS-only features listed below.

## 3. Config (`[ws]`, new section, every key optional)
| Key | Default | Meaning |
|---|---|---|
| `liveness` | `"heads"` | `"heads"` = current behaviour. `"ping"` = no `newHeads` subscription; liveness comes from a JSON-RPC ping over the same socket |
| `ping_interval_s` | `300` | seconds between pings. Validation: `30 ≤ ping_interval_s ≤ 900`, otherwise exit 2 |
| `ping_stall_count` | `2` | this many consecutive bad pings in a row end the session with `Retry` |

An unknown `liveness` value exits 2.

## 4. What the ping is
- It is `eth_blockNumber` sent over the **live WS socket**.
- It is not the existing Beast WebSocket ping frame. Those frames stay on, but they only prove the TCP/WS link is up; a stuck node behind a live socket still answers them.
- It is not `eth_chainId`, which a gateway can answer from cache without asking the node.
- It is not the HTTP endpoint, which would say nothing about this socket.

A ping is **bad** if any of these happen:
- no reply within `request_timeout`;
- an RPC error (classified as usual: quota and auth errors end the session with their own kind);
- the block number did not advance since the previous good ping. Base makes about 150 blocks in 300 s, so "no advance" means the node is stuck.

## 5. What changes in `ping` mode (audit of every `newHeads` consumer)
| Consumer of `newHeads` | `ping` mode behaviour | Impact |
|---|---|---|
| Session stall (`head_stall_ms`, `ProviderPool::tick`) | Replaced by `ping_stall_count` bad pings in a row. Detection takes ≈ `ping_stall_count × ping_interval_s` (10 min at defaults) instead of 15 s | WS failover is slower. No data impact |
| WS head lag vs the getLogs provider (`StaleDetector::ws_stale`) | Compared **once per ping** against the getLogs provider's latest `eth_blockNumber` (polled every `safe_poll_ms`). The ping head is not kept as a standing value: a 5-min-old head would read as ≈ 150 blocks behind and demote a healthy WS. `ws_lag_for` becomes "`ping_stall_count` lagging pings in a row" | Same rule, coarser timing |
| `StaleDetector::divergence()` head term | Uses the same per-ping comparison. The safe-tag term is unchanged | none |
| Hash ring (`ReorgDetector`, 1024 blocks) used by `LogSource::block(n)` (Phase 5 §5.4a) | Ring stays empty, so every distinct event block reads its canonical hash with `eth_getBlockByNumber` on a provider other than the one that returned the logs. That is already the path today for blocks outside the ring (catch-up) | Same safety (still an independent source). Cost: at most one call per distinct event block per page (the sweeper keeps `out.blocks` per page, so many logs in one block cost one call) while Live (≤ ≈ 9.7k/day at today's event rate, usually fewer), on a non-getLogs provider, paced by `reconcile_rps` |
| Unsafe-reorg detection above `G_s` | Not detected. Orphaned WS logs show up as `ws_extra_logs_total` when the sweep covers the block, instead of being purged early | Metric noise only. Nothing above `G_s` is ever released |
| Safe-reorg detection | **Unchanged.** It comes from `G_s` regression, `hash(G_s)` from tag polls, the final-sweep diff and re-verify. None of these uses `newHeads` | none |
| `head` in `gate_lag_blocks{class}` and alert details | Taken from the getLogs provider's `eth_blockNumber` poll, which already runs every `safe_poll_ms` | none |
| `WebSocket Stalled` alert (Phase 5 §5.3.3) | `ping` trigger: `ping_stall_count` bad pings in a row, details use `seconds_since_last_good_ping` | Pages later (≈ 10 min at defaults) |

**Unchanged in both modes:**
- release, the cursors and the checkpoint;
- the progress watchdog, `PipelineStalled`, `Safe Gate Stuck` and `Finalized Gate Stuck`;
- the sink, spill and lease;
- standby (no WS at all);
- takeover (the new leader opens its WS in the configured mode);
- the dead-man's switch, `/readyz`, and the process watchdog;
- the `logs` subscription and the WS-vs-sweep cross-check;
- getLogs provider selection (≠ active WS when possible).

## 6. Not a breaking change
- The default is `heads`, so an existing `base.toml` behaves exactly as before.
- The stream schema, Redis keys and downstream contract are unchanged.
- `subscribe_heads=false` already exists in `WsSession` / `ProviderPool` (it requires the `logs` subscription). Today that path has no liveness check beyond Beast idle; the ping fills that gap.
- **Rollout order:** the config loader rejects unknown keys. Deploy the new binary before any `base.toml` that contains `[ws]`, or the old binary exits 2.

## 7. Endpoint feasibility
| Provider | WS in our config | `ping` mode |
|---|---|---|
| QuickNode | yes (WS #1) | standard `eth_blockNumber` over WSS. Billed per request; `newHeads` is billed per notification |
| Infura | yes (WS #2, `BASE_WSS_URL` set & verified, master F4) | standard `eth_blockNumber` over WSS |
| Ankr, public Base, Alchemy | no WS (getLogs / HTTP only) | n/a |

Credit weights per method are not verified. Measure calls/day on both dashboards in each mode (`heads` numbers come from the Phase 6 measurement; take the `ping` numbers during this job's canary) before choosing the production setting.

**Quota (hypothetical, at today's rates):**
| | WS provider | other HTTP providers |
|---|---|---|
| `heads` | ≈ 43,200 head notifications/day + log notifications | ring hits are free |
| `ping` (300 s) | 288 pings/day + log notifications | ≤ ≈ 9.7k `eth_getBlockByNumber`/day |

Log notifications (≈ 9.7k/day) are billed in both modes. If the quota is still too high after this job, the next option is to drop the WS entirely. That would also lose the cross-check and need its own review; it is **not** part of this job.

## 8. Implementation outline
- `WsSession`:
  - optional ping timer;
  - the live loop accepts a `Response` whose id matches the outstanding ping. Any other response stays a `Retry`, as today;
  - Beast allows one write while a read is pending, on the same executor.
- `ProviderPool`: the stall branch and the lag branch switch on the mode.
- `StaleDetector`: a per-sample head comparison (`on_ws_ping_head`) next to `on_ws_head`.
- `pool_config.cpp`: `subscribe_heads = (liveness == heads)`.
- `config.cpp`: parses the `[ws]` section and validates it.
- `PoolLogSource` (`LogSource` adapter, [pool_log_source.hpp](../../evm_listener/src/app/pool_log_source.hpp), [pool_log_source.cpp](../../evm_listener/src/app/pool_log_source.cpp)): within a single page, the sweeper already calls `block(n)` at most once per distinct block ([sweeper.cpp:170-180](../../evm_listener/src/pipeline/sweeper.cpp#L170-L180), `out.blocks`). Across pages (`safe_step` followed by `final_step`), `PoolLogSource` maintains a bounded 4,096-entry FIFO `block_cache_` (`kMaxBlockCache = 4096`) and preserves I3 by calling `clear_cache()` on `report_bad(...)` (G2 hash mismatch), at the start of `Sweeper::reverify_released()`, on `Host::resync_ring()`, on `SafeRegression` / `SafeHashMismatch` gate signals, and whenever a cached entry disagrees with `ring_->ref_at(n)`.
- Phase 5 / Stage 4 host interplay (verified during W-0 against [host.cpp](../../evm_listener/src/app/host.cpp), [ws_reconciler.cpp](../../evm_listener/src/app/ws_reconciler.cpp), and [host_telemetry.cpp](../../evm_listener/src/app/host_telemetry.cpp)):
  1. `Host::on_tag` ([host.cpp:193-213](../../evm_listener/src/app/host.cpp#L193-L213)) drives `FinalityGate` and `PipelineDriver::set_gates` directly from `ProviderPool`'s `on_tag` callback, so the pipeline reaches `Live` without waiting for `newHeads`.
  2. `WsReconciler` ([ws_reconciler.cpp:70-97](../../evm_listener/src/app/ws_reconciler.cpp#L70-L97)) sets `reseed_live_watermark_ = true` on a WS provider switch or disconnect (`on_ws_provider_changed`) and waits for `on_head(head_number, live_c_s)` to reseed `live_from_block_`. In `ping` mode (no `newHeads`), a good ping reply must also call `ws_reconciler.on_head(ping_block_number, live_c_s)` so the live cross-check watermark reseeds after a WS reconnect.
  3. `HostTelemetry` ([host_telemetry.cpp:140-141,245,367](../../evm_listener/src/app/host_telemetry.cpp#L140-L141)) reads `ctx.ring->top()` for `evml_head_block` and `SnapshotInputs::head`; in `ping` mode (empty ring), W-4 must fall back to the getLogs provider's latest `eth_blockNumber` poll (per §5 above).

## 9. Task list
Gate: `make build` / `make test` exit 0 (ASan/UBSan `-Werror`). No commits without the user's instruction.

- [x] **W-0 Gate check:** confirm §1 (record the Phase 6 numbers). Re-read the then-current `WsSession` / `ProviderPool` / host code; update §8 if Phase 5 changed it. *(Completed 2026-10-29: §1 gate evaluated against measured Phase 6 §6.4 numbers in §11 below; §8 updated for Phase 5/6 `PoolLogSource::block_cache_`, `WsReconciler::on_head`, `Host::on_tag`, and `HostTelemetry`.)*
- [ ] **W-1 Config:** `[ws]` section parse and validation, `base.toml` keeps the defaults (`heads`). Tests in `tests/config_test.cpp`: defaults, `ping_interval_s` below 30 / above 900 exits 2, unknown `liveness` exits 2.
- [ ] **W-2 `WsSession` ping:** timer, id-matched response, bad-ping classification (timeout, RPC error with quota/auth kinds kept, non-advancing number). Tests with `tests/mock_ws_server` answering `eth_blockNumber` mid-stream.
- [ ] **W-3 Pool + stale detector:** stall branch and lag branch switch on mode; `on_ws_ping_head`; `subscribe_heads = (liveness == heads)`. Tests: see §10.
- [ ] **W-4 Host + alerts:** host runs without heads in `ping` mode (empty ring path via `LogSource::block(n)`, `WsReconciler::on_head` reseed on ping reply, `HostTelemetry` head fallback), `WebSocket Stalled` ping trigger and details. Test the ring-empty path asks a provider other than the logs provider.
- [ ] **W-5 Canary:** deploy the new binary to **both** services first (old binaries exit 2 on `[ws]`). Then set `[ws] liveness = "ping"` on **both** services together: the standby opens no WS, so a standby-only canary measures nothing, and mixed modes would flip WS cost on every takeover. Watch the leader for 24 h: provider-dashboard calls/day against the `heads` baseline, `WebSocket Stalled` silent, stream gap-free. Roll back = remove `[ws]` and redeploy. Record the decision here.
  - Config is baked into the image (`/app/config/base.toml`), so changing mode is a redeploy; both services build from the same commit.

## 10. Tests (MockProvider WS that answers `eth_blockNumber` mid-stream)
- a quiet stream (no logs) with good pings stays up past `head_stall_ms`;
- a ping timeout, an RPC error, or a non-advancing number `ping_stall_count` times in a row ends the session with `Retry` and fails over;
- quota and auth errors on a ping keep their classification;
- a healthy ping head is never demoted for lag between pings, but a ping head > `stale_after_blocks` behind the getLogs head for `ping_stall_count` pings is demoted;
- with the ring empty, `block(n)` asks a provider other than the one that returned the logs;
- calls per block: a first page with 200 logs spanning 50 uncached blocks and an empty ring makes 50 `eth_getBlockByNumber` calls (the sweeper's per-page `out.blocks`), and `PoolLogSource::clear_cache()` invalidates `block_cache_` on `report_bad` / `reverify_released` / safe-reorg signals;
- `heads` mode is byte-for-byte unchanged (the existing session and pool tests pass untouched);
- config: out-of-range `ping_interval_s` and an unknown `liveness` value exit 2.

## Acceptance
`heads` mode unchanged and green; `ping` mode passes the §10 tests; the W-5 canary shows the WS provider's calls/day drop as predicted (≈ 288 pings/day instead of ≈ 43,200 notifications) with no gap in the stream.

## Accepted trade-offs in `ping` mode
- WS stall detection takes minutes instead of 15 s;
- no unsafe-reorg purge (metric noise only);
- canonical hashes come from `eth_getBlockByNumber` on a non-getLogs provider.
- Release, safe-reorg handling and recovery are unaffected.

## 11. Phase 6 §6.4 Gate Decision (2026-10-29)

Measured steady-state `Live` traffic from [70_phase6_testing_rollout.md](70_phase6_testing_rollout.md) §6.4 (`evml_rpc_requests_total` and `evml_ws_messages_total` over a 180 s post-catch-up window on Base mainnet):

- **QuickNode (WS primary):** `43,200` `newHeads`/day (`0.5/s`) + `4,800` `logs`/day + `1,440` HTTP `eth_getBlockByNumber("safe")`/day (60 s cross-check) = **`49,440` msgs+calls/day (`1.48M / 30d`)**, leaving **~70%–85% headroom** on the 10M credits/month Discover plan (~1.48M–2.97M credits/30d at 1–2 credits/message).
- **Infura (HTTP primary):** **`37,920` calls/day measured (`1.14M / 30d`)** (`16,320` `eth_blockNumber` + `19,680` `eth_getBlockByNumber` + `1,920` `eth_getLogs`; **`65,280` calls/day = `1.96M / 30d`** worst-case if 100% of 3 s/15 s tag polls hit Infura), leaving **~35%–62% headroom** on the 3M requests/month Core plan.
- **Verdict:** Neither provider breaches the `< 30% headroom` (`> 70% monthly usage`) gate in [70_phase6_testing_rollout.md](70_phase6_testing_rollout.md) §6.4. Per §1 above, `heads` mode remains the production default for go-live and **Job 71 is deferred / closed for v1** without implementing W-1..W-5 (ready to open only if provider credit weights or multi-shard expansion later require cutting `newHeads` traffic by 99.3% from `43,200/day` to `288/day`).

## Changelog
- 2026-10-22: created from plan 60 §5.1a (rev 7), moved out of Phase 5 by user decision.
- 2026-10-22 review: renamed from `70_ws_ping_liveness.md` (70 is the Phase 6 plan); "§6.4" now confirmed against the Phase 6 plan; W-5 canary fixed (a standby-only canary measured nothing because the standby opens no WS).
- 2026-10-27: added a per-sweep block-info cache in the `LogSource` adapter (§5 cost row, §8, W-4) and its test in §10 (200 logs over 50 blocks → exactly 50 calls).
- 2026-10-27 (Phase 5 review Q4): that cache is **dropped**. The sweeper already reads each distinct block once per page, and a longer-lived cache would break the final sweep's canonical-hash re-check (I3). §5 cost row, §8, W-4 and §10 updated; the 50-calls test stays, without a cache.
- 2026-10-29: completed W-0 gate check against Phase 6 §6.4 measurements (`QuickNode` ~70%–85% headroom, `Infura` ~35%–62% headroom) and audited Phase 5/6 code (`PoolLogSource::block_cache_` with I3 invalidation on `report_bad`/`reverify_released`/safe-reorg, `WsReconciler::on_head`, `Host::on_tag`, and `HostTelemetry`); updated §8/§10, added §11, and closed/deferred Job 71 for v1.
