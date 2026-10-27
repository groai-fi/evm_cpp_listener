# Phase 4 pre-coding review: findings, corrections, fix plan (→ rev 6)

Reviewed against local `main` at `7036249` (`origin/main` = `02cb18a`): [sink.hpp](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/pipeline/sink.hpp), [sweeper.cpp](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/pipeline/sweeper.cpp), [pipeline_driver.cpp](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/pipeline/pipeline_driver.cpp), [sweep_fakes.hpp](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/tests/sweep_fakes.hpp), [base.toml](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/config/base.toml), [.devcontainer](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/.devcontainer), and plans 00/40/50/60. Nothing was built or run. All findings come from reading code and specs. The updated plan is [50_phase4_sink.md (rev 6)](50_phase4_sink.md).

## Summary

| # | Finding | Severity | Fix (rev 6) | Slice |
|---|---|---|---|---|
| P1 | A pipelined batch can apply a `ckpt` after an earlier emit in the same batch failed | 🔴 silent gap | **Barrier items**: ckpt / init / contracts / trim / reads are sent only after every earlier item is acked | 4d |
| A | **Read-your-writes**: `released_range` can miss emits still queued in the sink, so a vanished log never gets a revert | 🔴 silent phantom | Reads are FIFO barriers; while Degraded/Replaying, reads return *unavailable* and the sweep pauses | 4a + 4d |
| B | **Lua has no rollback**: `emit.lua` sets dedup *before* `XADD`; a failure between them makes the retry look like a duplicate, so the event is lost | 🔴 silent loss | Write dedup **last**; `#!lua` shebang so an OOM rejects the script up front; min Redis → **7.0** | 4c |
| P3 | A stale writer (old leader, or the standby replaying its WAL) can write below the new leader's `C_f` | 🔴 phantom | **Lease-term fencing** in every write script; old-term WAL **discarded**, never replayed; `BELOW_FINALIZED` assertion | 4b–4e |
| P2 | Redis unreachable at startup: §4.3 says exit 2, §4.6 says never crash (Railway crash loop); the "WAL-tail checkpoint" fallback is undefined | 🟠 | Unreachable → wait in `Init` with backoff; only config/compat failures exit 2; drop the WAL-tail fallback | 4e + 4a |
| C | `Sink` reads have no "unavailable" result; an exception would escape `PipelineDriver::tick()` | 🟠 | Reads return `std::optional`; nullopt → Recovering-retry / `Paused` | 4a |
| D | Phase 5 §5.5 contradicts itself: renew failure ⇒ demote, but "Redis down ⇒ leader keeps running in WAL mode" | 🟠 | Resolved by fencing (keep running into the WAL; `FENCED` on reconnect ⇒ discard + demote) | 4e, plan 60 |
| P4 | Lease renewals share the sink FIFO, so a long replay can starve them; `SinkItem` has no read items | 🟠 | Lease + heartbeat on a separate connection, serviced between batches; replay batches ≤ 50 ms; read items added | 4d |
| E | First-run init is 3 separate enqueues (Phase 3 gap "partial checkpoint") | 🟢 | `ckpt.lua` init mode writes all three atomically; new `Sink::init_checkpoint` | 4a + 4c |
| F | A discarded old-term WAL may hold an unapplied revert; only the final sweep (≈ 15–20 min) would catch it | 🟢 | `recover()` queues one `reverify_released()` whenever a checkpoint existed (≈ 1–2 pages) | 4a |
| P5 | Dedup keys live 7 d; "second backfill adds 0" holds only within 7 d; memory estimate excludes backfill | 🟢 doc | Document the horizon; add backfill to the estimate | doc |
| P6 | `MINID` trims entries a lagging consumer hasn't read; nothing alerts | 🟠 | 48 h read deadline in the consumer contract + consumer-group lag probe (Q2 ✅, plan §4.10) | 4e + Phase 5 |
| P7 | `ckpt.contracts` plain `HSET` → a stale replay can roll it back | 🟢 | Subsumed by fencing (see correction 2) | 4c |
| G | `saturated()` read on the io thread, written on the sink thread | 🟢 | `std::atomic<bool>` | 4d |
| H | `rewind_safe()` lowers `C_s` in memory while stored ckpt is set-if-greater; undocumented | 🟢 doc | Document why it is safe | doc |
| I | No Docker CLI in the dev container; no `toxiproxy` in compose (Phase 0 planned it); a Redis 6.x image test is impossible in-container | 🟢 | Add `toxiproxy` (container recreate, image unchanged); version check = unit test on `INFO` fixtures; eviction via `CONFIG SET` | 4d |
| J | Unwritten accepted risk: Redis AOF `everysec` tail loss **and** a reorg of those blocks ⇒ consumer keeps a never-reverted log | 🟢 doc | Add to accepted risks | doc |
| K | 00 §5 status is stale: property test is committed (`7036249`), not pushed | 🟢 doc | Patch 00 | doc |

## Corrections to my previous answer
1. **P3's fix was too weak.** "Reject a block ≤ `C_f` without a dedup key" covers only one write kind. Any write from a stale term (emit, ckpt, contracts, trim) is the hazard, so the fix is lease-term fencing, with the `C_f` check kept as an assertion.
2. **P7's fix would have broken existing code.** "Write the manifest only with an advancing cursor" contradicts [pipeline_driver.cpp:104](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/pipeline/pipeline_driver.cpp#L104), which legitimately rewrites the manifest without moving a cursor (a contract added above `C_s`, or one removed). Fencing covers P7 instead.
3. **P2 needs no WAL-tail checkpoint.** With the lease enabled no host can lead without Redis, and old-term WALs are discarded. The checkpoint therefore always comes from Redis, and "fallback: WAL tail" in 40 §3.2 is removed.
4. **"241 tests passed" was a doc figure, not a run.** [CMakeCache](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/build/Debug/CMakeCache.txt) has `CMAKE_BUILD_TYPE=Debug` and `EVML_SANITIZE=ON`. One fresh `ctest` in the dev container re-runs the suite **and** closes the ASan/UBSan gap (step 0).

## Details

### P1. Barriers
[sweeper.cpp:207-217](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/pipeline/sweeper.cpp#L207-L217) enqueues a page of emits followed immediately by `checkpoint`, so they share one pipelined batch. Redis executes every command in a pipeline even when an earlier one errors. If emit #3 fails (timeout, `NOSCRIPT` after a Redis restart, OOM), the following `ckpt` is still applied. Because it is set-if-greater, no retry can undo it.
Client-side ack accounting can't fix this, because the server has already run the command. A batch must **end** before any barrier item, and the barrier is sent only after every earlier item is acked. Cost: about one extra round trip per sweep page (≈ every 12 s).

### A. Read-your-writes
`final_step` ([:233](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/pipeline/sweeper.cpp#L233)) and `reverify_released` ([:288](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/pipeline/sweeper.cpp#L288)) diff the sweep against `released_range`. The preceding `safe_step` emits may still be queued or in the WAL. If one of them vanished in a reorg, it is in neither the sweep nor Redis, so the diff ignores it. `final_trim` later deletes it, and no revert is emitted. The synchronous `MemorySink` hides this.

### B. Partial Lua execution
Redis does not roll back a script that fails midway. Rev 5 runs `SET dedup` before `XADD`. If `XADD` fails, the retry sees `last == type` → "duplicate" → loss.
Rev 6 writes the stream entry and `released` first and the dedup state last, so a mid-script failure produces a duplicate on retry instead. The `#!lua` shebang (no `allow-oom`) makes Redis refuse the script before it starts when over `maxmemory`, the most likely mid-script failure under `noeviction`.
Shebang flags need Redis ≥ 7.0. **Verify** empirically (integration test 18) instead of trusting docs.

### P3. Fencing (standard epoch / fencing-token pattern)
Rule: discard old-term WALs. **Why that loses nothing:** a `ckpt` item always travels in order behind the events it covers. Anything still in an old-term WAL is therefore either already in Redis or above the stored checkpoint, which the new term re-sweeps. Replaying it instead can write stale data below the new leader's `C_f`, a range no sweep revisits. See rev 6 §4.2/§4.5/§4.8 for the mechanics.

### H. Rewind semantics
`rewind_safe(F)` lowers `C_s` in memory only. After a crash, recovery resumes from the higher stored value. That is correct because reverify has already reverted vanished logs and emitted new ones for `(C_f, old C_s]`, and `released` reflects it. The rewind only costs a redundant re-sweep that dedup collapses.

## Patches to other plan files (I can't edit the earlier conversation's folder; apply or ask me to produce full copies)

> [!NOTE]
> **Applied (2026-10-14)** in this folder's [00_master_plan.md](00_master_plan.md), [40_phase3_pipeline.md](40_phase3_pipeline.md) and [60_phase5_ops.md](60_phase5_ops.md), with D1 wording (spill on ephemeral disk) in place of "WAL" below. The list is kept as history; the plan files are authoritative.

**00_master_plan.md**
- Header "rev 5" → "rev 6"; plan table row 50 → "Redis Lua sink (fenced), barriers, WAL (term-scoped), lease term".
- Decisions log, row Redis: "Railway 8.4.0 … **min 7.0** (Lua shebang flags)".
- Arch diagram line 77: `└─ Redis down → WAL on /data → ordered replay (same lease term only; old term discarded)`.
- §5 Status: "Phase 3e done and committed (`7036249` local, not pushed). Next: Phase 4 rev 6 ([81_phase4_review.md](81_phase4_review.md)), step 0 baseline first."

**40_phase3_pipeline.md**
- §3.2 diagram: `load ckpt from Redis (fallback: WAL tail) · replay WAL to Redis first` → `load ckpt from Redis (wait with backoff while unreachable) · same-term WAL replayed by the sink before reads are served`.
- §3.2 Recovering: add "First run writes `{safe, finalized, contracts}` in one atomic `init_checkpoint`. When a checkpoint existed, queue one `reverify_released()` before normal sweeping (rev 6, F)."
- §3.5: add "`released_range` returns nullopt while the sink is Degraded/Replaying; the final sweep and reverify return `Paused`. A read is served only after every earlier write is acked (rev 6, A)."
- Gaps carried forward: strike "partial checkpoint" (fixed by E) and "ASan/UBSan not re-run" once step 0 passes.

**60_phase5_ops.md**
- §5.5 Standby: `only runs its sink thread (to replay its own WAL if one exists)` → `runs no sink writes; an old-term WAL is discarded on the next acquisition (rev 6)`.
- §5.5 Demotion: replace with "If renew returns `FENCED` or another term holds the lease ⇒ stop releasing, drop WS, discard the WAL. If Redis is merely unreachable ⇒ keep sweeping into the WAL; the first op on reconnect is renew/resume (Phase 4 §4.8)."
- §5.5 Redis down for both: drop "it owns the WAL, so this is safe"; reference fencing.
- §5.3.3 alerts: add `Redis Unreachable` (critical, > 60 s, `{since_s, addr_family}`), `Sink Fenced` (warning, `{old_term, new_term, discarded_records}`), `Sink Invariant` (critical, `BELOW_FINALIZED`, `{event_id, block, c_f}`).

- §5.3.3 alerts (r6, Q2), from the Phase 4 §4.10 probe, rate-limited to one per group per 15 min:
  | Title | Condition | severity | `details` |
  |---|---|---|---|
  | Consumer Lagging | `max(undelivered_age_s, pending_age_s)` > `consumer_lag_warn_s` (12 h) | warning | `stream, group, undelivered_age_s, pending_age_s, lag, retention_s` |
  | Consumer Near Trim | same > `consumer_lag_crit_s` (24 h) | critical | same |
  | Consumer Entries Trimmed | `trimmed_unread` | critical (data loss already happened; recovery = consumer re-reads from its own store, or a backfill into a fresh stream) | `stream, group, last_delivered_id, max_deleted_id` |
  | Consumer Group Missing | configured group not found for 10 min | warning | `stream, group` |
- §5.4 metrics (r6, Q2): `evml_consumer_undelivered_age_seconds{stream,group}`, `evml_consumer_pending_age_seconds{stream,group}`, `evml_consumer_lag_entries{stream,group}`.
- §5.6 Railway: drop the `/data` volume and `RAILWAY_RUN_UID` (D1: the spill lives on ephemeral disk).

## Answered questions
- **Q1 ✅ yes.** Lease-disabled mode (local only) skips the fence; the spill always replays after an outage within the process. Under D1 the spill is wiped at every process start in both modes. Integrated: plan §4.5, §4.8, test 18.
- **Q2 ✅ yes.** Consumer-group lag probe (read-only, leader only, 60 s) in Phase 4 §4.10 + config; alert rules and metrics in Phase 5 (patch above); tests 19 + config unit. Open item: the real consumer group name(s).
