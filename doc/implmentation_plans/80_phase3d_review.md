# Phase 3d review: deviations, gaps, and plan changes

I looked at this the way you would review a market-data or settlement feed. Wrong data costs more than late data, and a correction must make sense on its own. **R** marks a recommendation, and **Plan** names the file that changes.

## Summary

| # | Item | Severity | Recommendation | Where |
|---|---|---|---|---|
| N1 | **Dedup swallows a log that becomes canonical again** (A→B→A) | 🔴 correctness | Dedup keeps the *last emitted type* per event, not a set-once key | 3e + Phase 4 |
| N2 | **A new contract with an older `start_block` is silently skipped** | 🔴 silent gap | Store a contract-set fingerprint; fail fast at startup and backfill with an explicit tool | 3e + Phase 5 |
| D1 | What a revert carries | 🟠 design | Keep the full original fields (the "before image") | Phase 4 |
| D2 | A late log is labelled `finality=finalized` | 🟢 accept | Keep it, plus a WARN; document it in the consumer contract | docs |
| G1 | 429 on a wide range is not treated as a range problem | 🟠 throughput/stall | Treat any failure on a wide range as a reason to shrink, and add a token bucket per provider | 3e/Phase 5 |
| G2 | A bad response does not penalise the provider, and the canonical hash may come from that same provider | 🔴 trust | Report the bad provider to the pool, retry the page elsewhere, and check hashes against an independent source | 3e/Phase 5 |
| G3 | A safe reorg is only noticed when the final sweep runs (≈ 15–20 min later) | 🟠 latency of correction | Add a step that immediately re-checks `(C_f, C_s]` | 3e |
| G4 | Progress watchdog and pacing are missing | 🟢 planned | Driver loop with deadlines and escalation | 3e |
| G5 | I4 ("strictly increasing") cannot hold for corrections | 🟢 spec fix | Narrow I4 to the normal path; corrections are ordered by consumers | plan |
| G6 | One page can contain more events than the sink queue holds | 🟢 confirm | Writes always land (queue or WAL); check capacity before each page | Phase 4 |

---

## N1. Dedup swallows a log that becomes canonical again

**Problem.** Dedup is set-once: `SET NX dedup:{id}:log EX 7d`. Consider a block flip-flop A→B→A:
1. The log is emitted; `dedup:{id}:log` is set.
2. Reorg to B: a revert is emitted; `dedup:{id}:revert` is set.
3. Back to A, so the same `event_id` is canonical again. The re-emission hits the existing `dedup:{id}:log` and is **dropped without any error**.

The consumer's ledger now shows the trade as reverted, even though it is on-chain. The in-process `RecentSet` has the same flaw, because its key also contains the type.

On Base this needs an L1 reorg that reverts and then restores a block before finality. That is rare, but it is exactly the failure you only find in a post-mortem.

**R (an idempotency state machine, the same pattern as order-state handling in an OMS).** Keep one key per event that stores the last type emitted:
```lua
-- emit.lua (rev 5)
local last = redis.call('GET', KEYS[1])             -- KEYS[1] = dedup:{event_id}
if last == ARGV_type then return false end          -- same type again: duplicate
redis.call('SET', KEYS[1], ARGV_type, 'EX', ttl)
-- XADD ... / ZADD released ... as before
```
- `log, log` collapses to one log. `log, revert, log, revert` all pass. A revert never comes before its log, because there is nothing to revert.
- The in-process layer becomes a bounded `event_id → last type` map with the same rule.
- Cost: one key per event instead of up to two, so memory goes down.

## N2. A new contract with an older `start_block` is silently skipped

**Problem.** The cursors cover the whole class. Suppose you add a contract next month with `start_block` below the stored `C_s`. Its history is **never fetched**, and nothing fails. In a fund this is the worst failure mode: the dataset looks complete and is not.

**R (borrowed from indexers like Ponder and graph-node, which track coverage per data source).**
- v1: store `ckpt.contracts` = the sorted list of `address:start_block:class` next to the checkpoint. *(Integration refinement: originally a sha256; a hash can only say "changed", not which contract is uncovered.)*
- At startup, any contract whose `start_block ≤ C_s` and that is not in the stored set makes the process **exit with code 2**. The error names the contract and gives the fix.
- The fix is a new tool, `--backfill-contract <name>`. It runs a one-off `Sweeper` with a filter for that address only: `[start_block, C_f]` at finalized and, for a safe-class contract, `(C_f, C_s]` at safe and tracked in `released` *(refinement: stopping at `C_f` would leave that window uncovered)*. It emits through the same sink (dedup makes this idempotent) and then updates the manifest.
- v1.5 (optional): run the same thing automatically as a third sweep. This is deliberately not in v1, because it breaks per-class ordering in ways consumers must understand.
- Contracts with `start_block` = gate, or above `C_s`, need nothing. Removed contracts need nothing either.

## D1. What a revert carries: keep the full fields

**Battle-tested reference.** Change-data-capture tools such as Debezium include the full prior row (the "before image") in a delete event. Accounting does the same: a reversing entry mirrors the original amounts. A consumer that keeps no state, such as a PnL aggregator, must be able to post the contra entry from the revert message alone.

**R.** Keep `ReleasedEntry.fields`. In Phase 4:
- `emit.lua` writes `HSET released_data {event_id} <packed fields>` in the same script as the `ZADD released`, so the two are atomic.
- `final_trim.lua` runs `ZRANGEBYSCORE released -inf G_f` → `HDEL released_data …` → `ZREMRANGEBYSCORE`, all bounded by the `(C_f, C_s]` window (≈ 140 events).
- `released_range` returns the fields after a restart, so the D6a fix still holds.
- Packing format: a msgpack or JSON array of the field list. Memory is tiny (< 200 KB).

## D2. Late log labelled `finality=finalized`: accept

The log was missed by the safe sweep and recovered at finality, so `finalized` is the truth. It must not be tracked in `released`, because nothing can reorg it any more.

**R.**
- Keep the current behaviour.
- `safe_missed_total` plus a WARN. If it happens more than 3 times in an hour, raise a `SafeSweepUnreliable` alert, since repeated misses mean the safe-sweep provider is lagging its own `safe` tag.
- Consumer contract (README §5): a safe-class contract can occasionally produce `finality=finalized` entries; treat them as normal logs.

## G1. 429 on a wide range

The classifier can't tell "too many requests" from "this query is too expensive" (Infura answered 2000 blocks with 429). Two battle-tested approaches combine here:
- **TCP-style congestion control:** a failure triggers a decrease no matter what caused it. The sweeper treats `RateLimited`, a timeout or a server error on a range wider than `min_range` as a range signal too. It halves once, then retries after `retry_after`. Wrongly shrinking costs a few extra requests; wrongly not shrinking stalls the sweep forever.
- **Token bucket per provider** (`reconcile_rps`, default 2, burst 4) in the `LogSource` adapter, so bursts during catch-up don't cause the 429s in the first place.
- Also parse a suggested range from the provider's error message. Infura, Alchemy and QuickNode use different wording; Ponder's getLogs retry helper does this. `range_limit` already carries it when the classifier finds one.

## G2. Bad responses do not penalise the provider, and the hash check is not independent

**Problem 1.** A provider that returns logs outside the requested range, or with stale hashes, is retried and keeps being chosen. The sweep stalls on it instead of failing over.

**Problem 2.** `LogSource::block(n)` goes through the same getLogs provider. A node that is consistently wrong (a lagging replica behind a load balancer) can pass its own canonical-hash check.

**R (two independent sources, as with market data).**
- `LogSource::report_bad(provider, reason)` calls `on_failure(Retry)` on that provider and increments `provider_inconsistency_total{p}`. Then **retry the same page on another provider** (`pick_getlogs(exclude={p})` already exists). After `failover_after` failures the provider is `Down` and recovers only through a probe, which is the fix just committed.
- The canonical hash comes **from the hash ring first.** The ring is fed by the WS provider, which is by design a different provider from the getLogs one. Only on a ring miss (blocks older than the ring) does it fall back to `eth_getBlockByNumber`, on a provider **other than** the one that returned the logs.
- If the two sources disagree, the page is not released and the conflict is recorded. If there is no quorum after N attempts, the sweep stalls and raises CRITICAL. It never picks one source arbitrarily.

## G3. A safe reorg is only noticed when the final sweep runs

Plan 3.6 says to re-check `(C_f, C_s]` **immediately** on a `SafeRegression` or `SafeHashMismatch`. Today only the final sweep finds it, ≈ 15–20 min later, and consumers act on bad data in the meantime.

**R.** Add `Sweeper::reverify_released()`:
- It runs the safe-class getLogs over `(C_f, C_s]` in pages.
- It diffs against `released` using `diff_released`.
- Vanished entries get a revert. New logs are emitted at `finality=safe` and tracked in `released`. Matched entries need nothing.
- Then `C_s = min(C_s, F)` and a CRITICAL `SafeReorg`.
- The same code serves the deep-reorg path from the `ReorgDetector` (when `crosses_safe` is true).
- Nothing in `C_f` moves.

## G4. Driver loop: watchdog and pacing (3e)

- **Progress deadline per cursor:** if the gate is ahead and the cursor has not moved for `progress_stall_ms`, report the current provider as bad (G2) and log a WARN. After 3 such deadlines, raise the CRITICAL `PipelineStalled` alert.
- `Retry` is retried with jittered backoff (reuse `rpc::Backoff`). `Stalled` stops that sweep and alerts; the other sweep keeps going if it can, without crossing `C_s`. `Paused` waits for the sink.
- **Bulkheading:** the safe sweep always has priority. The final sweep runs only when the safe sweep reports `NoWork`, or every N steps, so slow finalization never adds delay to the safe stream.

## G5. Narrow I4

The new wording: I4 holds on the **normal release path**. Corrections, meaning reverts, late logs and backfilled contracts, are out of order by nature. Consumers order by `(block_number, log_index)` and handle each `event_id` idempotently, which is already in the consumer contract. The property-test checker asserts I4 only for `type=log` entries that are not late.

## G6. Page size versus sink capacity

One Ankr page of 10k blocks can hold thousands of Swap events. `emit()` never fails: it goes to the queue, or to the WAL when the queue is full. That keeps correctness intact.

**R:**
- Check `saturated()` before each page (already done).
- In Phase 4, make sure "queue full" means *spill to the WAL*, not *block*, and that `saturated` is raised only by the WAL cap.
- Cap events per page at about the queue capacity by shrinking the pager when a page returned more than `queue_capacity/2` logs. This is a soft limit and keeps memory bounded.

---

## Decisions (✅ accepted 2026-10-06: all recommendations)
1. **N1:** one last-type key per event, `dedup:{id}`; a revert with no prior log is a no-op. ✅
2. **N2:** fail fast plus a `--backfill-contract` tool in v1; automatic backfill deferred to v1.5. ✅
3. **D1:** full-field reverts via the `released_data` hash. ✅
4. D2, G1–G6 as recommended. ✅

Integrated into the bodies of [40_phase3_pipeline.md](40_phase3_pipeline.md), [50_phase4_sink.md](50_phase4_sink.md) and [60_phase5_ops.md](60_phase5_ops.md); each ends with a short "Rev 5 changelog".
