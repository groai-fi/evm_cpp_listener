# Phase 3: Pipeline (`evml::chain`, `evml::pipeline`), rev 6

**Goal:** an event stream with no gaps, deduplicated, in `(block, logIndex)` order, that stays correct across disconnects, restarts and reorgs.

## 3.1 Invariants (encode as asserts and tests)
Each contract has a **finality class** `k ∈ {safe, finalized}` (config, W3 = c). There are two gates, `G_s` (safe tag) and `G_f` (finalized tag), with `G_f ≤ G_s`, and two checkpoints, `C_s` and `C_f`.
- **I1 (checkpoint):** `C_s = x` means every safe-class log in blocks ≤ x that was canonical at `safe` when swept has been acked by Redis. `C_f = y` means every log of **both** classes in blocks ≤ y has been verified against the canonical chain at `finalized`, and any divergence has been corrected (missing → emitted, vanished → reverted).
- **I2:** `C_f ≤ C_s`, `C_s ≤ G_s`, `C_f ≤ G_f`.
- **I3:** a log of class k is released only if `block ≤ G_k` **and** `blockHash == canonical_hash(block)` at that time.
- **I4:** on the normal release path, release order within a class is strictly increasing `(block_number, log_index)`. Corrections (reverts, late logs, contract backfills) are out of order by nature. Consumers order by `(block_number, log_index)` and are idempotent per `event_id` (README §5).
- **I5:** no code path advances C_s or C_f past a range that has not been swept. If a step fails, the pipeline stalls and alerts. It never skips.
- **I6 (rev 5):** for every `event_id`, the emitted types alternate `log, revert, log, …`, starting with `log`. No two consecutive entries of the same type.

## 3.2 Pipeline state machine (process level)
```
Init ──► Recovering ──► CatchingUp ──► Live ──► Draining ──► Exit
            │               ▲  │          │
            │               └──┴──────────┘   (gap detected / R falls far behind G)
            └─ load ckpt from Redis (wait with backoff while unreachable) · same-process spill replayed by the sink before reads are served
```
- **Recovering:** read `ckpt` = `{safe, finalized, contracts}` via `load_checkpoint()`. While Redis is unreachable, `load_checkpoint()` returns `std::nullopt` and the driver waits in Recovering with `idle_poll` backoff (rev 6, C). If absent on first run, call atomic `init_checkpoint(c0, manifest)` to write safe = finalized = c0 and contracts in one call (rev 6, E). If present, compare the `contracts` manifest with the config: a contract with `start_block ≤ C_s` that the stored manifest does not list ⇒ **exit 2**, naming the contract and the `--backfill-contract` command (rev 5, N2). If the recovered checkpoint has a released window (`*ck.safe > *ck.finalized`), queue one `reverify_released()` before normal sweeping (rev 6, F). Both sweeps resume from their own cursor; the `released` zset and `released_data` hash (Phase 4) survive restarts, so the final sweep can still diff blocks released before a crash and build full-field reverts (fixes D6a).
- **CatchingUp:** both sweeps page through their ranges. The final sweep for a range always runs after the safe sweep has covered it, and WS sessions are already subscribed (subscribe-first ordering fixes D2).
- **Live:** `G_s − C_s ≤ 1 page` and `G_f − C_f ≤ 1 page`. Each gate advance triggers one sweep of its class.
- **Draining** (SIGTERM): stop releasing, flush the sink queue (bounded by `RAILWAY_DEPLOYMENT_DRAINING_SECONDS − 5 s`), persist both checkpoints, close sessions, exit 0.
- Per-session WS states live in Phase 2 (`Connecting → Subscribing → Live → Closed`). Pipeline correctness **does not depend** on any WS being up. WS only affects latency, liveness and observability.

## 3.3 Finality gates (`chain/finality_gate`)
- `G_s` / `G_f` come from the **current getLogs provider's own** `safe` / `finalized` tags, polled every 4 s / 12 s (Phase 2 §2.6). A sweep's `toBlock` is therefore always a block that provider has seen, so a lagging node cannot turn "not yet indexed" into a silent miss. The WS provider's tags are a 60 s cross-check.
- `depth` (tests only): `G = verified_head − depth`, where `verified_head` is the highest head with continuous parent-hash linkage in the hash ring.
- `G_f` must be monotonic **per provider**. A decrease from the same provider is a CRITICAL anomaly; release is frozen and the system alerts. After a getLogs failover the new provider may report a lower tag. That is not an anomaly: the sweeps simply wait until its tag passes the stored cursors (C never regresses).
- `G_s` can move backwards on an L1 reorg. A regression **from the same provider** is a **safe-reorg signal** (3.6), not a crash; a lower value right after a getLogs failover is not. The HeadTracker also remembers `hash(G_s)`; if the same number later returns a different hash, that is a safe-reorg signal too.
- Metrics: `gate_lag_blocks{class}` = head − G_k; `gate_age_seconds{class}`.

## 3.4 WS staging (`pipeline/pending_buffer`), now cross-check only
- `std::map<block_number, BlockBucket{hash_set, logs ordered by logIndex, first_seen_source}>` fed by the active WS session (and briefly by the previous session after a failover). Insert is idempotent by `(blockHash, logIndex)`.
- Its role: **getLogs is the source of truth for release**. When a sweep covers a block, the WS-staged logs for that block are compared with the sweep result:
  - logs in the sweep only → `ws_missed_logs_total`;
  - logs in WS only → `ws_extra_logs_total` (orphaned or not-yet-indexed);
  - then the bucket is dropped.
- This keeps the release path single-sourced and simple. The WS still cannot shorten latency below `G_s`; that is the cost of W3 = c vs the pending channel (b).
- `removed:true` on WS → erase the staged entry (it is not released yet, so no revert is needed from this path).
- Memory cap `pending_max_logs` (default 200k). When exceeded, evict the oldest buckets; nothing depends on them for correctness.
- v1.5 option `emit_pending=true` (master W3 b) is still additive: emit `status=pending` from this buffer to `evml:{shard}:pending`. Off.

## 3.5 Sweeps: reconciler / backfiller (`pipeline/reconciler`)
Two sweeps share one paging engine and the current getLogs provider (`getlogs_order`, first healthy ≠ active WS: **Infura → Alchemy (when its quota is back) → Ankr → public → QuickNode**). They are serialised, so at most one getLogs is in flight per provider.

| Sweep | Range | Filter | Output |
|---|---|---|---|
| **Safe sweep** | `(C_s, G_s]` | safe-class addresses (both pools) | emit `finality=safe`; the Lua script also `ZADD released` (Phase 4) → advance `C_s` |
| **Final sweep** | `(C_f, G_f]`, never beyond `C_s` | **all** addresses | finalized-class logs → emit `finality=finalized`. Safe-class logs → **diff** against `released` in `(C_f, G_f]`: in both → no-op; sweep only → emit late with `finality=finalized` (not tracked) + `safe_missed_total` + WARN; `released` only → emit `type=revert` carrying the retracted log's full fields + CRITICAL `SafeReorg`. Reverts are emitted before replacement logs → `final_trim` (zset + `released_data`) ≤ G_f → advance `C_f`. While sink is Degraded or Replaying, `released_range` returns nullopt and the final sweep returns `Paused` before emitting (rev 6, A/C). |
| **Re-verify** (rev 5) | `(C_f, C_s]` | safe-class addresses | runs **immediately** on `SafeRegression`, `SafeHashMismatch`, ReorgDetector walk with `crosses_safe`, or after restart with a released window (rev 6, F). Same diff as final sweep: vanished → revert, new → emit `finality=safe` + track in `released`, matched → no-op. Then `C_s = min(C_s, F)`, CRITICAL `SafeReorg`. `C_f` does not move. Returns `Paused` without reporting reorg if released set is unavailable (rev 6, C). |

- **Sink reads (rev 6, A/C):** `released_range` and `load_checkpoint` return `std::nullopt` while the sink is Degraded or Replaying; the final sweep and reverify then return `Paused` before any emit. A read is served only after every earlier write is acked (read-your-writes), so the diff never sees a released set that misses queued emissions.
- **AIMD paging:** start at `max_getlogs_range[p]` (Ankr 10,000; Infura and public 500; QuickNode 5). On `RangeTooLarge`, use the provider-suggested range if present, else halve (≥ `min_range`, which is clamped to the provider max). After 5 successes, double up to the max. **Rev 5:** any `RateLimited`, timeout or 5xx on a span wider than `min_range` also halves once (congestion-control rule: shrinking wrongly costs a few calls, not shrinking can stall forever), then waits `retry_after`. A page returning more than `queue_capacity/2` logs also shrinks the next page (soft memory cap).
- At `min_range` and still too large: split the request by address. If that still fails, CRITICAL + stall (I5).
- **Response validation:** every log must have `fromBlock ≤ block ≤ toBlock` and `removed=false`, and its address/topic0 must be in the filter. A violation discards the response, calls `LogSource::report_bad(p)` (counts as a provider failure, `provider_inconsistency_total{p}`) and retries the page **on another provider**.
- **Canonical hash per event-block (rev 5, independent source):** for each distinct block that has logs, the hash comes from the hash ring (fed by the WS provider, which differs from the getLogs provider), or else from `eth_getBlockByNumber(n)` on a provider **other than** the one that returned the logs. A mismatch means the response is discarded, `report_bad(p)`, and retried elsewhere. No agreement after N attempts ⇒ Stalled + CRITICAL; never pick a source arbitrarily.
- Progress watchdog: each cursor must advance within `progress_stall_ms` while its gate is ahead; otherwise `report_bad` the current provider (forces a switch) + WARN; after 3 consecutive deadlines CRITICAL `PipelineStalled`. This uses no fixed **total** timeout (fixes S3). The safe sweep has priority; the final sweep runs when the safe sweep has no work (or every N steps).
- Throughput (using the measured limits):
  - Safe sweep: safe advances roughly every L1 block (≈12 s) in jumps of a few blocks, so ≈ 7k getLogs/day. The finalized sweep adds ≈ 225/day.
  - Tag polls: 21.6k (safe) + 7.2k (finalized) per day, on the getLogs provider (Infura).
  - QuickNode carries only the WS (≈ 43k newHeads notifications/day plus logs) and the 60 s cross-check (≈ 2.9k/day).
  - **Verify against each plan's quota in live smoke (Phase 6 §6.4).** A 429/quota there is classified (2.3), not fatal.
  - Catch-up after 1 day down (43.2k blocks): Infura ≈ 87 pages; Ankr ≈ 5 pages; public ≈ 87 pages (best effort); QuickNode ≈ 8.6k calls ≈ 72 min.
  - Pacing: at most `reconcile_rps` (default 2) per provider, because burst getLogs triggered HTTP 429 on Infura.

## 3.6 Reorg detection and handling (`chain/reorg_detector`)
- Hash ring `number → {hash, parent}` (size `hash_ring_size`), fed by newHeads from the active session. After a failover the ring is re-seeded from the new provider, with a continuity check at the join.
- **Unsafe reorgs** (above `G_s`): new head with `parent ≠ ring[n−1].hash` → walk back with HTTP `getBlockByNumber` to fork point F. Nothing has been released above `G_s`, so the only action is purging WS-staged buckets plus metrics. This is the normal case.
- **Safe reorgs** (a block ≤ `C_s` changes; rare, needs an L1 reorg before finality). Triggers: `G_s` regression, a `hash(G_s)` mismatch, or a final-sweep divergence. Actions:
  1. Re-verify the released range `(C_f, C_s]` immediately (the **Re-verify** sweep, 3.5). Do not wait for finality.
  2. For each `released` member that is missing from the new result: emit `type=revert` with the member's full original fields (from `released_data`).
  3. For each new log: emit `type=log` (new `event_id`, since the blockHash changed). A log whose `event_id` is canonical again after an earlier revert is emitted again (I6, last-type dedup).
  4. Set `C_s = min(C_s, F)`, raise the CRITICAL `SafeReorg` alert, and resume.
- **Finalized-class** logs are never released above `G_f`, so a divergence there means a provider fault or a catastrophic L1 event: freeze and alert (unchanged).
- Metrics: `reorgs_total{depth_class=unsafe|safe}`, `reorg_depth` histogram, `reverts_emitted_total`.

## 3.7 Identity, dedup and ordering
- `event_id = "{chain_id}:{blockHash}:{logIndex}"`. logIndex is unique per block. txHash is carried as data.
- Dedup (rev 5): **per-event last-type state**, key `dedup:{event_id}` holding `log|revert`. An entry is emitted only if its type differs from the last one emitted for that `event_id`. Duplicates of the same type collapse; a log that becomes canonical again after a revert (A→B→A flip) is emitted again. This replaces the set-once `dedup:{event_id}:{type}` key, which silently dropped such logs.
- Layer 1: in-process bounded map `event_id → last type` (last 2× sweep window), which absorbs safe/final-sweep overlap and failover overlap without Redis round-trips.
- Layer 2: Redis Lua (Phase 4) is the authority across restarts and hosts, with the same rule.
- Ordering: the stream is ordered per instance. With multiple hosts the order is "first writer wins", so consumers **must** order by `(block_number, log_index)` and be idempotent on `event_id`. Write this into the downstream contract.

## 3.8 Stream event schema (v=1, flat fields)
```
v=1  type=log|revert  finality=safe|finalized
event_id  chain_id=8453  contract=<config name>  address  event=<name>  signature
block_number  block_hash  block_timestamp  tx_hash  tx_index  log_index
topics=<json array>  data=0x…
first_source=infura  observed_ms  emitted_ms  host  shard=base_eurc_usdc  listener_version
```
- Safe-class events are emitted **once** at `finality=safe`. No second "now final" entry is emitted; a later `revert` is the only follow-up, and it is rare. A consumer that needs finality certainty reads the `ckpt.finalized` field, which says every event ≤ that block is final.
- A safe-class log that the safe sweep missed arrives late with `finality=finalized` (rev 5). Consumers treat it as a normal log.
- A `revert` carries **all fields of the log it retracts** (the "before image", as in CDC delete events and reversing journal entries), with `type=revert` and a fresh `emitted_ms`. A stateless consumer can post the contra entry from the revert alone.
- `status` has been dropped. It was redundant with `finality`, and `pending` exists only in v1.5.
- Routing is by `contract.stream`: pool and gauge events go to `evml:{base_eurc_usdc}:events`, and factory and Voter events go to `evml:{base_eurc_usdc}:registry`. Both streams share the shard hash tag, so the Lua scripts stay single-slot.

## Tests
- Unit (no I/O): G_f monotonicity and G_s regression handling; per-class release; final-sweep diff (match / missing / vanished); AIMD sequences including a 5-block provider max; reorg walk-back on synthetic chains (depth 1, 3, 64; unsafe-only vs crossing `C_s`); invariant checker run after every step of randomized property tests (seeded, 10k iterations).
- Integration (MockProvider):
  - kill WS mid-range with no gap;
  - drop the `logs` subscription silently while heads continue; the sweeps recover 100% (`ws_missed_logs_total` > 0);
  - getLogs returns ">10000 results", and AIMD converges;
  - a provider returns a wrong blockHash, which is discarded and retried;
  - **safe reorg**: the mock rewrites 3 blocks below `G_s` after they were released → exact `revert` + re-emit set and a `SafeReorg` alert;
  - restart between the safe sweep and the final sweep → the diff still works from the persisted `released` set.

## Acceptance
Property tests hold I1–I6. In every MockProvider scenario the emitted set (logs minus reverts) equals the scripted canonical set exactly (no gaps, no extras, ordered per class).

## Rev 5 adjustments (after the 3d review, see 80_phase3d_review.md)
- **I4 narrowed:** strictly increasing `(block, logIndex)` holds per class on the normal release path only. Reverts, late logs and contract backfills are corrections and arrive out of order by nature; consumers order by `(block_number, log_index)` and are idempotent per `event_id`.
- **New I6:** for every event_id the emitted types alternate, starting with `log`.
- **Dedup = last-type state per event_id (N1):** both layers (in-process map and Redis) keep `event_id → last emitted type`. Emit only if the type differs; a revert with no prior log is a no-op (I6). This fixes the A→B→A flip, where a log that is canonical again would otherwise be dropped.
- **Revert payload (D1):** a revert carries the retracted log's full fields (the "before image"). `ReleasedEntry` holds the fields.
- **Late logs (D2):** emitted with `finality=finalized`, not tracked in `released`; `safe_missed_total` + WARN; `SafeSweepUnreliable` alert if more than 3 per hour.
- **3e scope added:**
  - `Sweeper::reverify_released()` (G3): runs immediately on SafeRegression / SafeHashMismatch / `crosses_safe`. It re-sweeps `(C_f, C_s]` for the safe class, diffs against `released`, reverts vanished entries, emits new logs at safe and tracks them.
  - Wide-range failure ⇒ shrink (G1): `RateLimited`, a timeout or 5xx on a span wider than `min_range` halves the pager once before the retry_after wait.
  - Bad response ⇒ `LogSource::report_bad(p)` and retry the page on another provider (G2).
  - Canonical hash from the hash ring first (the WS provider is independent); on a ring miss, use getBlockByNumber on a provider other than the one that returned the logs. Disagreement ⇒ do not release; no quorum after N tries ⇒ Stalled + CRITICAL.
  - Driver: progress deadline per cursor (WARN + report_bad; CRITICAL `PipelineStalled` after 3). Retry with jittered backoff. The safe sweep has priority over the final sweep.
  - Soft cap on events per page: shrink the pager when a page returns more than `queue_capacity/2` logs.
  - Contract coverage check at Recovering against the `ckpt.contracts` manifest (a sorted list, so the uncovered contract can be named) (N2); see Phase 5 §5.6a for `--backfill-contract`.
- **Property-test checker:** I4 is asserted only for non-late `type=log` entries, and I6 is checked.

## Results and deviations (Phase 3e)
**Delivered** (all in `evml_core`, 241 tests green: 208 unit + 33 integration):
- `LastTypeMap` (N1), `LogSource::report_bad`, attempt loop with `max_bad_attempts`, failure shrink (G1), `max_page_logs` soft cap (G6), `Sweeper::reverify_released()` (G3).
- `coverage.*`: `contract_manifest`, `uncovered_contracts`, `initial_cursor`; `Sink::set_contracts` / `Checkpoint::contracts`.
- `PipelineDriver`: recovery (first run, restart, partial checkpoint = fatal, uncovered contract = fatal naming `--backfill-contract`), safe-first scheduling with a final sweep every 4th working tick, retry/stall backoff (retried forever, I5), progress deadlines with `report_bad` and a consecutive-miss count, reverify on signal or on a final-sweep `safe_reorg`, Live iff both lags <= `live_lag_blocks`, drain, frozen gate, sink backpressure.
- `pipeline_property_test.cpp`: seeded simulation (20 seeds x 200 ticks per mode, 3 modes = 12k ticks) over a re-mined chain with random logs, retries, sink pauses, range limits, signalled and silent reorgs, and process restarts (only the sink survives, with emit.lua-style last-type dedup). Checked after every tick: I2, I3, I6, I1 at `C_f` (exact canonical set, reorged logs retracted), plus I4 and I1-safe when nothing reorgs. After the run, chaos stops, the gates move to the head, and the emitted set must equal the canonical set exactly. The reorg modes assert that reverts occurred and the no-reorg mode that none did, so the reorg checks are not vacuous.
- Scenario tests: a 3-block reorg below the safe tag (exact revert and re-emit set, fork point 104, `C_f` untouched); a restart between the safe and final sweeps (revert carries the before-image from the persisted `released`).

**Deviations**
- The manifest is a sorted list in `ckpt.contracts`, not a sha256 (so a mismatch can name the contract).
- The driver fires no alerts; it exposes `DriverStatus` and the Phase 5 alert engine evaluates it.
- Property tests do not inject malformed or wrong-hash responses (the per-call override cannot know the requested range). That path is covered by the sweeper unit tests only.
- The restart mode checks I6 on the stream after the Redis-like dedup, so it is true by construction there. The no-restart modes check it on the raw sink stream, which is where the in-process `LastTypeMap` is proven.

**Gaps carried forward**
- Real Redis `emit.lua` / `ckpt.lua` / `released_data` (Phase 4): scripts delivered and tested in 4c; `RedisSink` wiring is 4d.
- A shared per-class cursor means a later-starting contract also gets earlier blocks swept (extra RPC, no wrong events).
- A hash mismatch blames the log provider, so the Phase 5 adapter must take hashes from an independent source.
*(Partial checkpoint gap resolved by atomic `init_checkpoint` in rev 6 E; ASan/UBSan re-verified in Phase 4 step 0).*
