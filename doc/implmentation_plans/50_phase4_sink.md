# Phase 4: Sink, Downstream Fallback & Leader Lease (`evml::sink`), rev 6

**Goal:** released events reach Redis Streams at least once, idempotently and in order. A Redis outage of any length loses nothing; it only stalls the pipeline. A stale writer (old leader, partitioned host) can never write. The Redis is a **single Railway node**, an accepted SPOF (master W5).

> Rev 6 merges [81_phase4_review.md](81_phase4_review.md). Changes from rev 5 are tagged **(r6, <finding>)**.

> [!IMPORTANT]
> **Decision D1 (needs your OK): the durable WAL becomes a process-scoped spill buffer.**
> With epoch fencing (P3), a WAL left by an earlier process can never be safely replayed: its token is from an old epoch. So durability across restarts buys nothing. Correctness doesn't need it either, because the Redis checkpoint only advances behind events that already landed (P1 barriers), and a restart re-sweeps from it.
> Consequences:
> - no `fdatasync`, no torn-tail recovery, no ack-cursor file;
> - **no Railway volume** (removes the volume's no-replica, redeploy-downtime and root-mount issues from Phase 5);
> - after a crash during a Redis outage, the re-sweep costs RPC: 6 h of outage ≈ 10.8k blocks ≈ 22 Infura pages.
>
> Alternative (rejected): a durable WAL with a persisted epoch token and a resume-after-crash path. It saves those few pages at the cost of most of the WAL complexity.

## 4.1 Key schema (one hash tag per shard → every Lua script is single-slot)
```
evml:{base_eurc_usdc}:events                 Stream  pool + gauge logs
evml:{base_eurc_usdc}:registry               Stream  factory / voter discovery logs
evml:{base_eurc_usdc}:pending                Stream  (v1.5 only, W3 option b)
evml:{base_eurc_usdc}:ckpt                   Hash    {safe, finalized} set-if-greater; {contracts} manifest
evml:{base_eurc_usdc}:dedup:<event_id>       String  last emitted type `log|revert`, EX 604800 (7 d)
evml:{base_eurc_usdc}:released               ZSet    safe-class event_ids released above C_f, score = block
evml:{base_eurc_usdc}:released_data          Hash    event_id → packed field list (before image)
evml:{base_eurc_usdc}:leader                 String  lease token "<n>|<host_id>|<nonce>", PX 15000
evml:{base_eurc_usdc}:epoch                  String  token of the latest acquisition, no TTL   (r6, P3)
evml:{base_eurc_usdc}:hb:<host_id>           String  heartbeat JSON, EX 30
```
- **`ckpt.contracts`:** a JSON array of sorted `address:start_block:class` strings. Written by `init` (first run), by the driver when the manifest changes without a backfill (contract added above `C_s`, or removed; [pipeline_driver.cpp:104](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/pipeline/pipeline_driver.cpp#L104)), and by `--backfill-contract`. Every write is fenced (r6, P7).
- **`epoch`** holds the full token of the latest acquisition, including a random nonce, so a `FLUSHALL` can never make an old token match again.
- **`released_data`** is written and trimmed with `released`, in the same scripts.

**Memory estimate** at ≈ 9.7k events/day:
- streams over 48 h ≈ 20k entries ≈ 20 MB;
- dedup keys over 7 d ≈ 70k ≈ 10 MB;
- `released` + `released_data` (`(C_f, C_s]` ≈ 600 blocks ≈ 140 events) < 200 KB.

**(r6, P5)** A backfill adds its own events to the stream (48 h) and to dedup keys (7 d). With current start blocks (52,060,000) that is ≈ 100k events ≈ +15 MB for 7 days. It still fits a small Railway Redis.

## 4.2 Lua scripts (embedded, `SCRIPT LOAD` on connect, `EVALSHA`, reload on `NOSCRIPT`)
Every script starts with the **`#!lua` shebang and no `allow-oom` flag**. Redis then refuses the whole script *before* it runs when over `maxmemory`, the usual cause of a mid-script failure under `noeviction`. Shebang flags need Redis ≥ 7.0 (§4.3). **(r6, B)**

Every write script is **fenced**: it compares `GET leader` with `ARGV token` and returns `FENCED` without writing on a mismatch. Token `"0"` disables the fence (`[lease] enabled=false`, local only). **(r6, P3)**

```lua
#!lua
-- emit.lua (r6)
-- KEYS: 1 dedup:<event_id>  2 stream  3 released  4 released_data  5 leader  6 ckpt
-- ARGV: 1 ttl_s  2 minid_ms  3 type ('log'|'revert')  4 released_score (block, or "")
--       5 event_id  6 packed_fields ("" unless released_score set)  7 token  8.. field,value...
if ARGV[7] ~= '0' and redis.call('GET', KEYS[5]) ~= ARGV[7] then return 'FENCED' end
local last = redis.call('GET', KEYS[1])
if last == ARGV[3] then return 'DUP' end                       -- same type again
if ARGV[3] == 'revert' and not last then return 'DUP' end      -- nothing to revert (I6)
if ARGV[4] ~= '' then                                          -- tracked safe log
  local cf = redis.call('HGET', KEYS[6], 'finalized')
  if cf and tonumber(ARGV[4]) <= tonumber(cf) then return 'BELOW_FINALIZED' end
end
local id = redis.call('XADD', KEYS[2], 'MINID', '~', ARGV[2], '*', unpack(ARGV, 8))
if ARGV[4] ~= '' then
  redis.call('ZADD', KEYS[3], ARGV[4], ARGV[5])
  redis.call('HSET', KEYS[4], ARGV[5], ARGV[6])
end
if ARGV[3] == 'revert' then
  redis.call('ZREM', KEYS[3], ARGV[5])
  redis.call('HDEL', KEYS[4], ARGV[5])
end
-- Dedup state is written LAST: Redis does not roll back a failed script, so a failure above
-- leaves dedup unset and the retry re-emits (a duplicate, allowed), never a silent loss. (r6, B)
redis.call('SET', KEYS[1], ARGV[3], 'EX', ARGV[1])
return id
```
- **Replies:** stream id = written. `DUP` = duplicate (counted, not an error). `FENCED` → §4.8. `BELOW_FINALIZED` → CRITICAL `SinkInvariant`; the item is dropped. That is correct, because the final sweep has already covered the block and a canonical log there was emitted as late. Normal operation never produces it: tracked emits always lie above `C_f`.
- **Last-type state machine (N1):** `log, log` → 1 entry; `log, revert, log, revert` → 4 entries; a revert with no prior log, or a second revert, is a no-op. This is exactly I6.
- **Atomicity:** in a script that does not error, the stream entry, the `released` record and its before image are written together. A revert removes both.
- `packed_fields` is a JSON array of the field list.
- **Retention:** `MINID ~ (now_ms − retention_ms)`, with `minid_ms` taken from the listener clock. **(r6, P6)** Entries a consumer has not read within 48 h are trimmed. This is a hard read deadline in the consumer contract (README §5).

**`ckpt.lua` (r6)**, KEYS: 1 ckpt, 2 leader. ARGV: 1 token, 2 mode, …
- `advance <field> <v>`: `HSET field v` only if `v > HGET field`, else `NOOP`. The I2 check runs **before** the write (Redis does not roll back): a result with `finalized > safe`, or a `finalized` with no `safe` yet, is an error reply and nothing is written (a bug, never retried silently). *(4c, implemented)*
- `init <c0> <contracts_json>` **(r6, E)**: only if `EXISTS ckpt == 0`, writes `safe=c0, finalized=c0, contracts` in one call. Otherwise returns `EXISTS`. `Sink::init_checkpoint` is a void enqueue (4a, implemented): under a single fenced leader `EXISTS` cannot happen after a successful `load_checkpoint` returned empty, so `RedisSink` treats `EXISTS` as a logged no-op (the fence, not the driver, resolves any race; a deposed leader gets `FENCED`). This closes the Phase 3 "partial checkpoint" gap.
- `contracts <json>`: plain `HSET contracts`.

**`final_trim.lua` (r6)**, fenced: `ids = ZRANGEBYSCORE released -inf G_f` → `HDEL released_data ids…` (chunks ≤ 1000) → `ZREMRANGEBYSCORE released -inf G_f`. Returns the trimmed count as a string. The stream is never touched.

**`released_range(from, to)`** = `ZRANGEBYSCORE` + `HMGET released_data`. A member without data is a contract violation → explicit error.

**`lease.lua` (r6, P3)**, KEYS: 1 leader, 2 epoch.
- `acquire <host> <nonce> <ttl>`: if `EXISTS leader` → nil. Else `e = GET epoch`, `n = 1 + (number prefix of e, or 0)`, `token = n|host|nonce`, `SET epoch token`, `SET leader token PX ttl`, return `token .. '\n' .. (e or '')` (the new token on line 1, the previous epoch token on line 2 so `Leader Changed` is detected atomically).
- `renew <token> <ttl>`: if `GET leader == token` → `PEXPIRE`, `OK`. If leader is absent **and** `GET epoch == token` → `SET leader token PX ttl`, `RESUMED` (nobody took over during a blip). Otherwise `FENCED`.
- `release <token>`: if `GET leader == token` → `DEL`, `OK`; otherwise `NOOP`.

**Script plumbing (4c, implemented):** [lua_scripts.cpp](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/lua_scripts.cpp) holds the sources and typed call builders; [script_runner.cpp](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/script_runner.cpp) does `SCRIPT LOAD` on construction and `EVALSHA`; a `NOSCRIPT` reply reloads all scripts and retries **once**, any other error propagates (`OOM` → `sw::redis::OomError`). Every reply is a string or nil. Keys come from [keys.hpp](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/keys.hpp), which rejects a shard id containing braces.

## 4.3 Startup Redis checks (r6, P2)
| Check | On failure |
|---|---|
| Connect + `PING` (**IPv6**: `redis.railway.internal` is AAAA only) | **wait in `Init`** with full-jitter backoff (base 250 ms, cap 30 s). `/healthz` 200, `/readyz` 503, `RedisUnreachable` alert after 60 s. **No exit**, so no Railway crash loop. Log includes the resolved address family |
| `AUTH` rejected | exit 2 |
| `INFO server` → `redis_version ≥ 7.0` **(r6, B: shebang flags)** | exit 2 |
| `CONFIG GET maxmemory-policy` = `noeviction` | exit 2 if `require_noeviction`. If CONFIG is not permitted → WARN + `RedisMisconfigured` |
| `CONFIG GET appendonly` | WARN + alert if `no` |

Only conditions a restart cannot fix exit 2. **Railway smoke (Phase 6):** `--check-redis` confirms hiredis reaches the IPv6 private network. If it fails, the fallback is `getaddrinfo(AF_UNSPEC)` ourselves and passing the literal address.

## 4.4 Sink thread (`sink/redis_sink`), r6
- **Transport seam:** `RedisSink` drives a small `RedisConn` interface (pipeline send / replies). Production uses `sw::redis::Redis` (pool 1, connect/socket timeouts 2 s). Unit tests use a fake that injects errors and timeouts at chosen positions, so P1/A are proven without a real Redis. **TLS (`rediss://`) is not supported yet:** redis++ 1.3.15 `Uri` rejects the scheme, so it fails fast at startup with "invalid URI: invalid scheme". Railway's private network URL is plain `redis://`; if a TLS endpoint is ever needed, map `rediss` to `redis` and set `ConnectionOptions::tls.enabled` (the conan build already has TLS).
- **Queue:** bounded MPSC of `SinkItem{seq, kind}`. `kind ∈ {Emit, Ckpt, CkptInit, Contracts, FinalTrim, ReadCheckpoint, ReadReleased}`. Lease and heartbeat are **not** queue items (see below). **(r6, P4)**
- **Batching with barriers (r6, P1):**
  - Consecutive `Emit` items are pipelined (≤ 256 items or 5 ms).
  - Every other kind is a **barrier**. It ends the current batch and is sent alone, only after every earlier item has a successful reply.
  - On an error or timeout, every item from the **first** non-success reply onward is treated as unacked and re-sent **in order**. Items that already landed collapse to `DUP` or, at worst, re-emit an alternating `log/revert` pair (I6 still holds; consumers are idempotent on `event_id`).
  - A checkpoint can therefore never be applied ahead of an event it covers.
- **Reads (r6, A):** `ReadCheckpoint` / `ReadReleased` are barriers, so a read observes every write enqueued before it. While the sink is `Degraded` or `Replaying`, reads resolve to `std::nullopt` immediately. Results go back to the io thread with `asio::post`.
- **Modes:** `Connecting → Connected → Degraded (spill) → Replaying → Connected`, plus terminal `Fenced` (§4.8). While the spill is non-empty, new items go to the spill, never around it. Replay batches are capped at 50 ms wall time so the lease connection gets serviced between them.
- **Lease + heartbeat (r6, P4)** run on a **second connection** serviced by the same thread between batches. Renew every `renew_ms` (5 s), heartbeat every 10 s. Neither is spilled; both are meaningless once stale. A failed renew is reported to the io thread.
- **`saturated()`** reads a `std::atomic<bool>` written by the sink thread. **(r6, G)**
- The sink loop updates an atomic heartbeat timestamp each iteration for the process watchdog.

## 4.5 Spill buffer (`sink/spill`), process-scoped (r6, D1; replaces the rev 5 WAL)
- `spill_dir` on **ephemeral** container disk (default `/tmp/evml-spill`). **Wiped at process start**: only files named `seg-*.spill` are deleted, never anything else in the directory, so a mis-set `spill_dir` cannot destroy unrelated data. *(4b, implemented: [spill.cpp](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/spill.cpp))*
- 64 MiB segments. Records `[u32 len][u32 crc32c][u8 kind][payload]`. The CRC is kept to catch bugs: a mismatch is an explicit error, not a skip.
- No `fdatasync`, torn-tail handling or ack-cursor file. A fully acked segment is deleted.
- API: `append` never refuses (an accepted item is never dropped; `saturated()` at `spill_max_bytes` is the stop signal), `peek`/`pop` FIFO, `discard_all()` returns the dropped count for `evml_spill_discarded_records_total`. Single owner (the sink thread), no locking.
- **Replay scope (r6, Q1 ✅):** the spill is replayed only by the process that wrote it, after a Redis outage, in **both** lease modes. With `[lease] enabled=false` (local only) the fence is skipped (token `"0"`), so replay can never be refused. At process start the spill is wiped in both modes, so a single local host behaves exactly like production after a restart (re-sweep from the Redis checkpoint).
- **Why a crash loses nothing:** `ckpt` items reach Redis only behind the events they cover (P1). After a restart, recovery reads the checkpoint from Redis and re-sweeps everything above it. Anything that was only in the spill was never visible to consumers, so a reorged log among it needs no revert.

## 4.6 Backpressure (the "寧可 stall, 不丟" rule)
Spill ≥ `spill_max_bytes` → `saturated` = true (a full queue only spills to disk; it does not saturate). Release stops, C freezes, the sweeps return `Paused`, and the `SinkSaturated` alert fires. The process does **not** crash. When the sink drains, the pipeline resumes from C with no gap.

## 4.7 Heartbeat (liveness for sparse periods)
Every 10 s, on the lease connection: `SET hb:<host> {ts, role, state, head, G_s, G_f, C_s, C_f, active_provider, epoch_n, version} EX 30`. Downstream can tell "quiet market" from "listener dead". The external dead-man's switch is healthchecks.io (Phase 5 §5.3.2).

## 4.8 Lease, fencing and the Redis-outage lifecycle (r6, P3/D)
| Situation | Behaviour |
|---|---|
| Startup | connect (§4.3) → checks → `acquire` loop as standby (no sweeps, no writes). On a token: Recovering |
| Leader, Redis unreachable | keep sweeping; items go to the spill; reads → nullopt, so the final sweep and reverify pause; the safe sweep continues until `saturated` |
| Reconnect | first op is `renew` on the lease connection. `OK`/`RESUMED` → replay the spill → Connected. `FENCED` → **Fenced** |
| Any write script returns `FENCED` | the sink pauses and calls `renew`. `OK`/`RESUMED` → re-send from that item. `FENCED` → **Fenced** |
| **Fenced** | discard queue + spill (`evml_spill_discarded_records_total`, `SinkFenced` warning), stop releasing, report demotion to the io thread → standby (Phase 5 §5.5) |
| Draining (SIGTERM) | stop releasing, flush within the drain budget, `release` the lease. If the flush can't finish, exit anyway: the next leader re-sweeps |
| Lease disabled (`[lease] enabled=false`, local only; r6, Q1 ✅) | no `acquire`/`renew`; token `"0"` skips the fence in every script; `FENCED` can't occur, so an outage always ends in spill replay. Startup logs a WARN, and `/readyz` JSON shows `"lease":"disabled"`. **Never** run two hosts in this mode: nothing stops them both writing |

The standby never writes, because there is no spill to replay. A split-brain of any length is harmless: only the token in `leader` can write.

## 4.9 Phase 3 interface changes (r6, C/E/F), call sites audited
| Change | Call sites |
|---|---|
| `load_checkpoint()` → `awaitable<std::optional<Checkpoint>>`; nullopt keeps `recover()` in Recovering with `idle_poll` | [pipeline_driver.cpp:70](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/pipeline/pipeline_driver.cpp#L70); [sweep_fakes.hpp:73](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/tests/sweep_fakes.hpp#L73) |
| `released_range()` → `awaitable<std::optional<std::vector<ReleasedEntry>>>`; nullopt → `StepStatus::Paused` *before* any emit for that page | [sweeper.cpp:233](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/pipeline/sweeper.cpp#L233), [:288](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/pipeline/sweeper.cpp#L288); [sweep_fakes.hpp:74](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/tests/sweep_fakes.hpp#L74) |
| New `init_checkpoint(c0, manifest)` replaces three enqueues | [pipeline_driver.cpp:76-80](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/pipeline/pipeline_driver.cpp#L76-L80); `MemorySink`; `RedisLikeSink` (property test) inherits it |
| `recover()` queues one `reverify_released()` when a checkpoint existed (F) | `pipeline_driver.cpp`; driver tests that count reverify runs |
| `MemorySink` gains an `unreadable` toggle so Phase 3 tests cover the nullopt paths | `sweep_fakes.hpp` |

**Rewind note (r6, H):** `Sweeper::rewind_safe(F)` lowers `C_s` in memory only, while stored `ckpt.safe` stays higher (set-if-greater). This is safe because reverify has already corrected `(C_f, old C_s]` and `released` reflects it. After a crash the higher value is resumed, and the rewind's only effect was a redundant re-sweep.

## 4.10 Consumer-group lag probe (r6, Q2 ✅; alert rules in Phase 5 §5.3.3)
**Why:** `MINID` retention (48 h) trims entries whether or not a consumer has read them (P6). The listener can't see consumers, so without this probe a stalled downstream bot loses data silently.

- **Config:** consumer groups are created by the consumers, so the listener is told their names: `[sink] consumer_groups` (list of `{stream, group}`). An empty list disables the probe: INFO at startup, `/readyz` JSON `"consumer_lag":"disabled"`.
- **Who / when:** leader only, every 60 s, on the lease connection (read-only, never queued or spilled). Skipped while Redis is unreachable.
- **Per configured group:**
  | Measure | Source | Meaning |
  |---|---|---|
  | `undelivered_age_s` | `TIME` − ms of the first entry after the group's `last-delivered-id` (`XINFO GROUPS` → `XRANGE stream (<id> + COUNT 1`); 0 if none | how long the oldest unread entry has waited |
  | `pending_age_s` | `TIME` − ms of the smallest id in `XPENDING stream group` | delivered but not acked (also trimmed by `MINID`) |
  | `lag` | `XINFO GROUPS` `lag` (Redis ≥ 7.0); nil only when Redis cannot compute it (it stays valid after a `MINID` trim, verified on 8.4.7) | entry count, for the metric only. Alerts use ages, because `lag` can be nil |
  | `trimmed_unread` | `XINFO STREAM` `max-deleted-entry-id` > group `last-delivered-id`, **or** smallest pending id < `first-entry` id | entries were trimmed before this group read or acked them: **data loss already happened** |
  | group missing | `XINFO GROUPS` has no such name | consumer never started, or the name is misconfigured |
- Ages use Redis `TIME`, not the listener clock: stream ids are Redis arrival time.
- **Output:** a `ConsumerLag{stream, group, undelivered_age_s, pending_age_s, lag, trimmed_unread, missing, probed_at_ms}` snapshot in the sink status. `probed_at_ms` is the listener wall clock of the probe, so Phase 5 can treat an old snapshot (probe failing, Redis down) as unknown instead of trusting frozen ages. Phase 5 turns it into alerts and metrics. The probe itself never changes data and never blocks the sink: any error, including one that is not a connection error, is counted and logged, never fatal to the writer.
- **Validation (config load):** `0 < consumer_lag_warn_s < consumer_lag_crit_s < retention_ms / 1000`, `stream ∈ {events, registry}`, group names non-empty and unique per stream. A violation exits 2.

## Config changes (r6)
`[sink]`: `wal_dir` → `spill_dir` (default `/tmp/evml-spill`), `wal_max_bytes` → `spill_max_bytes`.

`[sink]` additions (r6, Q2):
```toml
consumer_groups     = []      # e.g. [{ stream = "events", group = "strategy_bot" }]; empty = probe disabled
consumer_lag_warn_s = 43200   # 12 h = 25% of the 48 h retention
consumer_lag_crit_s = 86400   # 24 h = 50%: half the safety margin is gone
```
**Resolved (4e):** the downstream bot's group is `strat:aero_eurcusdc:botB_v1` on the `events` stream (stream confirmed by the user). It is configured in `base.toml` `[sink] consumer_groups`, so the probe is enabled.

Call sites for both: [config.hpp:94-95](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/core/config.hpp#L94-L95), [config.cpp:471-479](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/core/config.cpp#L471-L479), [base.toml:136-137](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/config/base.toml#L136-L137), `config_test.cpp`.

## Tests
**Unit** (no I/O):
- spill: round-trip, segment GC, CRC mismatch → explicit error, wiped on start, cap → `saturated`;
- MINID computation; `INFO` parsing: 6.2 rejected, 7.0 and 8.4 accepted;
- `RedisSink` over the fake `RedisConn`:
  - a barrier is never sent while an earlier item is unacked (P1);
  - error/timeout at every position in a batch → suffix re-sent in order, final stream satisfies I6;
  - a read after queued emits sees them; reads → nullopt while Degraded (A);
  - `FENCED` → renew → `RESUMED` continues, `FENCED` → queue and spill discarded;
- 4a: Sweeper returns `Paused` with zero emits when `released_range` is nullopt; Recovering retries on a nullopt checkpoint; `init_checkpoint` on first run; reverify queued after recover.
- Config (Q2): `consumer_groups` parsing; warn ≥ crit, crit ≥ retention, unknown stream or duplicate group → error; empty list → disabled.

**Integration** (compose `redis:8.4`; each test uses its own shard id `t_<random>` so keys are isolated, and deletes them at the end):
- `REDIS_URL` is read from the **process environment only** (set by compose), never from a `.env` file: these tests write keys and change `maxmemory`, so they must never be pointed at a real deployment by accident.
- Tests that change server-wide state (`CONFIG SET maxmemory`, `SCRIPT FLUSH`) are tagged `[redis_serial]` and registered with ctest `RUN_SERIAL`; `maxmemory` is restored by an RAII guard even when an assertion fails.
1. Dedup state machine: same log twice → 1 entry; `log, revert, log, revert` → 4; `log, revert, revert` → 2; revert with no prior log → 0; a revert removes the id from `released` and `released_data`.
2. Toxiproxy: disable the Redis link for 60 s during 10k events → all land, in order, no duplicates.
3. Kill the process (driver + sink + MockProvider) while Degraded → restart → spill wiped → re-sweep → stream equals the canonical set exactly.
4. Spill cap → `saturated`, C frozen, no loss after recovery.
5. Neither ckpt field ever moves backwards; `advance` producing `finalized > safe` errors.
6. `FLUSHALL` → re-backfill from `contract_start` → stream repopulated (runbook step); an old token never resumes.
7. Retention: entries older than a small fake `retention_ms` are trimmed. *(4c: `MINID ~` trims whole stream nodes, so the test writes 300 entries and asserts that old entries went and the newest stayed, not an exact count.)*
8. Startup: `CONFIG SET maxmemory-policy allkeys-lru` → exit 2 (restore afterwards); proxy disabled → process waits in `Init`, no exit.
9. Lease: two sinks → exactly one leader; kill the leader → the other acquires within ttl + renew.
10. `emit` with a `released` score → stream entry, zset member and `released_data` all present; duplicate → nothing changes; `final_trim` removes only members ≤ G_f from both; `released_range` after reconnect returns full fields.
11. `ckpt.contracts` round-trip; `init` when `ckpt` exists → `EXISTS`.
12. **P1:** `SCRIPT FLUSH` between batches (forces `NOSCRIPT` mid-pipeline) and toxiproxy `reset_peer` mid-batch → `ckpt` is never ahead of a missing event.
13. **A:** emits queued, then `released_range` → contains them; link disabled → nullopt.
14. **B (verify the shebang claim empirically):** set `maxmemory` just above current usage → `emit` is refused with OOM, no key changed (dedup unset, no stream entry); after freeing memory the retry lands once. *(4c: ✅ verified on Redis 8.4.7 with `maxmemory 1`.)*
15. **Fencing:** write with an old token → `FENCED`, nothing written; leader expired with `epoch == token` → `RESUMED`; another acquisition in between → `FENCED`.
16. **P4:** replay 50k spilled items while renewing; the lease never expires.
17. `BELOW_FINALIZED`: a tracked emit at a block ≤ `ckpt.finalized` → rejected, nothing written.
18. **Lease disabled (Q1):** token `"0"`, outage via toxiproxy → spill replayed, exact set; restart → spill wiped, re-sweep, exact set.
19. **Consumer-lag probe (Q2):** create a group, emit, read part with `XREADGROUP`, leave some unacked → `undelivered_age_s` and `pending_age_s` match the entry ids; trim past the group's cursor with a small fake retention → `trimmed_unread` = true; an unknown group name → `missing`; empty config → probe never runs.

## Acceptance
Unit + scenarios 1–19 pass against `redis:8.4` in the dev container with `EVML_SANITIZE=ON`. In scenarios 2 and 3 the stream content equals the expected set exactly.

## Implementation slices (each gate = `ctest` exit 0 in the dev container)
| Step | Scope | Gate | Status |
|---|---|---|---|
| 0 | Baseline: fresh build + `ctest` (Debug = ASan/UBSan) | 241 green; closes the 3e sanitizer gap | ✅ 241/241 at `7036249` |
| 4a | §4.9 interface changes; no Redis code | existing + new 4a unit tests | ✅ 247/247 |
| 4b | Spill buffer + config rename | spill unit tests | ✅ 261/261, committed `bc9be61`, `ecde686` |
| 4c | Lua scripts (embedded header), script loader, lease scripts | integration 1, 5, 7, 10, 11, 14, 15, 17 | ✅ 274/274 (+ NOSCRIPT reload test) |
| 4d | `RedisSink` + `RedisConn` seam; **add toxiproxy to compose** | RedisSink unit tests; integration 2, 4, 12, 13, 16 | ✅ 315/315 via `make test` (uncommitted) |
| 4e | Startup checks, lease lifecycle incl. lease-disabled mode (§4.8), consumer-lag probe + config (§4.10), crash/runbook scenarios | integration 3, 6, 8, 9, 18, 19; then the full suite | ✅ 359/359 via `make test` (uncommitted) |

**Decided at 4d:** `Sink::readable()` was added (pure virtual); `final_step` and `reverify_released` return Paused without fetching when it is false, so a Redis outage no longer causes `getLogs` churn.

**4d results and deviations from this plan:**
1. `released_data` stores `{"stream":...,"fields":[[k,v],...]}`, not a bare field list: the stream is not derivable from the fields and a revert must go to the stream of the log it retracts.
2. `Sink::readable()` added (above).
3. `queue_capacity` is a soft cap: `saturated()` is true once the queue holds that many items, but the queue still accepts (it never drops). Normally the sink thread drains it to the spill first, so it only triggers if the sink thread is wedged or slow. **Approved by the user at 4e.**
4. Heartbeat (`hb:<host>`) writes move to 4e (their content comes from the io thread).
5. The acquire loop, startup checks, drain/`release` and lease-disabled config wiring stay in 4e; in 4d the sink takes the token in `RedisSinkOptions`.
6. Production connection: `RedisppConn` (2 s connect/socket timeouts; redis++ defaults to waiting forever). Tests reach Redis through a per-test toxiproxy proxy on a random port (`tests/toxiproxy.hpp`); `REDIS_URL` and `TOXIPROXY_URL` come from the process env only and a missing one fails the test.
7. Scenario 2 runs a real 60 s outage (about 62 s); scenario 12 is `[redis_serial]` because it runs `SCRIPT FLUSH`.

**4e results and deviations from this plan:**
1. Startup checks: `src/sink/redis_startup.{hpp,cpp}` (`evaluate_startup` is pure; `await_redis_ready` waits with full-jitter backoff, AUTH rejection is fatal). Lease: `src/sink/lease.{hpp,cpp}` (`try_acquire_lease`, `release_lease`, `acquire_lease_blocking`, `lease_disabled_token`). Sink: heartbeat (`set_heartbeat`), `drain(budget)`, `consumer_lag()` snapshot. Lag judgement is pure: `src/sink/consumer_lag.{hpp,cpp}`.
2. `RedisSinkOptions::from_config` copies `consumer_groups` but does **not** wire `host_id` / `heartbeat_ms`; the Phase 5 host sets them (from `[shard].host_id`, `[ops].heartbeat_s`). `validated()` rejects `heartbeat_ms > 0` with an empty `host_id`.
3. The lag probe is skipped when the lease is disabled (no leader election, as §4.10 says) and while Degraded. A `ConnError` is counted (`consumer_lag_failures_total`) and the last snapshot stays.
4. **Plan §4.10 corrected, verified on Redis 8.4.7:** `XTRIM MINID` does **not** update `max-deleted-entry-id` (stays `0-0`), and `XINFO GROUPS lag` stays valid after a trim (it is not nil). Rule 1 therefore only catches `XDEL`. `trimmed_unread` now has a third rule: `entries-added (XINFO STREAM) > entries-read + lag` (a group at last-delivered `0-0` counts as having read 0, because Redis reports `entries-read` nil for it). It is not flagged when Redis does not know `entries-read`.
5. Scenarios 3 and 6 run at library level (real driver, sweeper, `RedisSink`, and a scripted `FakeSource` instead of a mock provider; the canonical set is computed by the same chain on a `MemorySink`). Scenario 6's runbook step (new lease + checkpoint re-initialised at `contract_start - 1`) is driven through library calls; the `--backfill-contract` CLI that performs it is Phase 5. Scenario 6 is `[redis_serial]` (`FLUSHALL`). `main.cpp` is still a stub, so no process was killed: "crash" means destroying the sink, driver and sweeper while the spill is non-empty and building new ones on the same `spill_dir`.
6. Scenario 9: the leader that stops renewing is modelled by a standby race without any sink behind the winner (the key expires after the TTL); takeover measured at TTL + poll + slack. Scenario 18 runs with no lease at all and asserts that no `leader` / `epoch` key is ever created.
7. **Differences from §4.4 / §4.8 found in the 4e review (behaviour accepted, plan text was out of date):**
   - No 5 ms batching window: a batch is whatever is queued, up to `max_batch` (256). Lower latency, same safety (barriers are unchanged).
   - No 50 ms replay cap: one batch per `pump()`, with the lease serviced before each batch. Scenario 16 shows the lease never expires during a 50k replay.
   - Reads are delivered through an asio `concurrent_channel`, not `asio::post`.
   - There is no `Connecting` mode: the sink is built after the startup checks and the lease acquire, so it starts `Connected`.
   - A failed renew or a demotion is not pushed to the io thread: the host reads `status()` (`renew_failures_total`, `mode == Fenced`). Phase 5 polls it.
8. **Review fixes (4e):** the heartbeat and the lag probe now treat any error as non-fatal to the writer (counted, logged at ERROR); `ConsumerLag.probed_at_ms` added; `heartbeat_ttl_s` must exceed `heartbeat_ms`; MINID unit-tested; `CONFIG GET` retries `LOADING`/`BUSY` instead of reporting "denied"; `is_auth_error` matches only real AUTH rejections; `TOXIPROXY_URL` documented in `.env.example`.

### How to run
- **Steps 0–4c:** no rebuild. In the dev container terminal (`REDIS_URL=redis://redis:6379/0` is already set by compose):
  ```bash
  cmake --build --preset conan-debug -j && ctest --preset conan-debug --output-on-failure
  ```
- **Step 4d** adds to [docker-compose.yml](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/.devcontainer/docker-compose.yml) a `toxiproxy` service (`ghcr.io/shopify/toxiproxy`, pin the current 2.x tag at implementation time) and `TOXIPROXY_URL: http://toxiproxy:8474` for `dev`. Then run **"Dev Containers: Rebuild Container"** once. The dev image is unchanged; this only recreates the containers.
- Integration tests **fail** (they do not skip) when Redis or toxiproxy is unreachable.

## Accepted risks (r6, J)
- Single Railway Redis node (W5).
- Redis AOF `everysec` can lose ≈ 1 s of acked writes on a Redis crash. The loss is a prefix (checkpoint included), so the listener re-emits and consumers dedup. If those exact blocks are **also** reorged before the re-sweep, a consumer that read the lost entry keeps a log that is never reverted. This needs a Redis crash and a safe reorg in the same second; documented, not mitigated.
- Dedup keys expire after 7 d (r6, P5). Re-running a backfill after that re-emits; consumers dedup on `event_id`.

## Rev 6 changelog
P1 barriers (§4.4) · A read-your-writes (§4.4, §4.9) · B dedup-last write order, shebang, Redis ≥ 7.0 (§4.2, §4.3) · P3/D/P7 epoch fencing + Fenced lifecycle (§4.1, §4.2, §4.8) · D1 process-scoped spill replaces the durable WAL (§4.5) · P2 Redis-unreachable wait (§4.3) · C/E/F Phase 3 interface (§4.9) · P4 separate lease connection, read items (§4.4) · G atomic saturation · P5/P6/H/J docs · I test infra (toxiproxy, isolated shard ids) · Q1 lease-disabled mode (§4.5, §4.8) · Q2 consumer-group lag probe (§4.10, config).
