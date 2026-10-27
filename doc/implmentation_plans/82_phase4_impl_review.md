# Phase 4 implementation review (slices 0, 4a–4e) vs plan 50 rev 6

**Verdict:** no data-loss or fencing bug found. The core write path matches the plan: barriers, suffix re-send, renew-first reconnect, Fenced discard and spill order. There are **2 medium issues**, both in the 4e lag probe and heartbeat, plus a few low items and undocumented deviations. Nothing has been edited.

## Findings

| # | Sev | Where | What is wrong (evidence) | Proposed fix |
|---|---|---|---|---|
| 1 | **Med** | [redis_sink.cpp:737](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/redis_sink.cpp#L737), [:757](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/redis_sink.cpp#L757), [redispp_conn.cpp:36](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/redispp_conn.cpp#L36) | **A read-only probe can kill the writer.** The lag probe and the heartbeat catch only `ConnError`. Any other error reply (`NOPERM`, `WRONGTYPE`) or a parse error reaches `pump()` → `fail()` → sink **Failed**, and all writes stop. This breaks the plan's rule that "the probe never blocks the sink" (§4.10) | Catch `std::exception` in these two services only. Count the failure, log it at ERROR (still visible, not swallowed) and keep the sink running. Add a unit test that injects a non-`ConnError` |
| 2 | **Med** | [consumer_lag.hpp:42-50](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/consumer_lag.hpp#L42-L50), comment at [redis_sink.cpp:738](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/redis_sink.cpp#L738) | **A stale snapshot can't be detected.** The comment says "its staleness is visible to the host", but `ConsumerLag` has no timestamp. Hypothetical: the bot stalls, then Redis is down for 2 h. The snapshot still says `undelivered_age_s = 1h` while the real age is 3 h, so the alert fires late | Add `probed_at_ms` (Redis `TIME`) to the snapshot. Phase 5 then treats an old snapshot as unknown |
| 3 | Low | [redispp_conn.cpp:14](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/redispp_conn.cpp#L14), [redis_startup.cpp:118](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/redis_startup.cpp#L118) | **`rediss://` is not supported**, although §4.4 says "TLS for rediss://". Verified in redis++ 1.3.15 `redis_uri.cpp:43-48`: only `tcp`, `redis` and `unix` are accepted; anything else throws "invalid scheme". It fails fast (StartupFatal), so nothing goes silent. Railway's private URL is `redis://`, so today there is no impact | (a) Remove the claim from the plan. (b) Map `rediss` → `redis` and set `opts.tls.enabled = true` (conan already builds with TLS) |
| 4 | Low | [redis_sink.cpp:567-568](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/redis_sink.cpp#L567-L568) | **The MINID calculation has no unit test** (required by plan §Tests). If `wall − retention` were wrong, it would silently over-trim or never trim | Add a unit test that checks the emit's ARGV[2] is `wall − retention_ms`, and is clamped to 0 when wall < retention |
| 5 | Low | [redis_sink.cpp:167-180](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/redis_sink.cpp#L167-L180) | **`heartbeat_ttl_s` is not validated.** With 0, redis++ sets no expiry, so the heartbeat key never dies and "listener dead" can't be detected. With a TTL ≤ the interval, the key flaps | In `validated()`, when the heartbeat is on, require `heartbeat_ttl_s * 1000 > heartbeat_ms` |
| 6 | Low | [redis_startup.cpp:48](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/redis_startup.cpp#L48) | `config_get` treats **every** error reply as "CONFIG denied", including transient `LOADING` / `BUSY`. The result is a misconfiguration warning instead of a retry | Rethrow transient replies so the wait loop retries |
| 7 | Low | [redis_startup.cpp:33-34](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/redis_startup.cpp#L33-L34) | `is_auth_error` matches any message containing `"AUTH"`, which is broad enough to turn an unrelated error into exit 2 | Match only `WRONGPASS`, `NOAUTH` and `ERR AUTH` prefixes |
| 8 | Low | `evm_listener/.env.example` | `TOXIPROXY_URL` is not documented (`REDIS_URL` is). Your rule asks for every new variable to be documented | Add `TOXIPROXY_URL=` with the comment "tests only, set by compose" |
| 9 | Nit | [consumer_lag.hpp:48](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/consumer_lag.hpp#L48), plan §4.10 `lag` row | The comment "nil after a trim" contradicts what we verified (deviation 4: `lag` stays valid after `XTRIM MINID`) | Correct the comment and the plan row |
| 10 | Nit | [redis_sink.hpp:78](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/evm_listener/src/sink/redis_sink.hpp#L78) | `discarded_total` is described as "dropped on Fenced / Failed". On Failed, the held batch and the spill are not counted, though they are still in `pending_writes`, so nothing is hidden | Fix the comment, or count them in `fail()` |

## Plan items implemented differently and not documented
Behaviour is fine, but plan 50 still describes the original design. Proposed: add these to the "4e deviations" list.
- **5 ms batching window (§4.4):** not implemented. A batch is whatever is queued, up to 256. This gives lower latency and the same safety.
- **50 ms replay cap (§4.4):** replaced by one batch per `pump()`, with the lease serviced before each batch. Scenario 16 shows the lease never expires during a 50k replay.
- **Reads via `asio::post` (§4.4):** replaced by an asio `concurrent_channel`.
- **`Connecting` mode (§4.4):** does not exist. The sink starts `Connected`, because the startup checks and acquire run before it is built.
- **Failed renew / demotion "reported to the io thread" (§4.4, §4.8):** the host reads them from `status()` (`renew_failures_total`, `mode == Fenced`). Nothing is pushed.

## Intended for Phase 5 (not 4e gaps)
`main.cpp` wiring · `RedisUnreachable` alert (fed by the `on_waiting` callback) · `RedisMisconfigured` alert (fed by `config_denied`) · `/readyz` `lease` and `consumer_lag` fields · heartbeat payload and `host_id` / `heartbeat_s` wiring · Fenced → standby re-acquire loop (it must build a new `RedisSink`) · alerts that use `consumer_lag_warn_s` / `_crit_s` · `--backfill-contract`, `--check-redis` (Phase 6 smoke for IPv6) · a real process kill for scenario 3.

## Verified correct
- **Lua vs §4.2:**
  - fence check first;
  - dedup `SET` written last;
  - `final_trim` deletes in chunks of 1000;
  - `init` returns `EXISTS`;
  - lease acquire/renew/release follow the state table.
- **Write path:**
  - barriers are sent alone;
  - after a failure, the suffix is re-sent from the first non-ack;
  - the held batch is sent before the spill, so order is kept;
  - new items go behind the spill while Replaying;
  - reads resolve to nullopt when abandoned.
- **Lease:** reconnect renews first; Fenced discards held + spill + queue with the right count; `drain` releases only when not Fenced.
- **Thread safety:** `lease_` is touched in `drain()` only after the join; lag and heartbeat state are behind mutexes; the `next_*` deadlines are used only by the sink thread.
- **Tests:**
  - all 19 integration scenarios exist;
  - the server-wide tests (FLUSHALL, maxmemory, maxmemory-policy, SCRIPT FLUSH) are `[redis_serial]` + `RUN_SERIAL`;
  - every plan unit test exists except MINID (#4).
