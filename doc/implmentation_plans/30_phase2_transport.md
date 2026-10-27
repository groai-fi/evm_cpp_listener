# Phase 2: Transport & Provider Pool (`evml::rpc`, `evml::provider`)

**Goal:** resilient connections to N providers on a single `io_context` thread, with no blocking calls.

## 2.1 `WsSession` (Boost.Beast, `shared_from_this`)
Lifecycle: `resolve → tcp connect (10 s) → TLS (SNI + host verify) → WS handshake (10 s) → eth_chainId check → subscribe`.
- Transport is selected by URL scheme: `wss://` (TLS) or `ws://` (plain, for mock tests only).
- Socket options: `TCP_NODELAY`, keepalive. On Linux also `TCP_KEEPIDLE=10`, `KEEPINTVL=5`, `KEEPCNT=3`, `TCP_USER_TIMEOUT=25s`, which detects half-open sockets.
- Beast `stream_base::timeout{handshake=10s, idle=20s, keep_alive_pings=true}`, `read_message_max = 16 MiB`.
- **Single-writer queue** (one `async_write` in flight). Bounded at 256 frames; overflow becomes `fail(Retry)`.
- **Request correlation:** `id → {method, deadline, callback}` map, swept every 1 s by a timer. A timed-out request becomes `fail(Retry)`.
- Subscriptions: `newHeads` (unless `heads_from=primary` and this is not the primary) and `logs(filter)`. Each subscription must be ACKed within `subscribe_timeout_ms`. Subscription ids are keyed **per session only**: QuickNode and Infura returned the *identical* newHeads id in the 2026-10-05 probe, so ids are not globally unique.
- Callbacks into the owner: `on_head(Head)`, `on_log(Log)`, `on_ready()`, `on_closed(FailKind, reason)`.
- `fail()` is idempotent. It cancels timers, closes the socket and never throws. All callbacks check `closed_`.

## 2.2 `HttpRpcClient` (Beast, async, keep-alive)
- One persistent TLS connection per provider, with reconnect on error. Requests are serialised (1 in flight); a small queue holds the rest.
- Per-request deadline (`request_timeout_ms`). Sends `Accept-Encoding: identity`, because Beast does not gunzip, and an explicit `User-Agent: evm-listener/<version>`. The public `mainnet.base.org` sits behind Cloudflare, which returned 403 "error code 1010" to a default Python UA while curl got through.
- Used for: `eth_getLogs`, `eth_getBlockByNumber(tag|n,false)`, `eth_blockNumber`, `eth_chainId`.

## 2.3 Error classification → `FailKind`
| Signal | Kind | Action |
|---|---|---|
| net/TLS/WS errors, timeouts, half-open, write overflow, JSON-RPC `-32603 "service temporarily unavailable"` (Infura, observed) | `Retry` | full-jitter backoff |
| HTTP 429 / JSON-RPC `-32005` / "exceeded … capacity" (per-second throttling) | `RateLimited` | `max(backoff, Retry-After or 10 s)` + jitter |
| `"Monthly capacity limit exceeded"` (Alchemy, **observed 2026-10-05**) or another monthly-quota message | `QuotaExhausted` | provider skipped for `quota_cooldown_s` (1 h), immediate failover, re-probe hourly with `eth_blockNumber`. Alert: **one `warning` per incident, no 15-min re-notify**, and `RESOLVED` when the re-probe succeeds. It escalates to `critical` only if no healthy provider is left in that chain |
| HTTP 401/403, "invalid project id / api key" | `AuthFatal` | provider disabled for 10 min, alert, re-probe |
| `eth_chainId` ≠ config | `ConfigFatal` | provider disabled permanently, CRITICAL log |
| getLogs "range too large / >10000 results / response size", HTTP 429 on a large range, or HTTP 413 / `-32614`/`-32615` `"eth_getLogs is limited to a N range"` (public Base N=500, QuickNode N=5, both **observed**) | `RangeTooLarge` | **not a connection failure**; the backfiller shrinks the range (Phase 3), parsing N from the message (matched by message, not by code) |

The error-string regexes for each provider live in one table with fixture tests, so a provider changing its message format breaks a test instead of production.

## 2.4 `Backoff` (full jitter)
`delay = uniform(0, min(cap, base · 2^attempt))`, base 250 ms, cap 30 s. The counter resets only after the session has been **Live for 30 s** (prevents flapping). RNG is a per-instance `std::mt19937_64` seeded from `random_device`.

## 2.5 `ProviderPool` (v1 mode = **failover**, as you chose)
- **Enabled iff the env var is set.** Today that gives QuickNode (WS+HTTP), Infura (HTTP; its WS joins once `BASE_WSS_URL` is added), Alchemy (HTTP, quota cooldown), Ankr (HTTP) and public Base (HTTP literal).
- **Two independent chains**, each walked in config order:
  - WS chain `ws_order`: QuickNode → Infura → Alchemy (if `BASE_WSS_URL_ALCHEMY` is set).
  - getLogs/tags chain `getlogs_order`: Infura → Alchemy → Ankr → public → QuickNode. It picks the first healthy provider that is **not the active WS provider**, and falls back to the WS provider only if nothing else is healthy (independent cross-check).
- **One active WS session**, on the first healthy entry of the WS chain. Failover to the next happens when any of these occur:
  - 3 consecutive session failures;
  - `QuotaExhausted` or `AuthFatal`;
  - the active provider is *stale* (2.6).
- **Anti-flap:** go back to the preferred provider only after it has been healthy for `prefer_primary_after_s` (10 min) on the standby probe. Back-to-back failovers are rate-limited to 1 per 60 s.
- If a chain has no healthy provider, the pool keeps cycling it with backoff, the pipeline stalls safely (C frozen), and a critical alert fires.
- `hedged` mode stays implemented behind config (N concurrent WS sessions merged by dedup) but is **off** in v1.
- Metrics: `ws_connected{p}`, `provider_active{p}`, `provider_failovers_total{from,to,reason}`, `ws_reconnects_total{p,reason}`, `rpc_errors_total{p,kind}`, `rpc_latency_seconds{p,method}`.

## 2.6 `HeadTracker` (HTTP, cost-aware)
- Active WS provider: the `newHeads` stream drives `head_block`. A 15 s stall fails the session.
- **Finality tags are polled on the current getLogs provider** (`safe` every 4 s, `finalized` every 12 s), *not* on the WS provider.
  - Why: a sweep must never request blocks beyond what the provider answering it has seen. A lagging node returns `[]` for unseen blocks, which looks exactly like "no events".
  - Ankr's `safe` trailed the others by ~45 blocks in today's sample.
  - Rule: `G_k` = that provider's own tag. On a getLogs failover, G is re-read from the new provider before the next sweep, and is never taken from the old one.
  - The returned `hash` of the safe block is kept, for the safe-reorg check (Phase 3 §3.3).
- Cross-check every 60 s: the WS provider's tags and head. A divergence of more than 1 L1 epoch (~200 blocks) logs WARN.
- **Stale rules:**
  - WS head behind the getLogs provider's head by more than `stale_after_blocks` (5) for 30 s → WS failover.
  - getLogs provider's tag `safe` behind the WS provider's `safe` by more than 30 blocks (≈ 60 s) for 2 min → getLogs failover. This bounds the latency cost of a lagging getLogs node.
- Leader only. The standby host (lease, Phase 5) runs no head tracking.

## Tests
- Unit: backoff bounds and distribution, reset rule; error classifier fixture table.
- Integration (label `integration`, in-process **MockProvider** = Beast WS+HTTP server on an ephemeral `127.0.0.1:0` port, scripted):
  1. Clean connect → subscribe ACK → heads flow.
  2. Server closes the socket mid-stream → reconnect within the backoff bound.
  3. Server stops responding (half-open) → detected within ≤ 20 s.
  4. 429 with `Retry-After: 3` → no reconnect before 3 s.
  5. 401 → provider disabled; failover to the next provider.
  6. Wrong chainId → ConfigFatal.
  7. Subscription ACK never arrives → subscribe timeout.
  8. Active provider's heads 20 blocks behind the standby probe → failover within 30 s + ProviderFailover alert.
  9. Active returns `Monthly capacity limit exceeded` → immediate failover, provider cooled down for 1 h (fake clock).
  10. Preferred provider recovers → switch back only after 10 min healthy (fake clock); no flapping under alternating faults.
  11. All providers down → pipeline stalls with C frozen, page alert fires, recovery resumes with no gap.
  12. getLogs returns HTTP 413 `-32615 …limited to a 5 range` → classified `RangeTooLarge`, page size clamps to 5, no reconnect.
  13. getLogs provider lags 50 blocks behind the WS provider → sweeps stop at the lagging provider's own tag (no empty-range miss), then getLogs failover after 2 min.
  14. Provider whose env var is unset → skipped; setting it and restarting adds it to its chain.
  15. HTTP server that returns 403 unless a `User-Agent` header is present → the client passes.

## Acceptance
Integration scenarios 1–15 pass under ASan, with no leaked sessions (shared_ptr count at shutdown = 0).

## Results and deviations (implemented)

**Status:** Phase 2 complete, uncommitted. 124/124 tests green under ASan/UBSan (91 unit, 33 integration). `tools/check_secrets.sh` OK. Live probe `--probe-transport 20` against real providers: exit 0.

**Delivered (all under `evm_listener/src`)**
| Part | Files |
|---|---|
| Backoff, classifier | `rpc/backoff`, `rpc/classify` |
| URL parser, async HTTP client | `rpc/url`, `rpc/http_rpc_client` |
| WS session | `rpc/ws_session` (+ `build_subscribe_logs` in `rpc/codec`) |
| Failover policy (pure) | `provider/provider_book` |
| Stale/lag detection (pure) | `provider/stale_detector` |
| I/O shell | `provider/provider_pool` (WS supervisor, `call()` failover, probes, tag polling, per-provider tag cache) |
| Config mapping, live probe | `app/pool_config`, `app/tools` (`--probe-transport <s>`) |

**Live probe result (Base, TLS):** QuickNode WS active, 10 heads in 20 s (matches 2 s blocks); getLogs OK via Infura; `safe` and `finalized` read via Infura; Alchemy's exhausted quota was detected by the standby probe and moved to Cooldown. 0 logs is expected (the 3-block window had no events on the tracked contracts).

**Deviations from the plan text**
1. `HeadTracker` is split: pure `StaleDetector` plus polling inside `ProviderPool` (no separate class).
2. No bounded single-writer queue in `WsSession`: requests are only sent during setup, so a single writer holds by construction. Add a queue if Phase 3 needs mid-stream requests.
3. `WsSession::stop()` hard-closes the socket (no WS close handshake).
4. HTTP timeout is per phase (connect/TLS/write/read), not per request.
5. Forced failovers are immediate; `min_voluntary_gap` applies only to voluntary switches (switch-back, stale demotion).
6. A stale demotion is refused when no other provider is usable (a lagging stream beats no stream).
7. A WS session counts as a success only after staying Ready for `backoff_min_live`; connect-then-die flaps therefore still reach `failover_after`.
8. `Policy`/`StaleThresholds` use whole seconds, so pool integration tests use 0 s/1 s values with real time.
9. A 429 on a large range is `RateLimited` (the classifier cannot see the request span). **Phase 3 backfiller must handle this.**
10. `no WS provider enabled` is rejected by `Config::parse` (ConfigError) before the pool is built.

**Known gaps**
- Pool integration tests use plain TCP mocks; TLS is covered only by the live probe (manual, needs network and credentials).
- `divergence()` is exposed but not yet logged/alerted (Phase 5).
- topic0s for PoolCreated/GaugeCreated/GaugeKilled/GaugeRevived are still not live-confirmed.
- Provider names appear in probe output; the QuickNode subdomain still shows in `redact()` output.
