---
trigger: always_on
---

# C++20 Production Rules — EVM Event Ingestion (Financial Data Integrity)

This service streams verified contract logs into Redis Streams that downstream strategies treat as the record of on-chain activity. It signs nothing. The money risk is **bad data**: a lost, duplicated, reorged-out, or wrong-chain event leads to a wrong position or PnL downstream.

## 0. Priority Order
When rules conflict: **no silent data loss > no un-retracted reorged data > fencing & checkpoint invariants > auditability > latency > style**.
A performance argument never overrides sections A–E without benchmark evidence and a code comment explaining the trade.

## Context
- **Standard**: C++20 (`cxx_std_20`, `CXX_EXTENSIONS OFF`); CMake target-scoped `target_*` for flags; `clang-format` + `clang-tidy` enforced.
- **Enforced warnings**: `-Wall -Wextra -Wpedantic -Wconversion -Wimplicit-fallthrough -Wformat=2 -Wnon-virtual-dtor -Woverloaded-virtual -Werror`.
- **Enforced sanitizers**: Debug builds pass ASan + UBSan; TSan (`EVML_TSAN`) for code touching the sink thread, startup thread, or executors.
- **Lua**: `make lint-lua` rules in `evm_listener/src/sink/lua/README.md` are mandatory (`#!lua` first line, Lua 5.1, sandbox globals only).
- **What tooling actually enforces**: `.clang-tidy` covers parts of rules 24 and 28 (special members, owning memory, no malloc, no C-style casts, `explicit` constructors, enum size). Sections A–E are enforced only by review and tests (`redis_scripts_test`, sweeper/sink tests) — do not claim a tool guarantees them.
- **Target (not yet enforced — do not claim it is)**: `-Wshadow -Wsign-conversion`; release hardening (`-D_FORTIFY_SOURCE=3 -fstack-protector-strong`, `_GLIBCXX_ASSERTIONS`); libFuzzer targets for parsers of external bytes (JSON-RPC/WS frames, hex, TOML).
- Never `-ffast-math`.
- Specs in `openspec/specs/` (notably `cpp20-core-guidelines`, `fast-signal-stream`, `sink-consumer-groups`) are binding; this rule must not contradict them.
- **Known gaps** between these rules and the current code are listed at the end. Never report a rule as satisfied where a known gap exists.

## A. Data Fidelity & Numeric Correctness
1. **Pass through, don't reinterpret.** Log `topics` and `data` are forwarded byte-exact as received (hex). Never decode amounts into `double`. If decoding is ever added, `int256`/`uint256` become exact integers (two's complement) or decimal strings.
2. **Lua numbers are IEEE doubles.** `tonumber` is allowed only for values proven `< 2^53` (block numbers, ms timestamps, counts). Any 256-bit value or wei amount is passed and compared as a string, never through `tonumber`.
3. **Unsigned arithmetic checks order first.** Block-range and lag math (`to - from`, `head - safe`, `from + page - 1`) on values from providers checks ordering/overflow before computing, or carries a comment proving the bound. A provider reporting a regressing head must yield a typed error or an explicit "behind" state, never a wrapped `uint64_t`.
4. **Hex quantities are parsed strictly.** Block numbers, indexes, and timestamps parsed from RPC reject malformed hex and values above the target type; never truncate or default.
5. **Units are explicit.** Durations and timestamps carry their unit (`_ms`, `_s`); block counts are not mixed with time.

## B. Fail-Closed Verification
6. **`chain_id` is verified; a mismatch is `ConfigFatal`, never a warning.** Today the running host verifies it on every WebSocket connect (`WsSession` phase 1, so every reconnect too). HTTP-only providers are verified only by `--check-config --online` (see Known gaps). Any new provider path must verify `chain_id` before its data is used.
7. **No release without verification.** An event reaches an authoritative stream (`evml:{<shard>}:events`, `evml:{<shard>}:registry`) only after canonical block-hash verification against a second source: the WebSocket head ring, or an HTTP provider that did not return the logs (the ring may be fed by the same vendor; see Known gaps), and after the configured finality gate (`safe` / `finalized`; `depth` is for Anvil tests only). Disagreement holds the block; it never picks a side silently.
8. **External data is adversarial.** `eth_getLogs` batches pass `validate_logs` (`removed == false`, block within the query range, valid topic0, tracked route, finality class match, no duplicates) before entering the pipeline. A failing batch is discarded whole and the provider is penalized; never clamp, skip single logs, or fill defaults.
9. **The fast stream is a signal, not a record.** `evml:{<shard>}:fast` data must never be written to an authoritative stream, advance a checkpoint, or count toward coverage. It is lossy by design (dropped and counted in `evml_fast_dropped_total` when the sink is degraded or saturated) and never retracted on reorg; rules 10 and 15 do not apply to it (see `fast-signal-stream` spec).

## C. Delivery Guarantees & Recoverability
10. **Duplicate allowed, silent loss never** (authoritative streams). Delivery is at-least-once with deterministic `event_id = {chain_id}:{blockHash}:{logIndex}` and Redis-side dedup. Redis dedup keys expire (`dedup_ttl_s`, 7 d in `base.toml`), so dedup is time-bounded; consumers must still dedup on `event_id`. Any change that could drop an event on a failure path is a blocker.
11. **Retention and eviction are data-loss paths.** `XADD MINID ~` trims entries whether or not consumers read them. Changes to `retention_ms`, `[sink].consumer_groups`, or `consumer_lag_warn_s` / `consumer_lag_crit_s` are data-loss-sensitive: lag alerts must fire well before retention (`sink-consumer-groups` spec). Never relax `require_noeviction`; an evicted dedup, checkpoint, or stream key is a silent loss or duplicate.
12. **Commit markers are written last.** Redis does not roll back a failed script: in every Lua script, dedup keys, checkpoints, and other "done" markers are written after the data they guard.
13. **Every mutating Redis write is fenced.** Scripts check the leader token first and return `FENCED`; on `FENCED` the writer stops acting as leader. It does not retry as leader or fall back to unfenced writes. Token `'0'` (`[lease] enabled = false`) skips the fence and is for single-instance tests only; staging and prod run with `enabled = true`.
14. **Checkpoint invariants are checked before writing.** `finalized <= safe`, persisted checkpoints only move forward (in-memory cursors such as `C_s` may rewind on a safe reorg; the persisted value does not), and a checkpoint never passes a block whose events are not acked by Redis.
15. **Reorgs are retracted explicitly.** Every released (safe-class) log invalidated by a reorg gets a before-image `revert` event. Nothing is emitted at or below the stored `finalized` checkpoint: `emit.lua` enforces this (`BELOW_FINALIZED`) only for emissions carrying a released block (safe-class logs); finalized-class logs and reverts rely on sweep cursor ordering in C++, so changes to cursor logic must preserve it.
16. **Restart recovery = re-sweep from the Redis checkpoint.** The spill is process-scoped (no fsync, wiped at startup). Do not add a cross-restart replay path without fsync, torn-tail handling, and an ack cursor.

## D. Secrets
17. This service holds **no private keys** and must never gain signing capability. Secrets are RPC URLs (path/query tokens), `REDIS_URL` credentials, and alert/dead-man tokens.
18. Never put secrets into logs, `Error::message`, exception text, metrics labels, or snapshots; use the existing redaction helpers. See `no-hardcoded-secrets-in-scripts.md`.

## E. Errors, Observability & Time
19. Operational RPC, HTTP, decode, Redis, and lease paths return `core::Result<T, E>` and are `noexcept`; throwing wrappers exist only for startup/CLI (`acquire_lease` vs `try_acquire_lease`). Every `Result`-returning function is `[[nodiscard]]`. Never swallow errors; never return `std::optional<T>` where `nullopt` means failure (it drops the reason). Recoverable failures (network, timeout, malformed provider data, transient Redis) are never thrown. Unrecoverable ones may throw: a violated programming contract (`std::logic_error` / `std::invalid_argument`), a `Fatal` Redis script verdict, or a spill I/O error — but only where a top-level catch turns them into a terminal, loud state (the sink pump catches and goes `Failed` with a CRITICAL log), never across a thread boundary.
20. Retry/failover logic branches on `FailKind`, never on message strings.
21. A `switch` over an `enum class` lists every enumerator with no `default:`, so adding a variant fails compilation until every switch handles it. No compiler flag enforces this (`default:` silences `-Wswitch`); reviewers must check it.
22. **Log state changes and anomalies; count per-event outcomes.** Lease acquire/lose, leadership term start/end, `FENCED`, `BELOW_FINALIZED`, gate signals (safe regression / hash mismatch, finalized anomaly), head forks, pipeline state changes, and sink mode changes emit one structured log line with the relevant identifiers (block, `event_id`, provider, host); reorgs and stalls also raise alerts. Per-event outcomes (emit, `DUP`, `revert`) are Prometheus counters (`evml_sink_dup_total`, `evml_reverts_emitted_total`), not log lines; the authoritative stream itself is the per-event audit record.
23. `steady_clock` for timeouts, backoff, rate limits, and staleness; `system_clock` (UTC) only for `observed_ms` / `emitted_ms`, Redis stream-ID math (`MINID` retention, since stream IDs are wall-clock ms), and audit/log timestamps. Inject the clock in logic under test.

## F. C++ Core Hygiene
24. **RAII & Rule of Zero/Five (`R.1`, `C.20`, `C.21`)**: bind every resource to object lifetime. If any special member is declared, declare or `= delete` all five. No raw `new`/`delete`.
25. **Ownership (`R.20`–`R.23`)**: `std::unique_ptr` by default; `std::shared_ptr` only for async/coroutine lifetimes, with no ownership cycles (callbacks hold `std::weak_ptr`). Raw `T*` / `T&` are non-owning observers.
26. **Coroutines (`CP.51`, `CP.52`)**: never capture `this` or stack references across `co_await` unless the lifetime is held by a `shared_ptr`; coroutine state owns its `core::Config` copy; never hold a `std::mutex` across `co_await`.
27. **Thread ownership**: single-owner components (`Spill`, `PipelineDriver`) are touched only by their owning thread/strand; cross-thread hand-off goes through the sink's synchronized queue or atomics, never shared raw references.
28. **Types & casts (`Enum.3`, `ES.48`, `C.46`)**: `enum class` with explicit width (`: std::uint8_t`); no C-style casts; `explicit` single-argument constructors; narrowing of externally-sourced values goes through a checked cast.
29. **Views (`SL.str.2`, `Bounds.4`)**: `std::string_view` / `std::span<const T>` for read-only parameters; string-keyed maps use transparent hashing. Never store a view beyond the call; convert to `std::string` before C APIs that need NUL termination (`getenv`, OpenSSL, hiredis).
30. **Headers (`SF.7`, `SF.8`)**: `#pragma once`; no `using namespace` at file scope in headers.
31. **`const` & `noexcept`**: `const` locals and member functions by default (not top-level `const` on by-value parameters in declarations); `noexcept` only where the function genuinely cannot throw.

## Known Gaps (code does not yet meet the rule)
Do not widen these; fix them when touching the code, and remove the entry when fixed.
- **Rule 6**: HTTP-only providers (`ankr`, `base_public`, and the HTTP side of WS providers) are not `eth_chainId`-checked by the running host; the prod image does not run `--check-config --online`. Rule 7's second-source hash check limits the damage (a wrong-chain provider is penalized and the sweep stalls instead of emitting).
- **Rule 7**: `PoolLogSource::block()` accepts a head-ring hash without checking which vendor fed the ring. `ws_order` and `getlogs_order` share `quicknode`, `infura`, and `alchemy`, so after a failover the ring and the logs can come from the same vendor (not truly independent).
- **Rule 19**: `pipeline::LogSource::block()` returns `std::optional<BlockInfo>` where `nullopt` means a read failure.
- **Rule 21**: `default:` in switches over project enums at `pipeline/pipeline_driver.cpp` (`StepStatus`), `provider/provider_pool.cpp` (`rpc::FailKind`), `provider/provider_book.cpp` (`State`).
