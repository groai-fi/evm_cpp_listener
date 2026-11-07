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
- **Lua**: `make lint-lua` rules in `src/sink/lua/README.md` are mandatory (`#!lua` first line, Lua 5.1, sandbox globals only).
- **Target (not yet enforced — do not claim it is)**: `-Wshadow -Wsign-conversion`; release hardening (`-D_FORTIFY_SOURCE=3 -fstack-protector-strong`, `_GLIBCXX_ASSERTIONS`); libFuzzer targets for parsers of external bytes (JSON-RPC/WS frames, hex, TOML).
- Never `-ffast-math`.
- Specs in `openspec/specs/` (notably `cpp20-core-guidelines`, `fast-signal-stream`) are binding; this rule must not contradict them.

## A. Data Fidelity & Numeric Correctness
1. **Pass through, don't reinterpret.** Log `topics` and `data` are forwarded byte-exact as received (hex). Never decode amounts into `double`. If decoding is ever added, `int256`/`uint256` become exact integers (two's complement) or decimal strings.
2. **Lua numbers are IEEE doubles.** `tonumber` is allowed only for values proven `< 2^53` (block numbers, ms timestamps, counts). Any 256-bit value or wei amount is passed and compared as a string, never through `tonumber`.
3. **Unsigned arithmetic checks order first.** Block-range and lag math (`to - from`, `head - safe`, `from + page - 1`) on values from providers checks ordering/overflow before computing, or carries a comment proving the bound. A provider reporting a regressing head must yield a typed error or an explicit "behind" state, never a wrapped `uint64_t`.
4. **Hex quantities are parsed strictly.** Block numbers, indexes, and timestamps parsed from RPC reject malformed hex and values above the target type; never truncate or default.
5. **Units are explicit.** Durations and timestamps carry their unit (`_ms`, `_s`); block counts are not mixed with time.

## B. Fail-Closed Verification
6. **`chain_id` is verified** against every provider at startup and on reconnect; a mismatch is `ConfigFatal`, never a warning.
7. **No release without verification.** An event reaches the authoritative events stream only after cross-provider canonical block-hash verification and the configured finality gate (`safe` / `finalized`). Disagreement holds the block; it never picks a side silently.
8. **External data is adversarial.** `eth_getLogs` batches pass `validate_logs` (`removed == false`, block within the query range, valid topic0, tracked route, finality class match, no duplicates) before entering the pipeline. A failing batch is discarded whole and the provider is penalized; never clamp, skip single logs, or fill defaults.
9. **The fast stream is a signal, not a record.** `evml:{<shard>}:fast` data must never be written to `evml:{<shard>}:events`, advance a checkpoint, or count toward coverage (see `fast-signal-stream` spec).

## C. Delivery Guarantees & Recoverability
10. **Duplicate allowed, silent loss never.** Delivery is at-least-once with deterministic `event_id = {chain_id}:{blockHash}:{logIndex}` and Redis-side dedup. Any change that could drop an event on a failure path is a blocker.
11. **Commit markers are written last.** Redis does not roll back a failed script: in every Lua script, dedup keys, checkpoints, and other "done" markers are written after the data they guard.
12. **Every mutating Redis write is fenced.** Scripts check the leader token first and return `FENCED`; on `FENCED` the writer stops acting as leader. It does not retry as leader or fall back to unfenced writes.
13. **Checkpoint invariants are checked before writing.** `finalized <= safe`, checkpoints only move forward, and a checkpoint never passes a block whose events are not acked by Redis.
14. **Reorgs are retracted explicitly.** Every released log invalidated by a reorg gets a before-image `revert` event; nothing is ever emitted at or below the stored `finalized` checkpoint (`BELOW_FINALIZED`).
15. **Restart recovery = re-sweep from the Redis checkpoint.** The spill is process-scoped (no fsync, wiped at startup). Do not add a cross-restart replay path without fsync, torn-tail handling, and an ack cursor.

## D. Secrets
16. This service holds **no private keys** and must never gain signing capability. Secrets are RPC URLs (path/query tokens), `REDIS_URL` credentials, and alert/dead-man tokens.
17. Never put secrets into logs, `Error::message`, exception text, metrics labels, or snapshots; use the existing redaction helpers. See `no-hardcoded-secrets-in-scripts.md`.

## E. Errors, Observability & Time
18. Operational RPC, HTTP, decode, Redis, and lease paths return `core::Result<T, E>` and are `noexcept`; throwing wrappers exist only for startup/CLI (`acquire_lease` vs `try_acquire_lease`). Every `Result`-returning function is `[[nodiscard]]`. Never swallow errors; never return `std::optional<T>` where `nullopt` means failure (it drops the reason).
19. Retry/failover logic branches on `FailKind`, never on message strings.
20. A `switch` over an `enum class` lists every enumerator with no `default:`, so adding a variant fails compilation until every switch handles it.
21. Every emit, retract, checkpoint advance, lease acquire/lose, and `FENCED` / `DUP` / `BELOW_FINALIZED` outcome emits one structured log line: shard, `chain_id`, block, `event_id` (when applicable), epoch/host, and outcome.
22. `steady_clock` for timeouts, backoff, rate limits, and staleness; `system_clock` (UTC) only for `observed_ms` / `emitted_ms` and audit timestamps. Inject the clock in logic under test.

## F. C++ Core Hygiene
23. **RAII & Rule of Zero/Five (`R.1`, `C.20`, `C.21`)**: bind every resource to object lifetime. If any special member is declared, declare or `= delete` all five. No raw `new`/`delete`.
24. **Ownership (`R.20`–`R.23`)**: `std::unique_ptr` by default; `std::shared_ptr` only for async/coroutine lifetimes, with no ownership cycles (callbacks hold `std::weak_ptr`). Raw `T*` / `T&` are non-owning observers.
25. **Coroutines (`CP.51`, `CP.52`)**: never capture `this` or stack references across `co_await` unless the lifetime is held by a `shared_ptr`; coroutine state owns its `core::Config` copy; never hold a `std::mutex` across `co_await`.
26. **Thread ownership**: single-owner components (`Spill`, `PipelineDriver`) are touched only by their owning thread/strand; cross-thread hand-off goes through the sink's synchronized queue or atomics, never shared raw references.
27. **Types & casts (`Enum.3`, `ES.48`, `C.46`)**: `enum class` with explicit width (`: std::uint8_t`); no C-style casts; `explicit` single-argument constructors; narrowing of externally-sourced values goes through a checked cast.
28. **Views (`SL.str.2`, `Bounds.4`)**: `std::string_view` / `std::span<const T>` for read-only parameters; string-keyed maps use transparent hashing. Never store a view beyond the call; convert to `std::string` before C APIs that need NUL termination (`getenv`, OpenSSL, hiredis).
29. **Headers (`SF.7`, `SF.8`)**: `#pragma once`; no `using namespace` at file scope in headers.
30. **`const` & `noexcept`**: `const` locals and member functions by default (not top-level `const` on by-value parameters in declarations); `noexcept` only where the function genuinely cannot throw.
