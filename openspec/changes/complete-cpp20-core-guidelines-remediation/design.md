# Design

## Context

Commits `106a12b` through `9fa909a` from conversation `42f96826-bb97-4b54-9697-dc7afd28c4d5` refactored `Host`, `ProviderPool`, `WsSession`, `HttpRpcClient`, `ops::HttpClient`, `Sweeper`, and `PipelineDriver` to use static coroutine frames and introduced `core::Result<T>` across RPC and Redis sink boundaries. However, a post-remediation audit against `.agent/rules/cpp-core-guidelines.md` revealed four remaining architectural gaps:
1. `evm_listener/src/app/tools.cpp` still creates `std::shared_ptr` ownership cycles in `probe_transport` (`st -> st->pool -> handlers_ -> st`), embeds `boost::asio::io_context` inside `ProbeState`, `BackfillState`, and `AuditFetchState` while passing `std::shared_ptr<State>` into coroutines spawned onto `st->ioc`, and holds a raw `sink::RedisSink&` in `BackfillState`.
2. `Sweeper::State` (`evm_listener/src/pipeline/sweeper.cpp`) and `PipelineDriver::State` (`evm_listener/src/pipeline/pipeline_driver.cpp`) still hold `const core::Config& cfg` as a raw reference inside a `std::shared_ptr<State>` across `co_await` suspension points, which risks dangling when callers pass coroutine-local `Config` copies (as `run_backfill_coro` does at `tools.cpp:492-516`).
3. `evm_listener/src/sink/lease.hpp` inverts the repo's `try_*` naming convention (`try_acquire_lease` throws `ConnError`, while `try_acquire_lease_result` returns `Result`), `RedisppConn::try_read_*` (`evm_listener/src/sink/redispp_conn.cpp`) still throws `std::runtime_error` from internal reply parsers (`parse_u64`, `parse_score`, `require_array`, `reply_str`, `reply_u64`, `flat_get`, `flat_str`), and operational helpers in `tools.cpp` (`rpc_call`, `code_at`, `check_config_online`, `audit_stream`) still rely on exceptions (`RpcFailure`, `TransportError`, `DecodeError`, `std::stoull`).
4. Several callback signatures and helpers (`PoolHandlers`, `StartupOptions::on_waiting`, `AcquireOptions::on_waiting`, `AlertEngine::unchanged_for`/`sustained`, and helpers in `coverage.cpp`, `log_validation.cpp`, `tools.cpp`, `config.cpp`, `host_telemetry.cpp`, `pool_config.cpp`) still take `const std::string&` instead of `std::string_view`.

See `proposal.md` for full motivation and `specs/` for behavioral requirements.

## Goals / Non-Goals

**Goals:**
- Eliminate all `std::shared_ptr` reference cycles and coroutine teardown hazards in `evm_listener/src/app/tools.cpp` (`R.1`, `R.20`, `CP.51`).
- Ensure `Sweeper::State` and `PipelineDriver::State` own `core::Config` safely across suspension points (`R.20`, `CP.51`).
- Make all `try_*` methods across `sink` and `app/tools.cpp` genuinely non-throwing (`noexcept`) and consistent in naming (`E.2`, `E.6`).
- Complete `std::string_view` adoption across all remaining read-only string parameters and callbacks (`SL.str.2`).

**Non-Goals:**
- Changing external CLI flags, TOML configuration schema, or Redis stream wire formats.
- Replacing startup/configuration exceptions (`ConfigError`, `AbiError`), which are explicitly permitted by Rule 7 (`E.2`).

## Decisions

### Decision 1: Decouple `io_context` from Coroutine State and Use `std::weak_ptr` in `tools.cpp` (`CP.51`, `R.20`)
- **Choice**:
  1. In `probe_transport`, `backfill_contract`, and `audit_completeness`, keep `boost::asio::io_context ioc` on the stack of the synchronous entry function (matching `Host` in `host.cpp`, where `ioc_` outlives all coroutines and teardown runs after `ioc_.run()`), or pass `std::weak_ptr<State>` into the coroutines.
  2. Capture `std::weak_ptr<ProbeState>` (and `std::weak_ptr` for any state referenced by `PoolHandlers`) in `h.on_head` and `h.on_log` so `State -> ProviderPool -> PoolHandlers` never forms a strong cycle.
  3. Store `std::shared_ptr<sink::RedisSink> sink` in `BackfillState` instead of `sink::RedisSink&`.
  4. Perform deterministic cleanup (`pool->stop()`, `pool.reset()`, `sink->stop()`) after the coroutine completes so the coroutine frame unwinds cleanly before `ioc.run()` returns.
- **Alternatives Considered**:
  - *Resetting `st->pool` manually inside `run_probe_coro`*: Fragile if an early `co_return` or unexpected path occurs before reset; using `std::weak_ptr` in `PoolHandlers` prevents the reference cycle by construction.

### Decision 2: Store `core::Config` by Value inside `Sweeper::State` and `PipelineDriver::State` (`CP.51`, `R.20`)
- **Choice**: Store `const core::Config cfg;` by value inside `Sweeper::State` and `PipelineDriver::State`.
- **Rationale**: `Sweeper` and `PipelineDriver` are instantiated once per host startup or once per CLI backfill pass. Copying `core::Config` into `State` at construction time costs microseconds once, completely eliminates the possibility of a dangling `const core::Config&` across `co_await` when a caller constructs `Sweeper` with a coroutine-local `Config` (such as `final_cfg` and `safe_cfg` in `run_backfill_coro`), and requires zero changes to `Sweeper` or `PipelineDriver` constructor signatures.
- **Alternatives Considered**:
  - *Requiring `std::shared_ptr<const core::Config>` at all constructor call sites*: Would force signature churn across every `Sweeper` and `PipelineDriver` unit test with no runtime benefit.

### Decision 3: Standardize `try_acquire_lease` and Non-Throwing Redis Reply Parsing (`E.2`, `E.6`)
- **Choice**:
  1. In `evm_listener/src/sink/lease.hpp` and `lease.cpp`, rename `try_acquire_lease_result` to `try_acquire_lease(...) noexcept` (returning `core::Result<LeaseOutcome, LeaseAcquireError>`) and rename the throwing wrapper to `acquire_lease(...)`. Update all call sites (`host.cpp`, `tools.cpp`, `lease_test.cpp`).
  2. In `evm_listener/src/sink/redispp_conn.cpp`, convert `parse_u64`, `parse_score`, `require_array`, `reply_str`, `reply_u64`, `flat_get`, and `flat_str` to `noexcept` functions returning `core::Result<T, ConnError>` (using `std::from_chars` for integer parsing and `std::strtod` with errno/endptr check for score parsing).
  3. Remove the defensive `try { ... } catch (const std::exception&)` blocks around `conn_->try_read_*` in `RedisSink::execute_read`, `RedisSink::service_consumer_lag`, and `RedisSink::service_tenancy` (`evm_listener/src/sink/redis_sink.cpp`).
  4. In `evm_listener/src/app/tools.cpp`, convert `rpc_call` and `code_at` to return `core::Result<..., std::string> noexcept` (using `client.try_call`, `dec.try_decode`, `dec.try_parse_string`, `dec.try_parse_quantity`), and update `check_config_online` and `audit_stream` to consume `Result` and `std::from_chars` without operational `try/catch`.

### Decision 4: Complete `std::string_view` Migration (`SL.str.2`)
- **Choice**: Update `PoolHandlers` (`on_head`, `on_log`, `on_ws_ready`, `on_ws_down`, `on_tag`), `StartupOptions::on_waiting`, `AcquireOptions::on_waiting`, `AlertEngine::unchanged_for`/`sustained`, and the internal read-only string helpers in `coverage.cpp`, `log_validation.cpp`, `tools.cpp`, `config.cpp`, `host_telemetry.cpp`, and `pool_config.cpp` to accept `std::string_view`.
- **Rationale**: Eliminates unnecessary `std::string` allocations when passing string literals or `std::string_view` slices and aligns all callback signatures in `ProviderPool` and `sink`.

## Risks / Trade-offs

- **[Risk] Lambda signature mismatches in tests when changing `PoolHandlers`, `StartupOptions::on_waiting`, `AcquireOptions::on_waiting`, or `try_acquire_lease`** → **Mitigation**: Perform a full call-graph audit across `evm_listener/src/` and `evm_listener/tests/` and update all lambda and test call sites in the same step; note that in C++, a `std::function<void(std::string_view, ...)>` can still bind a lambda taking `const std::string&` (via implicit construction) or `std::string_view` or `auto`, so existing lambdas remain compatible while updated lambdas avoid allocation.
- **[Risk] Subtle Redis reply parsing edge cases when replacing `std::stoull` / `std::stod` in `redispp_conn.cpp`** → **Mitigation**: Require full-string consumption (`ptr == sv.data() + sv.size()`) in `std::from_chars` / `std::strtod` and add unit tests covering malformed Redis replies in `redis_sink_test.cpp`.
