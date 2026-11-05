# Proposal

## Why

The two previous remediation passes in conversation `42f96826-bb97-4b54-9697-dc7afd28c4d5` (commits `106a12b`..`9fa909a`) established C++20 Core Guidelines enforcement across `Host`, `ProviderPool`, `WsSession`, `HttpRpcClient`, `ops::HttpClient`, and `clang-tidy`, but left four concrete categories of C++20 Core Guidelines violations in the codebase:
1. **`std::shared_ptr` reference cycles and premature `io_context::stop()` inside coroutines in `evm_listener/src/app/tools.cpp` (`R.1`, `R.20`, `CP.51`)**: `probe_transport` creates a hard reference cycle (`st -> st->pool -> handlers_ -> st`) because `PoolHandlers` lambdas capture `std::shared_ptr<ProbeState> st` by value while `st` owns `st->pool`; additionally, `run_probe_coro`, `run_backfill_coro`, and `run_audit_coro` capture `std::shared_ptr<State>` where `State` owns `boost::asio::io_context ioc` and call `st->ioc.stop()` from inside the coroutine before the coroutine frame finishes unwinding, and `BackfillState` holds a raw `sink::RedisSink&` reference across coroutine suspension points.
2. **Heap-allocated `Sweeper::State` and `PipelineDriver::State` store raw `const core::Config&` references across `co_await` (`R.20`, `CP.51`)**: `Sweeper` and `PipelineDriver` store raw `const core::Config& cfg` inside their `std::shared_ptr<State>` even when constructed with stack-local `core::Config` copies inside coroutine frames (e.g., `final_cfg` and `safe_cfg` in `run_backfill_coro`).
3. **Incomplete `core::Result<T>` operational error handling and API inconsistency (`E.2`, `E.6`)**: `sink::try_acquire_lease` throws `ConnError` while `try_acquire_lease_result` returns `Result` (inverting the repo's `try_*` naming convention); `RedisppConn::try_read_*` only catches `sw::redis::Error` while internal Redis reply parsers (`parse_u64`, `parse_score`, `require_array`, `reply_str`, `reply_u64`, `flat_get`, `flat_str`) throw `std::runtime_error`, forcing `RedisSink` (`execute_read`, `service_consumer_lag`, `service_tenancy`) to wrap `try_*` calls in `try/catch`; and CLI/audit operational paths in `tools.cpp` (`rpc_call`, `code_at`, `check_config_online`, `audit_stream`) still throw `RpcFailure`, `TransportError`, `DecodeError`, `ConnError`, and `std::stoull` exceptions on network/Redis data.
4. **Remaining read-only `const std::string&` parameters (`SL.str.2`)**: `PoolHandlers` callbacks (`on_head`, `on_log`, `on_ws_ready`, `on_ws_down`, `on_tag`), `StartupOptions::on_waiting`, `AcquireOptions::on_waiting`, `AlertEngine::unchanged_for`/`sustained`, and helper functions in `coverage.cpp`, `log_validation.cpp`, `tools.cpp`, `config.cpp`, `host_telemetry.cpp`, and `pool_config.cpp` still accept `const std::string&` instead of `std::string_view`.

## What Changes

- **Break `std::shared_ptr` Reference Cycles and Fix Coroutine Teardown in `evm_listener/src/app/tools.cpp` (`R.1`, `R.20`, `CP.51`)**:
  - Capture `std::weak_ptr<ProbeState>`, `std::weak_ptr<BackfillState>`, and `std::weak_ptr<AuditFetchState>` in `PoolHandlers` callbacks inside `probe_transport`, `backfill_contract`, and `audit_completeness`.
  - Decouple `boost::asio::io_context` ownership from the coroutine-captured state in `probe_transport`, `backfill_contract`, and `audit_completeness` (or pass `std::weak_ptr<State>` into `run_probe_coro`, `run_backfill_coro`, and `run_audit_coro`) and explicitly run `pool->stop()` / reset `pool` after coroutine completion so `State`, `ProviderPool`, and `io_context` deterministically destruct with zero leaked cycles.
  - Replace the raw `sink::RedisSink&` reference in `BackfillState` with shared ownership (`std::shared_ptr<sink::RedisSink>`).
- **Eliminate Raw `const core::Config&` References in `Sweeper::State` and `PipelineDriver::State` (`R.20`, `CP.51`)**:
  - Store `core::Config` by value (or `std::shared_ptr<const core::Config>`) inside `Sweeper::State` (`evm_listener/src/pipeline/sweeper.cpp`) and `PipelineDriver::State` (`evm_listener/src/pipeline/pipeline_driver.cpp`) so coroutine frames never dangle on temporary or stack-local `Config` instances.
- **Unify `core::Result<T>` Operational Error Handling Across `sink` and `app/tools.cpp` (`E.2`, `E.6`)**:
  - Standardize `evm_listener/src/sink/lease.hpp` / `lease.cpp` naming so `try_acquire_lease(...) noexcept` returns `core::Result<LeaseOutcome, LeaseAcquireError>` and `acquire_lease(...)` is the throwing wrapper (matching `try_release_lease` / `release_lease` and all other `try_*` APIs).
  - Refactor internal reply parsers in `evm_listener/src/sink/redispp_conn.cpp` (`parse_u64`, `parse_score`, `require_array`, `reply_str`, `reply_u64`, `flat_get`, `flat_str`) to return `core::Result<T, ConnError> noexcept` using `std::from_chars` / `std::strtod` instead of throwing `std::runtime_error`, and remove the workaround `try/catch` blocks around `conn_->try_read_*` in `evm_listener/src/sink/redis_sink.cpp`.
  - Refactor operational RPC and Redis helpers in `evm_listener/src/app/tools.cpp` (`rpc_call`, `code_at`, `has_code`, `check_config_online`, `audit_stream`) to use `core::Result<T>` (`client.try_call`, `dec.try_decode`, `dec.try_parse_*`, `conn.try_read_checkpoint`, and `std::from_chars`) instead of throwing/catching operational exceptions.
- **Complete `std::string_view` Migration for Read-Only String Parameters (`SL.str.2`)**:
  - Update `provider::PoolHandlers` (`on_head`, `on_log`, `on_ws_ready`, `on_ws_down`, `on_tag`) in `evm_listener/src/provider/provider_pool.hpp` and all call sites/tests to take `std::string_view`.
  - Update `sink::StartupOptions::on_waiting` (`evm_listener/src/sink/redis_startup.hpp`) and `sink::AcquireOptions::on_waiting` (`evm_listener/src/sink/lease.hpp`) to take `std::string_view`.
  - Update remaining read-only `const std::string&` parameters in `ops/alerts.hpp` / `alerts.cpp`, `pipeline/coverage.cpp`, `pipeline/log_validation.cpp`, `app/tools.cpp`, `core/config.cpp`, `app/host_telemetry.cpp`, and `app/pool_config.cpp` to `std::string_view`.

## Capabilities

### New Capabilities
- `cpp20-core-guidelines`: Enforces C++20 Core Guidelines across `evm_listener` for coroutine and async lifetime safety (`CP.51`, `CP.52`), cycle-free smart-pointer ownership (`R.1`, `R.20`–`R.23`), non-throwing `core::Result<T>` operational error handling (`E.2`, `E.6`), and non-owning `std::string_view` / `BytesView` parameter hygiene (`SL.str.2`, `Bounds.4`).

### Modified Capabilities
- `build-and-packaging`: Adds strict C++20 standard compliance (`CXX_EXTENSIONS OFF`), `clang-tidy` enforcement (including `bugprone-exception-escape`, `bugprone-use-after-move`, `bugprone-dangling-handle`), and `ASan+UBSan` and `TSan` sanitizer verification requirements.

## Impact

- **Application & CLI Tools**: `evm_listener/src/app/tools.cpp`, `evm_listener/src/app/host.cpp`, `evm_listener/src/app/host_telemetry.cpp`, `evm_listener/src/app/pool_config.cpp`
- **Pipeline & Provider**: `evm_listener/src/pipeline/sweeper.cpp`, `evm_listener/src/pipeline/pipeline_driver.cpp`, `evm_listener/src/pipeline/coverage.cpp`, `evm_listener/src/pipeline/log_validation.cpp`, `evm_listener/src/provider/provider_pool.hpp`
- **Redis Sink & Tenancy**: `evm_listener/src/sink/lease.hpp`, `evm_listener/src/sink/lease.cpp`, `evm_listener/src/sink/redis_startup.hpp`, `evm_listener/src/sink/redispp_conn.cpp`, `evm_listener/src/sink/redis_sink.cpp`
- **Core & Ops**: `evm_listener/src/core/config.cpp`, `evm_listener/src/ops/alerts.hpp`, `evm_listener/src/ops/alerts.cpp`
- **Tests**: `evm_listener/tests/lease_test.cpp`, `evm_listener/tests/tools_test.cpp`, `evm_listener/tests/redis_sink_test.cpp`, `evm_listener/tests/provider_pool_test.cpp`
