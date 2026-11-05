# cpp20-core-guidelines Specification

## Purpose
Defines C++20 Core Guidelines requirements for the EVM listener runtime and CLI tools, covering coroutine and async lifetime safety, cycle-free resource ownership, non-throwing operational error handling, and non-owning string/byte view interfaces.

## Requirements

### Requirement: Cycle-Free Coroutine and Async Callback Ownership
All Boost.Asio coroutines and provider/sink callbacks across both long-running host execution and CLI subcommands (`probe-transport`, `backfill-contract`, `audit-completeness`) SHALL avoid cyclic `std::shared_ptr` ownership graphs and SHALL NOT capture raw references to stack-allocated state across `co_await` suspension points.

#### Scenario: CLI transport probe tears down cleanly without reference cycles
- **WHEN** `evm_listener probe-transport` completes or times out
- **THEN** all `ProviderPool` handlers hold non-owning `std::weak_ptr` observers, `ProviderPool::stop()` is executed before destruction, and the probe state, provider pool, and `io_context` destruct with zero leaked reference cycles under AddressSanitizer.

#### Scenario: CLI backfill and completeness audit coroutines unwind before io_context destruction
- **WHEN** `evm_listener backfill-contract` or `evm_listener audit-completeness` finishes its async coroutine workflow
- **THEN** the coroutine frame completes and releases all captured state before `io_context::run()` returns, with no raw stack references (`RedisSink&` or stack-local `Config&`) held across `co_await` points.

#### Scenario: Pipeline and sweeper state own configuration across suspension points
- **WHEN** `Sweeper` or `PipelineDriver` suspends across `co_await` during a sweep or backfill pass
- **THEN** the heap-allocated state owns its `core::Config` copy (rather than holding a raw `const core::Config&` reference) so temporary or coroutine-local configurations cannot dangle.

### Requirement: Non-Throwing Operational Error Handling via Result
Operational RPC, HTTP, JSON-RPC decode, Redis command/reply parsing, and tenancy lease operations SHALL return `core::Result<T, E>` and be marked `noexcept` rather than throwing exceptions on network, timeout, or malformed payload errors.

#### Scenario: Consistent try_ prefix for non-throwing tenancy lease acquisition
- **WHEN** a caller invokes `sink::try_acquire_lease`
- **THEN** the function is `noexcept` and returns `core::Result<LeaseOutcome, LeaseAcquireError>`, while `sink::acquire_lease` serves as the throwing wrapper.

#### Scenario: Malformed Redis replies return ConnError without throwing
- **WHEN** `RedisppConn::try_read_released`, `try_read_checkpoint`, `try_read_group_facts`, or `try_read_tenancy_facts` receives a malformed integer, floating-point score, or unexpected array structure from Redis
- **THEN** internal reply parsing returns `core::unexpected(ConnError{...})` without throwing `std::runtime_error`, and `RedisSink` consumes the `Result` directly without `try/catch` blocks.

#### Scenario: CLI operational RPC probes and stream audit handle failures via Result
- **WHEN** `check-config --online`, `backfill-contract`, or `audit-stream` encounters an RPC transport error, JSON-RPC error, or malformed Redis stream entry
- **THEN** the operational helper propagates the failure via `core::Result` and `std::from_chars` without throwing operational exceptions.

### Requirement: Non-Owning String Views on Read-Only Interfaces
Read-only string parameters in callbacks, configuration lookups, telemetry helpers, and log validation functions SHALL use `std::string_view` instead of `const std::string&`, and string-keyed associative containers SHALL support heterogeneous lookup via transparent hashing.

#### Scenario: ProviderPool and Redis startup/lease callbacks pass string_view
- **WHEN** `ProviderPool`, `StartupOptions`, or `AcquireOptions` invokes a registered status or event callback (`on_head`, `on_log`, `on_ws_ready`, `on_ws_down`, `on_tag`, `on_waiting`)
- **THEN** the provider name, address family, or error message is passed as `std::string_view` without forcing temporary `std::string` allocations.
