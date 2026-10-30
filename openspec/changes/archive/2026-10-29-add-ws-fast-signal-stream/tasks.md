# Tasks

## 1. Configuration, Finality Enum, and Key Schema

- [x] 1.1 Add `Finality::Latest` (`to_string` returning `"latest"`) in `evm_listener/src/core/config.hpp` and `evm_listener/src/core/config.cpp` (keeping TOML `parse_finality` restricted to `safe|finalized|depth`), add `bool fast_stream{false}` to `core::Contract` and `core::Route`, add `std::uint64_t fast_retention_ms{3600000}` (`>= 1000`) to `core::SinkCfg`, and verify `make build` compiles with zero warnings.
- [x] 1.2 Enable `fast_stream = true` on `eurc_usdc_cl1_pool` and `eurc_usdc_cl50_pool` (and `fast_retention_ms = 3600000` in `[sink]`) in `evm_listener/config/base.toml`, add `Keys::fast()` returning `evml:{<shard>}:fast` in `evm_listener/src/sink/keys.hpp`, and add unit tests in `evm_listener/tests/config_test.cpp` and `evm_listener/tests/keys_test.cpp` verifying default/explicit `fast_stream`, invalid `fast_retention_ms` rejection, and `Keys::fast()`, verified via `./build/Debug/evm_listener/tests/evml_tests "[config]" "[keys]"`.

## 2. Embedded `emit_fast.lua` Script and Redis Sink Non-Spilling Hot Path

- [x] 2.1 Create `evm_listener/src/sink/lua/emit_fast.lua` (fenced on `KEYS[2]` leader token when `ARGV[2] != '0'`, executing `XADD KEYS[1] MINID ~ ARGV[1] * ...` with zero `dedup:*` or `released` writes), register `emit_fast` in `evm_listener/CMakeLists.txt`, `evm_listener/src/sink/lua_sources.hpp.in`, `evm_listener/src/sink/lua_scripts.hpp`, and `evm_listener/src/sink/lua_scripts.cpp`, and verify `script_runner_test.cpp` passes (`./build/Debug/evm_listener/tests/evml_tests "[lua]"`).
- [x] 2.2 Add `virtual void emit_fast(pipeline::Event) {}` to `evm_listener/src/pipeline/sink.hpp` and implement best-effort, non-spilling `RedisSink::emit_fast(pipeline::Event)` (`detail::FastEmitItem`, `fast_retention_ms` in `RedisSinkOptions`, `fast_emitted_total` and `fast_dropped_total` in `SinkStatus`, dropping `FastEmitItem` when not `SinkMode::Connected`, when `saturated()`, or when transitioning to `Degraded`/`Spill`), and verify unit + integration tests in `evm_listener/tests/redis_sink_test.cpp` pass (`./build/Debug/evm_listener/tests/evml_tests "[sink]"`).

## 3. Host WebSocket Fast Emission, In-Memory Timestamp Lookup, and Telemetry

- [x] 3.1 Add `std::optional<std::uint64_t> timestamp_of(std::uint64_t number, const core::Hash32& hash) const` to `chain::ReorgDetector` (`evm_listener/src/chain/reorg_detector.hpp` / `.cpp`) to look up cached tip timestamps from `newHeads` in O(1) without RPC calls, and verify with a unit test in `evm_listener/tests/reorg_detector_test.cpp`.
- [x] 3.2 Update `Host::on_ws_log` (`evm_listener/src/app/host.cpp`) so that when a non-removed WebSocket log matches a route with `route->fast_stream == true`, the leader constructs a schema `v=1` `pipeline::Event` with `finality = core::Finality::Latest` and `block_timestamp` from `rt_->ring.timestamp_of(...)` (or `0` if not yet in ring) and calls `rt_->sink->emit_fast(std::move(ev))` in addition to `rt_->ws_reconciler.on_ws_log(log, provider)`; export `evml_fast_emitted_total` and `evml_fast_dropped_total` in `evm_listener/src/ops/metrics.hpp`, `metrics.cpp`, and `host_telemetry.cpp`, and verify with unit/integration tests in `evm_listener/tests/metrics_test.cpp` and `evm_listener/tests/host_test.cpp`.

## 4. Full Regression Suite, Docs Update, and Live Docker Verification

- [x] 4.1 Update `doc/System_Modules_and_Architecture.md` to document the Dual-Plane (`evml:{<shard>}:fast` + `evml:{<shard>}:events`) architecture and execute the full Debug (ASan+UBSan) and Release CTest suites (`make build && ctest --test-dir build/Debug --output-on-failure`), verifying 100% pass rate (`0` failures) and a live local WebSocket test confirming `evml:{base_eurc_usdc}:fast` receives sub-150ms `Swap` entries with zero `dedup:*` keys created by the fast path.

## Workflow follow-up

- Deploy to Railway `staging` (`staging` branch) and verify live `evml:{base_eurc_usdc}:fast` emissions alongside `evml:{base_eurc_usdc}:events`.
- Archive the OpenSpec change (`openspec archive add-ws-fast-signal-stream`) after implementation and verification are complete.

