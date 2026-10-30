# Tasks

## 1. Build Reproducibility & Compiler Warning Hardening

- [x] 1.1 Generate multi-profile `conan.lock` covering `Debug`, `Release`, and `RelWithDebInfo` across `armv8` and `x86_64`, and verify `conan install . -pr:a=conan/profiles/debug` and `conan install . -pr:a=conan/profiles/release` succeed using `conan.lock`
- [x] 1.2 Add `-Wconversion`, `-Wimplicit-fallthrough`, `-Wformat=2`, `-Wnon-virtual-dtor`, and `-Woverloaded-virtual` (under `-Werror`) to `EVML_TARGETS` in `evm_listener/CMakeLists.txt`, and verify both `conan-debug` and `conan-release` builds compile with zero warnings

## 2. Per-Provider Reconcile Rate Limiting (`reconcile_rps` / `reconcile_burst`)

- [x] 2.1 Add optional `reconcile_rps` and `reconcile_burst` overrides to `core::Provider` (`evm_listener/src/core/config.hpp` / `.cpp`), `provider::ProviderEndpoint` (`evm_listener/src/provider/provider_pool.hpp` / `.cpp`), and `app::make_pool_config` (`evm_listener/src/app/pool_config.cpp`), configure `quicknode` (`reconcile_rps = 10`, `reconcile_burst = 15`) in `evm_listener/config/base.toml`, and verify unit tests in `evm_listener/tests/config_test.cpp` and `evm_listener/tests/pool_config_test.cpp` pass
- [x] 2.2 Run `evml_tests` for `[config]`, `[pool_config]`, `[provider_pool]`, and `[token_bucket]` in both Debug and Release builds and verify 100% pass rate (`0` failures)

