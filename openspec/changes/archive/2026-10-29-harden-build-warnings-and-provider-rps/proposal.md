# Proposal

## Why

To close the remaining production-readiness gaps identified during the repository audit: (1) transitive Conan recipe revisions were not pinned in a tracked `conan.lock` lockfile, (2) a single global `[redundancy] reconcile_rps = 2` bottlenecked catch-up throughput on narrow-range providers like QuickNode (`max_getlogs_range = 5`, which allows 15 RPS on Discover), and (3) C++ compilation targets lacked strict implicit-conversion and virtual/format warning flags (`-Wconversion`, `-Wimplicit-fallthrough`, `-Wformat=2`, `-Wnon-virtual-dtor`, `-Woverloaded-virtual`).

## What Changes

- **Pin Conan 2 Dependency Graph (`conan.lock`)**: Generate and track a multi-profile `conan.lock` covering `Release`, `Debug`, and `RelWithDebInfo` across `armv8` and `x86_64` so all local, CI, and production Docker builds resolve identical recipe revisions.
- **Per-Provider `reconcile_rps` and `reconcile_burst` Overrides**: Allow optional `reconcile_rps` and `reconcile_burst` keys on individual `[[providers]]` entries in TOML configuration (falling back to `[redundancy] reconcile_rps` and `reconcile_burst` when omitted), and configure `quicknode` with `reconcile_rps = 10` and `reconcile_burst = 15` in `evm_listener/config/base.toml` (increasing QuickNode fallback catch-up throughput 5x from `10 blocks/s` to `50 blocks/s`).
- **Enforce Strict Compiler Warning Flags**: Add `-Wconversion`, `-Wimplicit-fallthrough`, `-Wformat=2`, `-Wnon-virtual-dtor`, and `-Woverloaded-virtual` (under `-Werror`) to `evml_core`, `evm_listener`, and `evml_tests` in `evm_listener/CMakeLists.txt`.

## Capabilities

### New Capabilities
- `provider-rate-limiting`: Per-provider token-bucket rate limiting (`reconcile_rps` and `reconcile_burst`) with global `[redundancy]` defaults and per-provider `[[providers]]` overrides.

### Modified Capabilities
- `build-and-packaging`: Pin deterministic Conan dependency revisions via a committed `conan.lock` and enforce strict C++ compiler warning flags (`-Wconversion`, `-Wimplicit-fallthrough`, `-Wformat=2`, `-Wnon-virtual-dtor`, `-Woverloaded-virtual`, `-Werror`) across all targets.

## Impact

- **Build & Packaging**: `conan.lock`, `evm_listener/CMakeLists.txt`
- **Core Config & Provider Pool**: `evm_listener/src/core/config.hpp`, `evm_listener/src/core/config.cpp`, `evm_listener/src/provider/provider_pool.hpp`, `evm_listener/src/provider/provider_pool.cpp`, `evm_listener/src/app/pool_config.cpp`, `evm_listener/config/base.toml`
- **Tests**: `evm_listener/tests/config_test.cpp`, `evm_listener/tests/pool_config_test.cpp`

