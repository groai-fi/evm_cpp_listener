# Proposal

## Why

To close the remaining production-readiness gaps identified during the repository audit: (1) `conan.lock` was committed at the repository root, but `conan install` across Docker (`docker/Dockerfile.prod` with optional `conan.lock*`), CI (`.github/workflows/ci.yml`), and Make (`mk/10-conan.mk`, `mk/20-build.mk`) ran without `--lockfile=conan.lock`, (2) a single global `[redundancy] reconcile_rps = 2` bottlenecked catch-up throughput on narrow-range providers like QuickNode (`max_getlogs_range = 5`, which allows 15 RPS on Discover), and (3) C++ compilation targets enabled `-Wall -Wextra -Wpedantic -Werror` but lacked strict implicit-conversion and virtual/format warning flags (`-Wconversion`, `-Wimplicit-fallthrough`, `-Wformat=2`, `-Wnon-virtual-dtor`, `-Woverloaded-virtual`).

## What Changes

- **Enforce `conan.lock` Across Docker, CI, and Make**: Require `conan.lock` explicitly in `docker/Dockerfile.prod` (`COPY VERSION conanfile.py conan.lock ./`) and pass `--lockfile=conan.lock` to every `conan install` invocation across `docker/Dockerfile.prod`, `.github/workflows/ci.yml`, `mk/10-conan.mk`, and `mk/20-build.mk` so all local, CI, and production Docker builds strictly enforce locked recipe revisions.
- **Per-Provider `reconcile_rps` and `reconcile_burst` Overrides**: Allow optional `reconcile_rps` and `reconcile_burst` keys on individual `[[providers]]` entries in TOML configuration (falling back to `[redundancy] reconcile_rps` and `reconcile_burst` when omitted), and configure `quicknode` with `reconcile_rps = 10` and `reconcile_burst = 15` in `evm_listener/config/base.toml` (increasing QuickNode fallback catch-up throughput 5x from `10 blocks/s` to `50 blocks/s`).
- **Enforce Strict Compiler Warning Flags**: Enforce `-Wall`, `-Wextra`, `-Wpedantic`, `-Wconversion`, `-Wimplicit-fallthrough`, `-Wformat=2`, `-Wnon-virtual-dtor`, `-Woverloaded-virtual`, and `-Werror` on both the core library (`evml_core`), application executable (`evm_listener`), and test binary (`evml_tests`) in `evm_listener/CMakeLists.txt`.

## Capabilities

### New Capabilities
- `provider-rate-limiting`: Per-provider token-bucket rate limiting (`reconcile_rps` and `reconcile_burst`) with global `[redundancy]` defaults and per-provider `[[providers]]` overrides.

### Modified Capabilities
- `build-and-packaging`: Enforce deterministic Conan dependency revisions via `--lockfile=conan.lock` across Docker, CI, and Make, and enforce strict C++ compiler warning flags (`-Wall`, `-Wextra`, `-Wpedantic`, `-Wconversion`, `-Wimplicit-fallthrough`, `-Wformat=2`, `-Wnon-virtual-dtor`, `-Woverloaded-virtual`, `-Werror`) across all targets.

## Impact

- **Build & Packaging**: `conan.lock`, `docker/Dockerfile.prod`, `.github/workflows/ci.yml`, `mk/10-conan.mk`, `mk/20-build.mk`, `evm_listener/CMakeLists.txt`
- **Core Config & Provider Pool**: `evm_listener/src/core/config.hpp`, `evm_listener/src/core/config.cpp`, `evm_listener/src/provider/provider_pool.hpp`, `evm_listener/src/provider/provider_pool.cpp`, `evm_listener/src/app/pool_config.cpp`, `evm_listener/config/base.toml`
- **Tests**: `evm_listener/tests/config_test.cpp`, `evm_listener/tests/pool_config_test.cpp`, `evm_listener/tests/provider_pool_test.cpp`, `evm_listener/tests/host_test.cpp`

