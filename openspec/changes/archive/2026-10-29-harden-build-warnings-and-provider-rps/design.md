# Design

## Context

See `proposal.md` for motivation. Three production-readiness gaps were identified across build reproducibility, provider catch-up throughput, and compile-time static analysis:
1. `conan.lock` was committed at the repository root, but `docker/Dockerfile.prod` used an optional `conan.lock*` glob and `conan install` across `docker/Dockerfile.prod`, `.github/workflows/ci.yml`, `mk/10-conan.mk`, and `mk/20-build.mk` ran without `--lockfile=conan.lock`.
2. `ProviderPool` initialized every provider's `TokenBucket` with the single `[redundancy].reconcile_rps` (`2`) and `reconcile_burst` (`4`), limiting QuickNode (`max_getlogs_range = 5`, Discover limit 15 RPS) to `10 blocks/s` during catch-up when wide-range providers are unavailable.
3. `evm_listener/CMakeLists.txt` enabled `-Wall -Wextra -Wpedantic -Werror` on `EVML_TARGETS` (`evml_core`, `evm_listener`, `evml_tests`), leaving implicit numeric conversions (`-Wconversion`), format strings (`-Wformat=2`), implicit switch fallthrough (`-Wimplicit-fallthrough`), and virtual destructor/overload mismatches unchecked.

## Goals / Non-Goals

**Goals:**
- Enforce the committed `conan.lock` via `--lockfile=conan.lock` across `docker/Dockerfile.prod` (with mandatory `COPY VERSION conanfile.py conan.lock ./`), `.github/workflows/ci.yml`, `mk/10-conan.mk`, and `mk/20-build.mk`.
- Allow per-provider `reconcile_rps` and `reconcile_burst` overrides on `[[providers]]` while strictly falling back to `[redundancy]` defaults when omitted and failing fast if effective `reconcile_burst < reconcile_rps`.
- Enforce `-Wall -Wextra -Wpedantic -Wconversion -Wimplicit-fallthrough -Wformat=2 -Wnon-virtual-dtor -Woverloaded-virtual -Werror` across `evml_core`, `evm_listener`, and `evml_tests` in `evm_listener/CMakeLists.txt`.

**Non-Goals:**
- Changing global `[redundancy]` defaults (`reconcile_rps = 2`, `reconcile_burst = 4`) for providers that omit per-provider overrides.
- Modifying AIMD paging bounds (`min_range` / `max_getlogs_range`).

## Decisions

### D1: Optional `reconcile_rps` and `reconcile_burst` on `core::Provider` and `provider::ProviderEndpoint`
- **Choice**: Add `std::optional<std::uint32_t> reconcile_rps{}` and `std::optional<std::uint32_t> reconcile_burst{}` to `core::Provider` and `provider::ProviderEndpoint`. In both `Config::parse` and `ProviderPool`, resolve `eff_rps = p.reconcile_rps.value_or(default_rps)` and `eff_burst = p.reconcile_burst.value_or(default_burst)`, and fail fast if `eff_burst < eff_rps`.
- **Rationale**: Preserves backward compatibility with all existing TOML files and unit test aggregate initializers `ProviderEndpoint{"A", "ws://...", "http://..."}` while allowing high-RPS narrow-range providers like `quicknode` (`reconcile_rps = 10`, `reconcile_burst = 15`) to run 5x faster during catch-up without silently clamping or overriding `[redundancy].reconcile_burst`.
- **Alternative Considered**: Raising `[redundancy].reconcile_rps` globally to `10`. Rejected because public/shared endpoints (`base_public`) would be rate-limited at 10 RPS.

### D2: Warning Flags Scope in `evm_listener/CMakeLists.txt`
- **Choice**: Apply `-Wall -Wextra -Wpedantic -Wconversion -Wimplicit-fallthrough -Wformat=2 -Wnon-virtual-dtor -Woverloaded-virtual -Werror` to `EVML_TARGETS` (`evml_core`, `evm_listener`, `evml_tests`), while omitting `-Wnull-dereference` due to GCC 12 `-O3` inliner false positives inside `libstdc++` `<variant>` and Boost.Beast `fields.hpp`.

## Risks / Trade-offs

- **[Risk] Per-provider `reconcile_rps` set higher than effective `reconcile_burst`** → **Mitigation**: Both `Config::parse` (`[[providers]][i].reconcile_burst`) and `ProviderPool::validate` reject configurations where effective `reconcile_burst < reconcile_rps` at startup without silent `std::max` clamping.
- **[Risk] Conan lockfile drift when `conanfile.py` changes** → **Mitigation**: `conan install --lockfile=conan.lock` fails fast if `conanfile.py` diverges from `conan.lock`, prompting explicit lockfile regeneration.

