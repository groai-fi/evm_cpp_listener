# Design

## Context

See `proposal.md` for motivation. Three production-readiness gaps were identified across build reproducibility, provider catch-up throughput, and compile-time static analysis:
1. `docker/Dockerfile.prod` and `.github/workflows/ci.yml` reference `conan.lock`, which was not yet committed to the repository.
2. `ProviderPool` initialized every provider's `TokenBucket` with the single `[redundancy].reconcile_rps` (`2`) and `reconcile_burst` (`4`), limiting QuickNode (`max_getlogs_range = 5`, Discover limit 15 RPS) to `10 blocks/s` during catch-up when wide-range providers are unavailable.
3. `evm_listener/CMakeLists.txt` enabled `-Wall -Wextra -Wpedantic -Werror`, leaving implicit numeric conversions (`-Wconversion`), format strings (`-Wformat=2`), implicit switch fallthrough (`-Wimplicit-fallthrough`), and virtual destructor/overload mismatches unchecked.

## Goals / Non-Goals

**Goals:**
- Commit a deterministic Conan 2 lockfile (`conan.lock`) covering `Debug`, `Release`, and `RelWithDebInfo` across `armv8` and `x86_64`.
- Allow per-provider `reconcile_rps` and `reconcile_burst` overrides on `[[providers]]` while preserving `[redundancy]` defaults when omitted.
- Enable `-Wconversion`, `-Wimplicit-fallthrough`, `-Wformat=2`, `-Wnon-virtual-dtor`, and `-Woverloaded-virtual` under `-Werror` across all C++ targets.

**Non-Goals:**
- Changing global `[redundancy]` defaults (`reconcile_rps = 2`, `reconcile_burst = 4`) for providers that omit per-provider overrides.
- Modifying AIMD paging bounds (`min_range` / `max_getlogs_range`).

## Decisions

### D1: Optional `reconcile_rps` and `reconcile_burst` on `core::Provider` and `provider::ProviderEndpoint`
- **Choice**: Add `std::optional<std::uint32_t> reconcile_rps{}` and `std::optional<std::uint32_t> reconcile_burst{}` to `core::Provider` and `provider::ProviderEndpoint`. In `ProviderPool`, resolve `rps = p.reconcile_rps.value_or(cfg_.reconcile_rps)` and `burst = p.reconcile_burst.value_or(std::max(cfg_.reconcile_burst, rps))`.
- **Rationale**: Preserves backward compatibility with all existing TOML files and unit test aggregate initializers `ProviderEndpoint{"A", "ws://...", "http://..."}` while allowing high-RPS narrow-range providers like `quicknode` (`reconcile_rps = 10`, `reconcile_burst = 15`) to run 5x faster during catch-up.
- **Alternative Considered**: Raising `[redundancy].reconcile_rps` globally to `10`. Rejected because public/shared endpoints (`base_public`) would be rate-limited at 10 RPS.

### D2: Warning Flags Scope in `evm_listener/CMakeLists.txt`
- **Choice**: Apply `-Wall -Wextra -Wpedantic -Wconversion -Wimplicit-fallthrough -Wformat=2 -Wnon-virtual-dtor -Woverloaded-virtual -Werror` to `EVML_TARGETS` (`evml_core`, `evm_listener`, `evml_tests`), while omitting `-Wnull-dereference` due to GCC 12 `-O3` inliner false positives inside `libstdc++` `<variant>` and Boost.Beast `fields.hpp`.

## Risks / Trade-offs

- **[Risk] Per-provider `reconcile_rps` set higher than `reconcile_burst`** → **Mitigation**: Both `Config::parse` (`[[providers]][i].reconcile_burst`) and `ProviderPool::validate` reject configurations where `reconcile_burst < reconcile_rps` at startup.
- **[Risk] Conan lockfile drift when `conanfile.py` changes** → **Mitigation**: `conan install` fails fast if `conanfile.py` diverges from `conan.lock`, prompting explicit lockfile regeneration.

