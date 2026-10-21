# Technical Design: Modular Build, Versioning, and Packaging System

## Context

The EVM listener is a high-performance C++20 event listener and Redis stream publisher. It uses CMake (presets), Conan package management, CTest, and container runtimes (Docker/Docker Compose). Prior to this change, build steps, container triggers, version numbers, and test invocations were scattered across a monolithic 400+ line `Makefile`, inline Conan profile scriptlets, and ad-hoc header definitions. See [proposal.md](file:///Users/jonathan/MyCode/GroAI.fi/evm_cpp_listener/openspec/changes/build-system-modularization/proposal.md) for background and motivation.

## Goals / Non-Goals

**Goals:**
- Decompose the root `Makefile` into modular components under `mk/` with ordered prefixing.
- Establish a single source of truth for semantic versioning via root `VERSION`.
- Automatically inject SemVer and Git describe metadata into compiled binaries via CMake definitions.
- Provide automated SemVer bumping (`bump-patch`, `bump-minor`, `bump-major`).
- Enable granular test suite filtering via CTest labels (`unit` and `integration`).
- Standardize multi-stage container builds for dev, staging, and production environments.

**Non-Goals:**
- Replacing CMake or CTest with alternative build orchestration systems (e.g., Bazel, Meson).
- Modifying core C++ event processing, pipeline concurrency, or Redis sink protocols.
- Cloud-native Kubernetes helm charts or remote deployment orchestration (kept to local Docker Compose and image builds).

## Decisions

### 1. Ordered Makefile Decomposition (`mk/*.mk`) vs Recursive Make
- **Decision**: Include modular files from a central root `Makefile` using ordered numeric prefixes (`00-vars.mk`, `10-version.mk`, `20-dev.mk`, `30-test.mk`, `40-lint.mk`, `50-staging.mk`, `60-prod.mk`).
- **Rationale**: Keeps Make execution flat and single-process, avoiding the well-documented pitfalls of Recursive Make (variable leakage, skewed dependency tracking, race conditions).
- **Alternative considered**: Recursive Make (`make -C mk/test`). Rejected due to synchronization overhead and variable scope isolation problems.

### 2. File-Based `VERSION` vs Git-Tag Only or C++ Header Macro
- **Decision**: Store canonical version in plain text `VERSION` at repository root.
- **Rationale**: Universal format directly readable by shell scripts, Make, CMake (`file(READ ...)`), Docker, and CI pipelines without requiring language runtimes.
- **Alternative considered**: C++ header (`version.hpp`). Rejected because parsing C++ macros in Dockerfiles and shell scripts is brittle. Git tags only was rejected because shallow git clones in CI/Docker often lack tags.

### 3. Dynamic Git Metadata Injection
- **Decision**: Read `VERSION` in `CMakeLists.txt` for `PROJECT_VERSION`, and query `git describe --tags --always --dirty` if inside a git repository, passing `-DEVML_VERSION_STRING` and `-DEVML_GIT_COMMIT` to `evm_listener`.
- **Rationale**: Local dev builds cleanly report `0.1.0 (hash-dirty)`, making binary provenance explicit while preserving clean release versions in immutable production containers.

### 4. CTest Label Tagging for Unit vs Integration Suites
- **Decision**: Tag CMake test targets with `set_tests_properties(... PROPERTIES LABELS "unit")` and `set_tests_properties(... PROPERTIES LABELS "integration")`.
- **Rationale**: Allows developers to run fast unit suites (`make test-unit`) in sub-second cycles, while reserving slower integration and network tests (`make test-integration`) for comprehensive validation.

### 5. Centralized Conan Profile
- **Decision**: Store Conan profile under `conan/profiles/dev-linux-x86_64` and reference it directly in `make conan` (`conan install ... -pr:b=conan/profiles/dev-linux-x86_64 -pr:h=conan/profiles/dev-linux-x86_64`).
- **Rationale**: Eliminates dynamic profile generation inside make recipes and ensures reproducible lockfile generation across dev machines and CI runners.

## Risks / Trade-offs

- **[Risk] Host toolchain discrepancies**: Different host OSes (macOS vs Linux) have varying versions of `sed` or `awk` for version bumping.  
  → *Mitigation*: Version bump targets use portable standard `awk` scripts that execute identically across BSD (macOS) and GNU (Linux) platforms.
- **[Risk] Missing local dependencies (luacheck, clang-tidy)**: Developer machines may lack specific linters.  
  → *Mitigation*: Makefile checks tool presence gracefully and provides containerized fallback targets (`dev-sh`, `make lint-lua`).
