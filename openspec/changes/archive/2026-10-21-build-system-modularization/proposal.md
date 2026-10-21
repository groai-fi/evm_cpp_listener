# Proposal: Modular Build, Versioning, and Packaging System

## Why

The original build configuration relied on a monolithic, 400+ line `Makefile` with mixed developer, CI, container, and test concerns, hardcoded paths, duplicated Conan profile definitions, and fragmented versioning logic across scripts and headers. This prevented reproducible builds, clean environment segregation (dev vs. staging vs. prod), and reliable automated semantic version bumping.

Refactoring into modular makefiles (`mk/*.mk`) with a single source-of-truth `VERSION` file, dynamic git metadata embedding, and unified container workflows provides deterministic local and CI builds, streamlined developer onboarding, and verified multi-stage production images.

## What Changes

- **Modular Makefile Hierarchy**: Split the root `Makefile` into focused modules under `mk/`:
  - `mk/00-vars.mk`: Common paths, compiler flags, cross-platform architecture detection, and container runtime checks.
  - `mk/10-version.mk`: Single-source-of-truth version management (`VERSION`), git metadata extraction (`VERSION_GIT_TAG`), and automated bumping targets (`bump-patch`, `bump-minor`, `bump-major`).
  - `mk/20-dev.mk`: Devcontainer orchestration, Conan dependency resolution using centralized profile, debug compilation, and cleaning.
  - `mk/30-test.mk`: Granular CTest execution with test label filtering (`unit`, `integration`), coverage hooks, and file-watching test cycles (`test-watch`).
  - `mk/40-lint.mk`: Code formatting (`clang-format`), static analysis (`clang-tidy`), and Lua script linting (`lint-lua`).
  - `mk/50-staging.mk`: Staging container lifecycles (`staging-up`, `staging-down`, `staging-logs`) and smoke verification.
  - `mk/60-prod.mk`: Multi-stage production Docker image generation (`docker/Dockerfile.prod`), binary artifact strip checks, and version verification.
- **Single Source of Truth Versioning**: Introduced root `VERSION` file (starting at `0.1.0`), integrated directly into CMake (`PROJECT_VERSION`), passed to C++ binaries via compile definitions (`EVML_VERSION_STRING` and `EVML_GIT_COMMIT`), and displayed in CLI output (`--version`).
- **Centralized Conan Profiles**: Replaced inline profile generation with a committed profile under `conan/profiles/dev-linux-x86_64`.
- **Granular Test Categorization**: Tagged C++ test targets in CMake with `LABELS "unit"` or `LABELS "integration"`, enabling selective test suite execution via `make test-unit` and `make test-integration`.

## Capabilities

### New Capabilities
- `build-and-packaging`: Defines requirements for build orchestration, semantic version derivation, dependency management via Conan, categorized CTest suite execution, and multi-stage container image creation.

### Modified Capabilities
*(None - initial specification)*

## Impact

- **Build System**: Root `Makefile` now acts as a thin dispatcher including `mk/*.mk`.
- **CMake**: `CMakeLists.txt` reads root `VERSION` dynamically and exposes compile definitions to `evm_listener`.
- **C++ CLI**: `evm_listener --version` now reports the canonical semantic version and current git commit describe hash.
- **Developer Workflow**: Developers run `make build`, `make test`, `make test-unit`, `make test-integration`, `make lint-lua`, and `make bump-*` through standardized targets.
