# Spec Delta

## ADDED Requirements

### Requirement: Locked Conan Dependency Revisions
The repository SHALL include a committed `conan.lock` lockfile pinning exact recipe revisions and timestamps for all direct and transitive Conan dependencies across `Debug`, `Release`, and `RelWithDebInfo` profiles, and all `conan install` invocations in `docker/Dockerfile.prod` (with mandatory `COPY VERSION conanfile.py conan.lock ./`), `.github/workflows/ci.yml`, `mk/10-conan.mk`, and `mk/20-build.mk` SHALL enforce it via `--lockfile=conan.lock`.

#### Scenario: Installing dependencies with conan.lock enforced
- **WHEN** `conan install . --lockfile=conan.lock` is executed via Make, CI, or `docker/Dockerfile.prod` for either `conan/profiles/debug` or `conan/profiles/release`
- **THEN** Conan SHALL resolve all direct and build requirements strictly against the committed `conan.lock` and fail fast if `conan.lock` is missing or out of sync with `conanfile.py`

### Requirement: Strict C++ Compiler Warning Enforcement
All C++ build targets (`evml_core`, `evm_listener`, and `evml_tests`) SHALL compile with `-Wall`, `-Wextra`, `-Wpedantic`, `-Wconversion`, `-Wimplicit-fallthrough`, `-Wformat=2`, `-Wnon-virtual-dtor`, `-Woverloaded-virtual`, and `-Werror` enabled across both Debug and Release configurations.

#### Scenario: Implicit narrowing conversion or virtual signature mismatch introduced
- **WHEN** a source or test file introduces an implicit narrowing conversion or overloaded virtual mismatch and `make build` is executed
- **THEN** compilation SHALL fail with a compiler error under `-Werror`

