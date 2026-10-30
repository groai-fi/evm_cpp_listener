# Spec Delta

## ADDED Requirements

### Requirement: Locked Conan Dependency Revisions
The repository SHALL include a committed `conan.lock` lockfile pinning exact recipe revisions and timestamps for all direct and transitive Conan dependencies across `Debug`, `Release`, and `RelWithDebInfo` profiles.

#### Scenario: Installing dependencies with conan.lock present
- **WHEN** `conan install .` is executed for either `conan/profiles/debug` or `conan/profiles/release`
- **THEN** Conan SHALL resolve all direct and build requirements against the committed `conan.lock` without floating to newer upstream recipe revisions

### Requirement: Strict C++ Compiler Warning Enforcement
All C++ build targets (`evml_core`, `evm_listener`, and `evml_tests`) SHALL compile with `-Wall`, `-Wextra`, `-Wpedantic`, `-Wconversion`, `-Wimplicit-fallthrough`, `-Wformat=2`, `-Wnon-virtual-dtor`, `-Woverloaded-virtual`, and `-Werror` enabled across both Debug and Release configurations.

#### Scenario: Implicit narrowing conversion or virtual signature mismatch introduced
- **WHEN** a source or test file introduces an implicit narrowing conversion or overloaded virtual mismatch and `make build` is executed
- **THEN** compilation SHALL fail with a compiler error under `-Werror`

