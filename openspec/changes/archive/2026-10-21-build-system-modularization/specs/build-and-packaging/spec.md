# Spec Delta: build-and-packaging

## Purpose

Provides a deterministic, modular build, versioning, test orchestration, and multi-stage container packaging workflow for the EVM listener service.

## ADDED Requirements

### Requirement: Modular Make Entrypoint
The build system SHALL provide a modular Make-based interface split into domain-specific modules under `mk/`, orchestrating build variables, versioning, development, testing, linting, staging, and production targets.

#### Scenario: Inspecting available targets
- **WHEN** a user or CI runner executes `make help` or `make`
- **THEN** the system displays a categorized menu of all available targets with brief descriptions.

#### Scenario: Delegating to modular target definitions
- **WHEN** a user invokes a target defined in any `mk/*.mk` module (e.g. `make build` or `make clean`)
- **THEN** Make evaluates the included module rules and executes the recipe without requiring duplicate target declarations.

### Requirement: Single-Source Semantic Versioning
The project SHALL define its release version solely in a root `VERSION` file, read at build time by CMake, embedded into compiled binaries, and incrementable via standardized Make targets.

#### Scenario: Displaying current project version
- **WHEN** `make version` is executed
- **THEN** the system prints the semantic version read from the `VERSION` file.

#### Scenario: Incrementing semantic version components
- **WHEN** `make bump-patch`, `make bump-minor`, or `make bump-major` is executed
- **THEN** the corresponding component in `VERSION` is incremented according to SemVer rules and written back to `VERSION`.

#### Scenario: Binary version reporting with Git metadata
- **WHEN** the compiled `evm_listener` binary is invoked with `--version`
- **THEN** the program prints the version string from `VERSION` along with the short Git commit describe hash.

### Requirement: Categorized Test Execution
The test runner SHALL allow selective execution of test suites based on CTest labels, differentiating unit tests from integration tests.

#### Scenario: Running full test suite
- **WHEN** `make test` is executed
- **THEN** CTest executes both unit and integration test suites and returns non-zero if any test fails.

#### Scenario: Running unit tests only
- **WHEN** `make test-unit` is executed
- **THEN** CTest runs only test targets annotated with the `unit` label.

#### Scenario: Running integration tests only
- **WHEN** `make test-integration` is executed
- **THEN** CTest runs only test targets annotated with the `integration` label.

### Requirement: Containerized Multi-Environment Workflow
The system SHALL provide distinct container workflows for local development, staging verification, and production container builds.

#### Scenario: Local development container execution
- **WHEN** `make dev-up` or `make dev-sh` is executed
- **THEN** Docker Compose starts the development environment container with toolchains mounted.

#### Scenario: Production container packaging
- **WHEN** `make prod-image` is executed
- **THEN** the system builds a stripped, multi-stage production Docker image using `docker/Dockerfile.prod` and tags it with the current project version.

### Requirement: Code Quality and Linting Verification
The build system SHALL provide automated linting targets for all primary codebases, including C++ sources and Lua scripts.

#### Scenario: Executing Lua script linting
- **WHEN** `make lint-lua` is executed
- **THEN** `luacheck` analyzes all Lua scripts under `evm_listener/lua/` and exits non-zero if syntax or lint errors are detected.

#### Scenario: Formatting C++ source files
- **WHEN** `make format` is executed
- **THEN** `clang-format` is run across all C++ headers and source files in `evm_listener/src/`.
