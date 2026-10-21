# Tasks: Modular Build, Versioning, and Packaging System

## 1. Modular Makefile Architecture

- [x] 1.1 Create `mk/` directory structure with modular makefiles (`00-vars.mk`, `10-version.mk`, `20-dev.mk`, `30-test.mk`, `40-lint.mk`, `50-staging.mk`, `60-prod.mk`) and verify `make help` outputs the categorized targets.
- [x] 1.2 Refactor the root `Makefile` into a thin dispatcher including `mk/*.mk` and verify clean Make recipe resolution without syntax warnings.

## 2. Single-Source Semantic Versioning

- [x] 2.1 Create root `VERSION` file (`0.1.1`) and implement SemVer increment targets (`make version`, `make bump-patch`, `make bump-minor`, `make bump-major`), verifying that `VERSION` is accurately updated.
- [x] 2.2 Integrate `VERSION` parsing into `CMakeLists.txt` via `file(READ ...)` and pass `EVML_VERSION_STRING` and `EVML_GIT_COMMIT` compile definitions to `evm_listener`.
- [x] 2.3 Verify `evm_listener --version` displays the canonical version along with the short git describe commit hash.

## 3. Test Suite Labeling & Filtering

- [x] 3.1 Tag CTest test targets in `evm_listener/CMakeLists.txt` with `LABELS "unit"` and `LABELS "integration"`.
- [x] 3.2 Wire `make test`, `make test-unit`, and `make test-integration` targets in `mk/30-test.mk` and verify filtered test execution with CTest.

## 4. Centralized Dependencies & Packaging

- [x] 4.1 Define centralized Conan profile in `conan/profiles/dev-linux-x86_64` and bind `make conan` to utilize this profile.
- [x] 4.2 Standardize staging and production container recipes in `mk/50-staging.mk` and `mk/60-prod.mk` and verify image build recipes.

## 5. Code Quality & Linting

- [x] 5.1 Implement `make lint-lua` to run `luacheck` against scripts in `evm_listener/lua/` and verify clean exit status.
- [x] 5.2 Implement `make format` and `make lint` targets for C++ sources using `clang-format` and `clang-tidy`.
