# Spec Delta

## ADDED Requirements

### Requirement: Strict C++20 Standard and Clang-Tidy Enforcement
The build system SHALL enforce `-std=c++20` with compiler extensions disabled (`CXX_EXTENSIONS OFF`) across all C++ targets and provide a `clang-tidy` verification target checking `bugprone-exception-escape`, `bugprone-use-after-move`, and `bugprone-dangling-handle` with warnings treated as errors.

#### Scenario: Running clang-tidy verification across C++ sources
- **WHEN** `make lint-cpp` (or `clang-tidy` in CI) is executed against `evm_listener/src/`
- **THEN** `clang-tidy` verifies all translation units using `.clang-tidy` and exits non-zero if any enabled guideline or `bugprone-*` check is violated.

### Requirement: Address, Undefined Behavior, and Thread Sanitizer Verification
The build and CI system SHALL support and verify `ASan + UBSan` (`-fsanitize=address,undefined`) in Debug builds and `ThreadSanitizer` (`-fsanitize=thread`) via dedicated sanitizer targets so memory leaks, reference cycles, undefined behavior, and data races fail the test suite.

#### Scenario: Running sanitizer test suites
- **WHEN** `make test-asan` or `make test-tsan` is executed
- **THEN** `evml_tests` runs under the respective sanitizer instrumentation and exits `0` with zero leak, undefined-behavior, or data-race reports.
