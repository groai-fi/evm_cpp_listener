# Tasks

## 1. Production Configuration Update

- [x] 1.1 Update `[sink].consumer_groups` in `evm_listener/config/base.toml` to register `{ stream = "events", group = "cg_parquet_archiver" }` alongside `strat:aero_eurcusdc:botB_v1` using multiline array formatting

## 2. Unit Test Synchronization

- [x] 2.1 Update `evm_listener/tests/config_test.cpp` to assert `c.sink.consumer_groups.size() == 2` and verify both `strat:aero_eurcusdc:botB_v1` and `cg_parquet_archiver` have `StreamKind::Events`
- [x] 2.2 Update `mutate()` call in `config_test.cpp:98` to match the exact multiline `consumer_groups` array in `base.toml`
- [x] 2.3 Update `mutate()` in `config_test.cpp:352` to match the multiline array for the negative array test, and update line 357 to assert `c.sink.consumer_groups.size() == 3`

## 3. Verification & Git Policy Compliance

- [x] 3.1 Execute Catch2 unit tests `./build/Debug/evm_listener/tests/evml_tests "[unit][config]"` and verify all 18 test cases and 300+ assertions pass cleanly
- [x] 3.2 Run full unit test suite `make test-unit` to confirm zero regressions
- [x] 3.3 Validate OpenSpec change structure with `openspec status --change "register-cg-parquet-archiver"`
- [x] 3.4 Confirm all changes remain uncommitted in the working tree and output conventional commit recommendation
