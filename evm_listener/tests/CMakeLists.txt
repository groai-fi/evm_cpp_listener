find_package(Catch2 3 REQUIRED CONFIG)
include(CTest)
include(Catch)

add_executable(evml_tests
    toolchain_smoke_test.cpp
    env_test.cpp
    hex_test.cpp
    keccak_test.cpp
    config_test.cpp
    codec_test.cpp
    log_test.cpp
    backoff_test.cpp
    classify_test.cpp
    http_rpc_client_test.cpp
    mock_http_server.cpp
    ws_session_test.cpp
    mock_ws_server.cpp
    provider_book_test.cpp
    stale_detector_test.cpp
    provider_pool_test.cpp
    pool_config_test.cpp
    pool_log_source_test.cpp
    token_bucket_test.cpp
    finality_gate_test.cpp
    reorg_detector_test.cpp
    aimd_pager_test.cpp
    pending_buffer_test.cpp
    final_diff_test.cpp
    sweeper_test.cpp
    coverage_test.cpp
    pipeline_driver_test.cpp
    pipeline_property_test.cpp
    event_test.cpp
    spill_test.cpp
    redis_scripts_test.cpp
    redis_sink_test.cpp
    redis_sink_integration_test.cpp
    redis_startup_test.cpp
    consumer_lag_test.cpp
    watchdog_test.cpp
    metrics_test.cpp
    ops_http_server_test.cpp
    http_client_test.cpp
)
target_link_libraries(evml_tests PRIVATE evml_core Catch2::Catch2WithMain)
target_compile_definitions(evml_tests PRIVATE
    EVML_CONFIG_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../config"
    EVML_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/fixtures"
    EVML_EXPECTED_VERSION="${EVML_VERSION_STRING}")

# Tests tagged [integration] use in-process mock servers on 127.0.0.1 or the compose Redis (REDIS_URL); everything
# else is a pure unit test. [redis_serial] tests change server-wide Redis state (maxmemory, SCRIPT FLUSH) and run alone.
catch_discover_tests(evml_tests TEST_SPEC "~[integration]" PROPERTIES LABELS unit)
catch_discover_tests(evml_tests TEST_SPEC "[integration]~[redis_serial]" PROPERTIES LABELS integration)
catch_discover_tests(evml_tests TEST_SPEC "[redis_serial]" PROPERTIES LABELS integration RUN_SERIAL TRUE)
