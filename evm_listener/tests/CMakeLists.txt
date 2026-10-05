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
)
target_link_libraries(evml_tests PRIVATE evml_core Catch2::Catch2WithMain)
target_compile_definitions(evml_tests PRIVATE
    EVML_CONFIG_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../config"
    EVML_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/fixtures")

# Tests tagged [integration] use in-process mock servers on 127.0.0.1; everything else is a pure unit test.
catch_discover_tests(evml_tests TEST_SPEC "~[integration]" PROPERTIES LABELS unit)
catch_discover_tests(evml_tests TEST_SPEC "[integration]" PROPERTIES LABELS integration)
