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
)
target_link_libraries(evml_tests PRIVATE evml_core Catch2::Catch2WithMain)
target_compile_definitions(evml_tests PRIVATE
    EVML_CONFIG_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../config"
    EVML_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/fixtures")

catch_discover_tests(evml_tests PROPERTIES LABELS unit)
