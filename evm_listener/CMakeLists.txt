cmake_minimum_required(VERSION 3.20)
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/../VERSION")
    file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/../VERSION" EVML_VERSION_RAW)
    string(STRIP "${EVML_VERSION_RAW}" EVML_VERSION_STRING)
elseif(DEFINED PROJECT_VERSION)
    set(EVML_VERSION_STRING "${PROJECT_VERSION}")
else()
    set(EVML_VERSION_STRING "0.1.0")
endif()

project(evm_listener VERSION ${EVML_VERSION_STRING} LANGUAGES CXX)

# Version string: `git describe`, overridable (e.g. Docker/Railway builds without .git) via -DEVML_GIT_DESCRIBE=...
set(EVML_GIT_DESCRIBE "" CACHE STRING "Override for git describe output")
if(NOT EVML_GIT_DESCRIBE)
    execute_process(
        COMMAND git describe --always --dirty --tags
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        OUTPUT_VARIABLE EVML_GIT_DESCRIBE_OUT
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE EVML_GIT_RC
        ERROR_VARIABLE EVML_GIT_ERR)
    if(NOT EVML_GIT_RC EQUAL 0 OR NOT EVML_GIT_DESCRIBE_OUT)
        set(EVML_GIT_DESCRIBE "v${EVML_VERSION_STRING}")
    else()
        set(EVML_GIT_DESCRIBE "${EVML_GIT_DESCRIBE_OUT}")
    endif()
endif()
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/src/core/version.hpp.in
               ${CMAKE_CURRENT_BINARY_DIR}/generated/core/version.hpp @ONLY)

# Redis Lua scripts: src/sink/lua/<name>.lua are embedded into generated/sink/lua_sources.hpp (no runtime loading).
# Editing a .lua re-runs configure (CMAKE_CONFIGURE_DEPENDS), which regenerates the header and rebuilds the binary.
foreach(evml_lua emit ckpt final_trim lease)
    set(evml_lua_path ${CMAKE_CURRENT_SOURCE_DIR}/src/sink/lua/${evml_lua}.lua)
    set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${evml_lua_path})
    file(READ ${evml_lua_path} evml_lua_src)
    if(NOT evml_lua_src MATCHES "^#!lua\n")
        message(FATAL_ERROR "${evml_lua_path}: must start with '#!lua' followed by LF (Redis runs it in legacy mode otherwise)")
    endif()
    string(FIND "${evml_lua_src}" ")lua\"" evml_lua_bad)
    if(NOT evml_lua_bad EQUAL -1)
        message(FATAL_ERROR "${evml_lua_path}: contains ')lua\"', which ends the C++ raw string it is embedded in")
    endif()
    string(FIND "${evml_lua_src}" "\r" evml_lua_cr)
    if(NOT evml_lua_cr EQUAL -1)
        message(FATAL_ERROR "${evml_lua_path}: contains a CR character; use LF line endings")
    endif()
    string(TOUPPER ${evml_lua} evml_lua_upper)
    set(EVML_LUA_${evml_lua_upper} "${evml_lua_src}")
endforeach()
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/src/sink/lua_sources.hpp.in
               ${CMAKE_CURRENT_BINARY_DIR}/generated/sink/lua_sources.hpp @ONLY)

option(EVML_SANITIZE "Build with ASan+UBSan" ON)
# Test-only CLI hooks (--test-freeze-io-after). On for Debug (dev/test) builds, off for Release (the prod image).
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(EVML_TEST_HOOKS_DEFAULT ON)
else()
    set(EVML_TEST_HOOKS_DEFAULT OFF)
endif()
option(EVML_TEST_HOOKS "Compile test-only CLI hooks" ${EVML_TEST_HOOKS_DEFAULT})

find_package(Boost REQUIRED CONFIG COMPONENTS json)
find_package(OpenSSL REQUIRED)
find_package(simdjson REQUIRED CONFIG)
find_package(ethash REQUIRED CONFIG)
find_package(redis++ REQUIRED CONFIG)
find_package(tomlplusplus REQUIRED CONFIG)
find_package(spdlog REQUIRED CONFIG)
find_package(prometheus-cpp REQUIRED CONFIG)
find_package(Catch2 3 REQUIRED CONFIG)

add_library(evml_core STATIC
    src/core/version.cpp
    src/core/env.cpp
    src/core/hex.cpp
    src/core/keccak.cpp
    src/core/config.cpp
    src/core/log.cpp
    src/rpc/codec.cpp
    src/rpc/blocking_http.cpp
    src/rpc/backoff.cpp
    src/rpc/classify.cpp
    src/rpc/url.cpp
    src/rpc/http_rpc_client.cpp
    src/rpc/ws_session.cpp
    src/provider/provider_book.cpp
    src/provider/stale_detector.cpp
    src/provider/provider_pool.cpp
    src/chain/finality_gate.cpp
    src/chain/reorg_detector.cpp
    src/pipeline/aimd_pager.cpp
    src/pipeline/pending_buffer.cpp
    src/pipeline/sink.cpp
    src/pipeline/final_diff.cpp
    src/pipeline/sweeper.cpp
    src/pipeline/coverage.cpp
    src/pipeline/pipeline_driver.cpp
    src/pipeline/event.cpp
    src/pipeline/log_validation.cpp
    src/sink/spill.cpp
    src/sink/lua_scripts.cpp
    src/sink/script_runner.cpp
    src/sink/redis_sink.cpp
    src/sink/redispp_conn.cpp
    src/sink/redis_startup.cpp
    src/sink/lease.cpp
    src/sink/consumer_lag.cpp
    src/app/pool_config.cpp
    src/app/pool_log_source.cpp
    src/app/host.cpp
    src/app/tools.cpp
    src/ops/watchdog.cpp
    src/ops/metrics.cpp
    src/ops/ops_http_server.cpp
    src/ops/http_client.cpp
    src/ops/alert_sender.cpp
    src/ops/alerts.cpp
    src/ops/deadman.cpp
    src/ops/snapshot.cpp
)
target_include_directories(evml_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}/generated)
target_compile_features(evml_core PUBLIC cxx_std_20)
if(EVML_TEST_HOOKS)
    target_compile_definitions(evml_core PUBLIC EVML_TEST_HOOKS=1)
endif()
target_link_libraries(evml_core PUBLIC
    Boost::json
    OpenSSL::SSL OpenSSL::Crypto
    simdjson::simdjson
    ethash::ethash
    redis++::redis++_static
    tomlplusplus::tomlplusplus
    spdlog::spdlog
    prometheus-cpp::core)

add_executable(evm_listener src/main.cpp)
target_link_libraries(evm_listener PRIVATE evml_core)

set(EVML_TARGETS evml_core evm_listener)

if(EVML_BUILD_TESTS OR NOT DEFINED EVML_BUILD_TESTS)
    add_subdirectory(tests)
    list(APPEND EVML_TARGETS evml_tests)
endif()

foreach(t IN LISTS EVML_TARGETS)
    target_compile_options(${t} PRIVATE -Wall -Wextra -Wpedantic -Werror)
    if(EVML_SANITIZE AND CMAKE_BUILD_TYPE STREQUAL "Debug")
        target_compile_options(${t} PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
        target_link_options(${t} PRIVATE -fsanitize=address,undefined)
    endif()
endforeach()
