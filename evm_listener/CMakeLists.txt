cmake_minimum_required(VERSION 3.20)
project(evm_listener VERSION 0.1.0 LANGUAGES CXX)

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
        message(FATAL_ERROR "git describe failed (rc=${EVML_GIT_RC}): ${EVML_GIT_ERR}. Pass -DEVML_GIT_DESCRIBE=<version> explicitly.")
    endif()
    set(EVML_GIT_DESCRIBE "${EVML_GIT_DESCRIBE_OUT}")
endif()
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/src/core/version.hpp.in
               ${CMAKE_CURRENT_BINARY_DIR}/generated/core/version.hpp @ONLY)

option(EVML_SANITIZE "Build with ASan+UBSan" ON)

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
    src/pipeline/event.cpp
    src/pipeline/log_validation.cpp
    src/app/pool_config.cpp
    src/app/tools.cpp
)
target_include_directories(evml_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}/generated)
target_compile_features(evml_core PUBLIC cxx_std_20)
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
