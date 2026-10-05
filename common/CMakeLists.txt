cmake_minimum_required(VERSION 3.10)
set(ProjectName common)
project(common VERSION 0.1)

add_library(
        ${ProjectName}
        src/common.cpp
        src/redis_client_base.cpp
        src/redis_kv_client.cpp
        src/redis_stream_client.cpp
        src/market_data_service.cpp
        src/ohlc_generator.cpp
        include/utils.h
        include/market_data.h
        include/market_spec.h
        include/redis_client_base.h
        include/redis_kv_client.h
        include/redis_stream_client.h
        include/market_data_service.h
)

# pthread
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Find required packages
find_package(PkgConfig REQUIRED)
pkg_check_modules(HIREDIS REQUIRED hiredis)
pkg_check_modules(UUID REQUIRED uuid)

# Find nlohmann/json
find_package(nlohmann_json REQUIRED)

# Include directories
include_directories(${HIREDIS_INCLUDE_DIRS})

target_include_directories(${ProjectName} PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
        ${HIREDIS_INCLUDE_DIRS}
)

# Compiler flags
target_compile_options(${ProjectName} PRIVATE
        ${HIREDIS_CFLAGS_OTHER}
)

# Set C++ standard
set_target_properties(${ProjectName} PROPERTIES
        CXX_STANDARD 17
        CXX_STANDARD_REQUIRED ON
)

target_link_libraries(${ProjectName}
        Threads::Threads
        ${HIREDIS_LIBRARIES}
        ${UUID_LIBRARIES}
        nlohmann_json::nlohmann_json
)

# ========== TEST EXECUTABLES ==========
# Option to build tests (can be disabled with -DBUILD_TESTS=OFF)
option(BUILD_TESTS "Build test executables" ON)

if(BUILD_TESTS)
    # Test for RedisClient
    add_executable(test_redis_client
            tests/test_redis_client.cpp
    )

    target_link_libraries(test_redis_client
            ${ProjectName}
    )

    # Test for MarketDataService
    add_executable(test_market_data
            tests/test_market_data.cpp
    )

    target_link_libraries(test_market_data
            ${ProjectName}
    )

    # Enable testing
    enable_testing()
    add_test(NAME RedisClientTest COMMAND test_redis_client)
    add_test(NAME MarketDataTest COMMAND test_market_data)
endif()

# ========== INSTALLATION ==========
# Install targets
install(TARGETS ${ProjectName}
        EXPORT ${ProjectName}Targets
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        INCLUDES DESTINATION include
)

# Install headers
install(FILES
        include/utils.h
        include/types.h
        include/market_data.h
        include/market_spec.h
        include/redis_client_base.h
        include/redis_kv_client.h
        include/redis_stream_client.h
        include/market_data_service.h
        DESTINATION include
)

# Export targets
install(EXPORT ${ProjectName}Targets
        FILE ${ProjectName}Targets.cmake
        NAMESPACE ${ProjectName}::
        DESTINATION lib/cmake/${ProjectName}
)

# Create config file
include(CMakePackageConfigHelpers)
write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/${ProjectName}ConfigVersion.cmake"
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY AnyNewerVersion
)

configure_package_config_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/${ProjectName}Config.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/${ProjectName}Config.cmake"
        INSTALL_DESTINATION lib/cmake/${ProjectName}
)

install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/${ProjectName}Config.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/${ProjectName}ConfigVersion.cmake"
        DESTINATION lib/cmake/${ProjectName}
)
