cmake_minimum_required(VERSION 3.20)

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/VERSION")
    file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/VERSION" EVML_VERSION_RAW)
    string(STRIP "${EVML_VERSION_RAW}" EVML_VERSION_STRING)
else()
    set(EVML_VERSION_STRING "0.1.0")
endif()

project(evm_cpp_listener VERSION ${EVML_VERSION_STRING} LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(EVML_BUILD_TESTS "Build unit/integration tests" ON)
if(EVML_BUILD_TESTS)
    enable_testing()
endif()

set(SubProjects
        evm_listener
)

foreach(Project IN LISTS SubProjects)
    # Check because in railway, not all folder is up
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${Project}")
        include_directories(${Project}/src)
        include_directories(${Project}/include)
        if (NOT TARGET ${Project})
            add_subdirectory(${Project})
        endif()
    endif()
endforeach()