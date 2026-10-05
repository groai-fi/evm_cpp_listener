cmake_minimum_required(VERSION 3.20)
project(evm_cpp_listener)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
# simdjson library
find_package(simdjson QUIET)
pkg_check_modules(UUID REQUIRED uuid)

set(SubProjects
        common
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