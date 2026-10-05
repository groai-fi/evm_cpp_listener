find_package(Catch2 3 REQUIRED CONFIG)
include(CTest)
include(Catch)

add_executable(evml_tests
    toolchain_smoke_test.cpp
)
target_link_libraries(evml_tests PRIVATE evml_core Catch2::Catch2WithMain)

catch_discover_tests(evml_tests PROPERTIES LABELS unit)
