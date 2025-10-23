cmake_minimum_required(VERSION 3.21)

project(bored VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(${PROJECT_NAME}_lib
    src/greeter.cpp
    src/storage/checksum.cpp
    src/storage/async_io.cpp
    src/storage/page_operations.cpp
    src/storage/free_space_map.cpp
    src/storage/checkpoint_manager.cpp
    src/storage/wal_payloads.cpp
    src/storage/page_manager.cpp
    src/storage/lock_manager.cpp
    src/storage/free_space_map_persistence.cpp
    src/storage/wal_writer.cpp
    src/storage/wal_reader.cpp
    src/storage/wal_recovery.cpp
    src/storage/wal_replayer.cpp
    src/storage/wal_retention.cpp
    src/storage/wal_telemetry_registry.cpp
)

target_include_directories(${PROJECT_NAME}_lib
    PUBLIC
        include
)

target_compile_features(${PROJECT_NAME}_lib
    PUBLIC
        cxx_std_20
)

add_executable(${PROJECT_NAME}
    src/main.cpp
)

target_link_libraries(${PROJECT_NAME}
    PRIVATE
        ${PROJECT_NAME}_lib
)

option(BORED_ENABLE_TESTS "Build and enable tests" ON)

if(BORED_ENABLE_TESTS)
    enable_testing()

    find_package(Catch2 3 REQUIRED CONFIG)

    add_executable(${PROJECT_NAME}_tests
        tests/main.cpp
        tests/storage_format_tests.cpp
        tests/async_io_tests.cpp
        tests/async_io_ioring_tests.cpp
        tests/page_manager_tests.cpp
        tests/free_space_map_tests.cpp
        tests/wal_writer_tests.cpp
        tests/wal_reader_tests.cpp
        tests/wal_recovery_tests.cpp
        tests/wal_replay_tests.cpp
        tests/wal_telemetry_tests.cpp
        tests/wal_checkpoint_tests.cpp
    tests/lock_manager_tests.cpp
    )

    target_link_libraries(${PROJECT_NAME}_tests
        PRIVATE
            ${PROJECT_NAME}_lib
            Catch2::Catch2WithMain
    )

    include(CTest)
    include(Catch)
    catch_discover_tests(${PROJECT_NAME}_tests)
endif()
