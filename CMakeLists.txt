cmake_minimum_required(VERSION 3.21)

project(bored VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(${PROJECT_NAME}_lib
    src/greeter.cpp
    src/ddl/ddl_errors.cpp
    src/ddl/ddl_validation.cpp
    src/ddl/ddl_telemetry.cpp
    src/ddl/ddl_dispatcher.cpp
    src/ddl/ddl_dependency_graph.cpp
    src/ddl/ddl_handlers.cpp
    src/ddl/parser_diagnostics.cpp
    src/catalog/catalog_bootstrapper.cpp
    src/catalog/catalog_encoding.cpp
    src/catalog/catalog_ddl.cpp
    src/catalog/catalog_cache.cpp
    src/catalog/catalog_telemetry.cpp
    src/catalog/catalog_checkpoint_registry.cpp
    src/catalog/catalog_mutator.cpp
    src/catalog/catalog_accessor.cpp
    src/catalog/catalog_transaction.cpp
    src/storage/checksum.cpp
    src/storage/async_io.cpp
    src/storage/page_operations.cpp
    src/storage/free_space_map.cpp
    src/storage/checkpoint_manager.cpp
    src/storage/checkpoint_scheduler.cpp
    src/storage/wal_payloads.cpp
    src/storage/page_manager.cpp
    src/storage/lock_manager.cpp
    src/storage/free_space_map_persistence.cpp
    src/storage/wal_apply_helpers.cpp
    src/storage/wal_writer.cpp
    src/storage/wal_reader.cpp
    src/storage/wal_recovery.cpp
    src/storage/wal_undo_walker.cpp
    src/storage/wal_replayer.cpp
    src/storage/wal_retention.cpp
    src/storage/wal_telemetry_registry.cpp
    src/storage/storage_telemetry_registry.cpp
    src/storage/storage_diagnostics.cpp
    src/storage/vacuum_scheduler.cpp
    src/storage/vacuum_background_loop.cpp
    src/storage/vacuum_worker.cpp
    src/planner/logical_plan.cpp
    src/planner/physical_plan.cpp
    src/planner/memo.cpp
    src/planner/cost_model.cpp
    src/planner/statistics_catalog.cpp
    src/planner/planner_context.cpp
    src/planner/planner.cpp
    src/planner/planner_telemetry.cpp
    src/planner/rule.cpp
    src/planner/rules/join_rules.cpp
    src/planner/rules/predicate_pushdown_rule.cpp
    src/txn/transaction_manager.cpp
    src/txn/wal_commit_pipeline.cpp
    src/txn/persistent_transaction_id_allocator.cpp
)

option(BORED_ENABLE_PARSER "Build parser front-end components" ON)

if(BORED_ENABLE_PARSER)
    find_package(pegtl CONFIG REQUIRED)
    target_sources(${PROJECT_NAME}_lib
        PRIVATE
            src/parser/grammar.cpp
            src/parser/ddl_command_builder.cpp
            src/parser/parser_telemetry.cpp
            src/parser/ddl_script_executor.cpp
            src/parser/relational_ast.cpp
            src/parser/relational_binder.cpp
            src/parser/relational_catalog_binder_adapter.cpp
            src/parser/relational_logical_lowering.cpp
            src/parser/relational_logical_normalization.cpp
        src/parser/relational_logical_plan_dump.cpp
        src/parser/relational_logical_plan_printer.cpp
    )
    target_link_libraries(${PROJECT_NAME}_lib
        PUBLIC
            taocpp::pegtl
    )
    target_compile_definitions(${PROJECT_NAME}_lib
        PUBLIC
            BORED_ENABLE_PARSER=1
    )
else()
    target_compile_definitions(${PROJECT_NAME}_lib
        PUBLIC
            BORED_ENABLE_PARSER=0
    )
endif()

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
option(BORED_ENABLE_BENCHMARKS "Build storage benchmarking harness" ON)

if(BORED_ENABLE_TESTS)
    enable_testing()

    find_package(Catch2 3 REQUIRED CONFIG)

    add_executable(${PROJECT_NAME}_tests
        tests/main.cpp
        tests/storage_format_tests.cpp
        tests/async_io_tests.cpp
        tests/async_io_ioring_tests.cpp
        tests/catalog_bootstrap_tests.cpp
        tests/catalog_ddl_tests.cpp
        tests/catalog_cache_tests.cpp
    tests/catalog_mutator_tests.cpp
    tests/catalog_checkpoint_registry_tests.cpp
        tests/catalog_mvcc_tests.cpp
        tests/catalog_integration_tests.cpp
    tests/transaction_abort_integration_tests.cpp
        tests/transaction_crash_recovery_integration_tests.cpp
        tests/ddl_validation_tests.cpp
        tests/ddl_dispatcher_tests.cpp
    tests/ddl_handlers_tests.cpp
        tests/page_manager_tests.cpp
        tests/free_space_map_tests.cpp
        tests/wal_writer_tests.cpp
        tests/wal_reader_tests.cpp
        tests/wal_recovery_tests.cpp
        tests/wal_replay_tests.cpp
        tests/wal_telemetry_tests.cpp
        tests/wal_checkpoint_tests.cpp
        tests/checkpoint_scheduler_tests.cpp
        tests/lock_manager_tests.cpp
        tests/storage_telemetry_tests.cpp
        tests/storage_diagnostics_tests.cpp
        tests/vacuum_scheduler_tests.cpp
    tests/vacuum_background_loop_tests.cpp
        tests/vacuum_worker_tests.cpp
        tests/planner_scaffolding_tests.cpp
        tests/planner_statistics_catalog_tests.cpp
        tests/planner_cost_model_tests.cpp
    tests/planner_rule_tests.cpp
        tests/planner_integration_tests.cpp
        tests/planner_telemetry_tests.cpp
    tests/transaction_manager_tests.cpp
    tests/wal_commit_pipeline_tests.cpp
    )

    target_link_libraries(${PROJECT_NAME}_tests
        PRIVATE
            ${PROJECT_NAME}_lib
            Catch2::Catch2WithMain
    )

    if(BORED_ENABLE_PARSER)
        target_sources(${PROJECT_NAME}_tests
            PRIVATE
                tests/parser_smoke_tests.cpp
                tests/parser_ddl_database_tests.cpp
                tests/parser_ddl_schema_tests.cpp
                tests/parser_ddl_table_tests.cpp
                tests/parser_ddl_script_tests.cpp
                tests/parser_ddl_view_tests.cpp
                tests/parser_ddl_command_builder_tests.cpp
                tests/parser_expression_primitives_tests.cpp
                tests/parser_diagnostics_tests.cpp
                tests/parser_telemetry_tests.cpp
                tests/parser_ddl_integration_tests.cpp
                tests/parser_select_tests.cpp
                tests/parser_relational_ast_tests.cpp
                tests/parser_binding_tests.cpp
                tests/parser_logical_plan_tests.cpp
                tests/parser_logical_lowering_tests.cpp
                tests/parser_logical_normalization_tests.cpp
                tests/parser_logical_plan_printer_tests.cpp
        )
    endif()

    include(CTest)
    include(Catch)
    catch_discover_tests(${PROJECT_NAME}_tests)
endif()

if(BORED_ENABLE_BENCHMARKS)
    add_executable(${PROJECT_NAME}_benchmarks
        benchmarks/storage_benchmarks.cpp
    )

    target_link_libraries(${PROJECT_NAME}_benchmarks
        PRIVATE
            ${PROJECT_NAME}_lib
    )

    target_compile_features(${PROJECT_NAME}_benchmarks
        PUBLIC
            cxx_std_20
    )

    if(BORED_ENABLE_PARSER)
        add_executable(${PROJECT_NAME}_parser_benchmarks
            benchmarks/parser_benchmarks.cpp
        )

        target_link_libraries(${PROJECT_NAME}_parser_benchmarks
            PRIVATE
                ${PROJECT_NAME}_lib
        )

        target_compile_features(${PROJECT_NAME}_parser_benchmarks
            PUBLIC
                cxx_std_20
        )
    endif()
endif()
