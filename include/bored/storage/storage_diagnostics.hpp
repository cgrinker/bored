#pragma once

#include "bored/ddl/ddl_telemetry.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/planner/planner_telemetry.hpp"
#include "bored/storage/storage_telemetry_registry.hpp"

#include <chrono>
#include <string>
#include <vector>

namespace bored::storage {

struct StorageDiagnosticsOptions final {
    bool include_page_manager_details = true;
    bool include_checkpoint_details = true;
    bool include_retention_details = true;
    bool include_recovery_details = true;
    bool include_index_retention_details = true;
    bool include_index_details = true;
    bool include_temp_cleanup_details = true;
    bool include_durability_details = true;
    bool include_vacuum_details = true;
    bool include_catalog_details = true;
    bool include_ddl_details = true;
    bool include_parser_details = true;
    bool include_transaction_details = true;
    bool include_planner_details = true;
    bool include_executor_details = true;
};

struct StorageDiagnosticsPageManagerEntry final {
    std::string identifier;
    PageManagerTelemetrySnapshot snapshot;
};

struct StorageDiagnosticsCheckpointEntry final {
    std::string identifier;
    CheckpointTelemetrySnapshot snapshot;
};

struct StorageDiagnosticsRetentionEntry final {
    std::string identifier;
    WalRetentionTelemetrySnapshot snapshot;
};

struct StorageDiagnosticsRecoveryEntry final {
    std::string identifier;
    RecoveryTelemetrySnapshot snapshot;
};

struct StorageDiagnosticsIndexRetentionEntry final {
    std::string identifier;
    IndexRetentionTelemetrySnapshot snapshot;
};

struct StorageDiagnosticsIndexEntry final {
    std::string identifier;
    IndexTelemetrySnapshot snapshot;
};

struct StorageDiagnosticsTempCleanupEntry final {
    std::string identifier;
    TempCleanupTelemetrySnapshot snapshot;
};

struct StorageDiagnosticsDurabilityEntry final {
    std::string identifier;
    DurabilityTelemetrySnapshot snapshot;
};

struct StorageDiagnosticsVacuumEntry final {
    std::string identifier;
    VacuumTelemetrySnapshot snapshot;
};

struct StorageDiagnosticsCatalogEntry final {
    std::string identifier;
    CatalogTelemetrySnapshot snapshot;
};

struct StorageDiagnosticsDdlEntry final {
    std::string identifier;
    bored::ddl::DdlTelemetrySnapshot snapshot;
};

struct StorageDiagnosticsParserEntry final {
    std::string identifier;
    bored::parser::ParserTelemetrySnapshot snapshot;
};

struct StorageDiagnosticsTransactionEntry final {
    std::string identifier;
    bored::txn::TransactionTelemetrySnapshot snapshot;
};

struct StorageDiagnosticsPlannerEntry final {
    std::string identifier;
    bored::planner::PlannerTelemetrySnapshot snapshot;
};

struct StorageDiagnosticsExecutorEntry final {
    std::string identifier;
    bored::executor::ExecutorTelemetrySnapshot snapshot;
};

struct StorageDiagnosticsPageManagerSection final {
    PageManagerTelemetrySnapshot total{};
    std::vector<StorageDiagnosticsPageManagerEntry> details{};
};

struct StorageDiagnosticsCheckpointSection final {
    CheckpointTelemetrySnapshot total{};
    std::vector<StorageDiagnosticsCheckpointEntry> details{};
};

struct StorageDiagnosticsRetentionSection final {
    WalRetentionTelemetrySnapshot total{};
    std::vector<StorageDiagnosticsRetentionEntry> details{};
};

struct StorageDiagnosticsRecoverySection final {
    RecoveryTelemetrySnapshot total{};
    std::vector<StorageDiagnosticsRecoveryEntry> details{};
};

struct StorageDiagnosticsIndexRetentionSection final {
    IndexRetentionTelemetrySnapshot total{};
    std::vector<StorageDiagnosticsIndexRetentionEntry> details{};
};

struct StorageDiagnosticsIndexSection final {
    IndexTelemetrySnapshot total{};
    std::vector<StorageDiagnosticsIndexEntry> details{};
};

struct StorageDiagnosticsTempCleanupSection final {
    TempCleanupTelemetrySnapshot total{};
    std::vector<StorageDiagnosticsTempCleanupEntry> details{};
};

struct StorageDiagnosticsDurabilitySection final {
    DurabilityTelemetrySnapshot total{};
    std::vector<StorageDiagnosticsDurabilityEntry> details{};
};

struct StorageDiagnosticsVacuumSection final {
    VacuumTelemetrySnapshot total{};
    std::vector<StorageDiagnosticsVacuumEntry> details{};
};

struct StorageDiagnosticsCatalogSection final {
    CatalogTelemetrySnapshot total{};
    std::vector<StorageDiagnosticsCatalogEntry> details{};
};

struct StorageDiagnosticsDdlSection final {
    bored::ddl::DdlTelemetrySnapshot total{};
    std::vector<StorageDiagnosticsDdlEntry> details{};
};

struct StorageDiagnosticsParserSection final {
    bored::parser::ParserTelemetrySnapshot total{};
    std::vector<StorageDiagnosticsParserEntry> details{};
};

struct StorageDiagnosticsTransactionSection final {
    bored::txn::TransactionTelemetrySnapshot total{};
    std::vector<StorageDiagnosticsTransactionEntry> details{};
};

struct StorageDiagnosticsPlannerSection final {
    bored::planner::PlannerTelemetrySnapshot total{};
    std::vector<StorageDiagnosticsPlannerEntry> details{};
};

struct StorageDiagnosticsExecutorSection final {
    bored::executor::ExecutorTelemetrySnapshot total{};
    std::vector<StorageDiagnosticsExecutorEntry> details{};
};

struct StorageDiagnosticsDocument final {
    std::chrono::system_clock::time_point collected_at{};
    std::uint64_t last_checkpoint_lsn = 0U;
    std::uint64_t outstanding_replay_backlog_bytes = 0U;
    StorageDiagnosticsPageManagerSection page_managers{};
    StorageDiagnosticsCheckpointSection checkpoints{};
    StorageDiagnosticsRetentionSection retention{};
    StorageDiagnosticsRecoverySection recovery{};
    StorageDiagnosticsIndexRetentionSection index_retention{};
    StorageDiagnosticsIndexSection indexes{};
    StorageDiagnosticsTempCleanupSection temp_cleanup{};
    StorageDiagnosticsDurabilitySection durability{};
    StorageDiagnosticsVacuumSection vacuum{};
    StorageDiagnosticsCatalogSection catalog{};
    StorageDiagnosticsParserSection parser{};
    StorageDiagnosticsDdlSection ddl{};
    StorageDiagnosticsTransactionSection transactions{};
    StorageDiagnosticsPlannerSection planner{};
    StorageDiagnosticsExecutorSection executors{};
};

StorageDiagnosticsDocument collect_storage_diagnostics(const StorageTelemetryRegistry& registry,
                                                       const StorageDiagnosticsOptions& options = {});

std::string storage_diagnostics_to_json(const StorageDiagnosticsDocument& document);

}  // namespace bored::storage
