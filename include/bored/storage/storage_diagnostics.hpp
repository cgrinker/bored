#pragma once

#include "bored/storage/storage_telemetry_registry.hpp"

#include <chrono>
#include <string>
#include <vector>

namespace bored::storage {

struct StorageDiagnosticsOptions final {
    bool include_page_manager_details = true;
    bool include_checkpoint_details = true;
    bool include_retention_details = true;
    bool include_catalog_details = true;
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

struct StorageDiagnosticsCatalogEntry final {
    std::string identifier;
    CatalogTelemetrySnapshot snapshot;
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

struct StorageDiagnosticsCatalogSection final {
    CatalogTelemetrySnapshot total{};
    std::vector<StorageDiagnosticsCatalogEntry> details{};
};

struct StorageDiagnosticsDocument final {
    std::chrono::system_clock::time_point collected_at{};
    StorageDiagnosticsPageManagerSection page_managers{};
    StorageDiagnosticsCheckpointSection checkpoints{};
    StorageDiagnosticsRetentionSection retention{};
    StorageDiagnosticsCatalogSection catalog{};
};

StorageDiagnosticsDocument collect_storage_diagnostics(const StorageTelemetryRegistry& registry,
                                                       const StorageDiagnosticsOptions& options = {});

std::string storage_diagnostics_to_json(const StorageDiagnosticsDocument& document);

}  // namespace bored::storage
