#pragma once

#include "bored/storage/storage_telemetry_registry.hpp"

#include <string>

namespace bored::catalog {

storage::CatalogTelemetrySnapshot collect_catalog_telemetry();
void register_catalog_telemetry(storage::StorageTelemetryRegistry& registry, std::string identifier);
void unregister_catalog_telemetry(storage::StorageTelemetryRegistry& registry, const std::string& identifier);

}  // namespace bored::catalog
