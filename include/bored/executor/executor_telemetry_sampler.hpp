#pragma once

#include "bored/executor/executor_telemetry.hpp"

#include <string>

namespace bored::storage {
class StorageTelemetryRegistry;
}

namespace bored::executor {

class ExecutorTelemetrySampler final {
public:
    ExecutorTelemetrySampler() = default;
    ExecutorTelemetrySampler(storage::StorageTelemetryRegistry* registry,
                             std::string identifier,
                             ExecutorTelemetry* telemetry);
    ~ExecutorTelemetrySampler();

    ExecutorTelemetrySampler(const ExecutorTelemetrySampler&) = delete;
    ExecutorTelemetrySampler& operator=(const ExecutorTelemetrySampler&) = delete;

    ExecutorTelemetrySampler(ExecutorTelemetrySampler&& other) noexcept;
    ExecutorTelemetrySampler& operator=(ExecutorTelemetrySampler&& other) noexcept;

    void register_sampler(storage::StorageTelemetryRegistry* registry,
                          std::string identifier,
                          ExecutorTelemetry* telemetry);
    void unregister();

    [[nodiscard]] bool registered() const noexcept;

private:
    storage::StorageTelemetryRegistry* registry_ = nullptr;
    ExecutorTelemetry* telemetry_ = nullptr;
    std::string identifier_{};
};

}  // namespace bored::executor
