#include "bored/executor/executor_telemetry_sampler.hpp"

#include "bored/storage/storage_telemetry_registry.hpp"

#include <utility>

namespace bored::executor {

ExecutorTelemetrySampler::ExecutorTelemetrySampler(storage::StorageTelemetryRegistry* registry,
                                                   std::string identifier,
                                                   ExecutorTelemetry* telemetry)
{
    register_sampler(registry, std::move(identifier), telemetry);
}

ExecutorTelemetrySampler::~ExecutorTelemetrySampler()
{
    unregister();
}

ExecutorTelemetrySampler::ExecutorTelemetrySampler(ExecutorTelemetrySampler&& other) noexcept
    : registry_{other.registry_}
    , telemetry_{other.telemetry_}
    , identifier_{std::move(other.identifier_)}
{
    other.registry_ = nullptr;
    other.telemetry_ = nullptr;
    other.identifier_.clear();
}

ExecutorTelemetrySampler& ExecutorTelemetrySampler::operator=(ExecutorTelemetrySampler&& other) noexcept
{
    if (this != &other) {
        unregister();
        registry_ = other.registry_;
        telemetry_ = other.telemetry_;
        identifier_ = std::move(other.identifier_);
        other.registry_ = nullptr;
        other.telemetry_ = nullptr;
        other.identifier_.clear();
    }
    return *this;
}

void ExecutorTelemetrySampler::register_sampler(storage::StorageTelemetryRegistry* registry,
                                                std::string identifier,
                                                ExecutorTelemetry* telemetry)
{
    unregister();

    if (registry == nullptr || telemetry == nullptr || identifier.empty()) {
        return;
    }

    registry->register_executor(identifier, [telemetry] {
        return telemetry->snapshot();
    });

    registry_ = registry;
    telemetry_ = telemetry;
    identifier_ = std::move(identifier);
}

void ExecutorTelemetrySampler::unregister()
{
    if (registry_ != nullptr && !identifier_.empty()) {
        registry_->unregister_executor(identifier_);
    }
    registry_ = nullptr;
    telemetry_ = nullptr;
    identifier_.clear();
}

bool ExecutorTelemetrySampler::registered() const noexcept
{
    return registry_ != nullptr && telemetry_ != nullptr && !identifier_.empty();
}

}  // namespace bored::executor
