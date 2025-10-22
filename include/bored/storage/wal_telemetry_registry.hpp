#pragma once

#include "bored/storage/wal_writer.hpp"

#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>

namespace bored::storage {

class WalTelemetryRegistry final {
public:
    using Sampler = std::function<WalWriterTelemetrySnapshot()>;
    using Visitor = std::function<void(const std::string&, const WalWriterTelemetrySnapshot&)>;

    void register_sampler(std::string identifier, Sampler sampler);
    void unregister_sampler(const std::string& identifier);

    [[nodiscard]] WalWriterTelemetrySnapshot aggregate() const;
    void visit(const Visitor& visitor) const;

private:
    using SamplerMap = std::unordered_map<std::string, Sampler>;

    mutable std::mutex mutex_{};
    SamplerMap samplers_{};
};

}  // namespace bored::storage
