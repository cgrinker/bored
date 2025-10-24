#pragma once

#include "bored/ddl/ddl_command.hpp"
#include "bored/ddl/ddl_errors.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>

namespace bored::ddl {

struct DdlVerbTelemetrySnapshot final {
    std::uint64_t attempts = 0U;
    std::uint64_t successes = 0U;
    std::uint64_t failures = 0U;
};

struct DdlFailureTelemetrySnapshot final {
    std::uint64_t handler_missing = 0U;
    std::uint64_t validation_failures = 0U;
    std::uint64_t execution_failures = 0U;
    std::uint64_t other_failures = 0U;
};

struct DdlTelemetrySnapshot final {
    std::array<DdlVerbTelemetrySnapshot, static_cast<std::size_t>(DdlVerb::Count)> verbs{};
    DdlFailureTelemetrySnapshot failures{};
};

class DdlCommandTelemetry final {
public:
    void record_attempt(DdlVerb verb) noexcept;
    void record_success(DdlVerb verb) noexcept;
    void record_failure(DdlVerb verb, std::error_code error) noexcept;

    [[nodiscard]] DdlTelemetrySnapshot snapshot() const noexcept;
    void reset() noexcept;

private:
    static constexpr std::size_t verb_count = static_cast<std::size_t>(DdlVerb::Count);

    std::array<std::atomic<std::uint64_t>, verb_count> attempts_{};
    std::array<std::atomic<std::uint64_t>, verb_count> successes_{};
    std::array<std::atomic<std::uint64_t>, verb_count> failures_{};

    std::atomic<std::uint64_t> handler_missing_{0U};
    std::atomic<std::uint64_t> validation_failures_{0U};
    std::atomic<std::uint64_t> execution_failures_{0U};
    std::atomic<std::uint64_t> other_failures_{0U};
};

class DdlTelemetryRegistry final {
public:
    using Sampler = std::function<DdlTelemetrySnapshot()>;
    using Visitor = std::function<void(const std::string&, const DdlTelemetrySnapshot&)>;

    void register_sampler(std::string identifier, Sampler sampler);
    void unregister_sampler(const std::string& identifier);

    [[nodiscard]] DdlTelemetrySnapshot aggregate() const;
    void visit(const Visitor& visitor) const;

private:
    using SamplerMap = std::unordered_map<std::string, Sampler>;

    mutable std::mutex mutex_{};
    SamplerMap samplers_{};
};

}  // namespace bored::ddl
