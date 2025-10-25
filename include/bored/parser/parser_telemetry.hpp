#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>

namespace bored::parser {

struct ParserTelemetrySnapshot final {
    std::uint64_t scripts_attempted = 0U;
    std::uint64_t scripts_succeeded = 0U;
    std::uint64_t statements_attempted = 0U;
    std::uint64_t statements_succeeded = 0U;
    std::uint64_t diagnostics_info = 0U;
    std::uint64_t diagnostics_warning = 0U;
    std::uint64_t diagnostics_error = 0U;
    std::uint64_t total_parse_duration_ns = 0U;
    std::uint64_t last_parse_duration_ns = 0U;
};

class ParserTelemetry final {
public:
    void record_script_attempt() noexcept;
    void record_script_result(bool success,
                              std::uint64_t parse_duration_ns,
                              std::size_t statements_attempted,
                              std::size_t statements_succeeded,
                              std::size_t info_diagnostics,
                              std::size_t warning_diagnostics,
                              std::size_t error_diagnostics) noexcept;

    [[nodiscard]] ParserTelemetrySnapshot snapshot() const noexcept;
    void reset() noexcept;

private:
    static void add_relaxed(std::atomic<std::uint64_t>& target, std::uint64_t value) noexcept;

    std::atomic<std::uint64_t> scripts_attempted_{0U};
    std::atomic<std::uint64_t> scripts_succeeded_{0U};
    std::atomic<std::uint64_t> statements_attempted_{0U};
    std::atomic<std::uint64_t> statements_succeeded_{0U};
    std::atomic<std::uint64_t> diagnostics_info_{0U};
    std::atomic<std::uint64_t> diagnostics_warning_{0U};
    std::atomic<std::uint64_t> diagnostics_error_{0U};
    std::atomic<std::uint64_t> total_parse_duration_ns_{0U};
    std::atomic<std::uint64_t> last_parse_duration_ns_{0U};
};

class ParserTelemetryRegistry final {
public:
    using Sampler = std::function<ParserTelemetrySnapshot()>;
    using Visitor = std::function<void(const std::string&, const ParserTelemetrySnapshot&)>;

    void register_sampler(std::string identifier, Sampler sampler);
    void unregister_sampler(const std::string& identifier);

    [[nodiscard]] ParserTelemetrySnapshot aggregate() const;
    void visit(const Visitor& visitor) const;

private:
    mutable std::mutex mutex_{};
    std::unordered_map<std::string, Sampler> samplers_{};
};

}  // namespace bored::parser
