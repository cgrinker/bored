#pragma once

#include "bored/ddl/ddl_command.hpp"
#include "bored/ddl/ddl_telemetry.hpp"

#include <array>
#include <functional>
#include <memory>
#include <optional>
#include <string>

namespace bored::ddl {

class DdlCommandDispatcher final {
public:
    struct Config final {
        std::function<std::unique_ptr<catalog::CatalogTransaction>()> transaction_factory;
        std::function<std::unique_ptr<catalog::CatalogMutator>(catalog::CatalogTransaction&)> mutator_factory;
        std::function<std::unique_ptr<catalog::CatalogAccessor>(catalog::CatalogTransaction&)> accessor_factory;
        catalog::CatalogIdentifierAllocator* identifier_allocator = nullptr;
        std::function<std::uint64_t()> commit_lsn_provider;
        DdlTelemetryRegistry* telemetry_registry = nullptr;
        std::string telemetry_identifier;
    };

    explicit DdlCommandDispatcher(Config config);
    ~DdlCommandDispatcher();

    DdlCommandDispatcher(const DdlCommandDispatcher&) = delete;
    DdlCommandDispatcher& operator=(const DdlCommandDispatcher&) = delete;
    DdlCommandDispatcher(DdlCommandDispatcher&&) = delete;
    DdlCommandDispatcher& operator=(DdlCommandDispatcher&&) = delete;

    template <typename Request>
    void register_handler(std::function<DdlCommandResponse(DdlCommandContext&, const Request&)> handler)
    {
        const auto index = request_verb_index<Request>();
        handlers_[index] = [handler = std::move(handler)](DdlCommandContext& context, const DdlCommand& command) {
            return handler(context, std::get<Request>(command));
        };
    }

    [[nodiscard]] DdlCommandResponse dispatch(const DdlCommand& command);
    [[nodiscard]] const DdlCommandTelemetry& telemetry() const noexcept { return telemetry_; }

private:
    using HandlerFn = std::function<DdlCommandResponse(DdlCommandContext&, const DdlCommand&)>;

    struct TransactionScope final {
        explicit TransactionScope(catalog::CatalogTransaction& transaction);
        ~TransactionScope();

        TransactionScope(const TransactionScope&) = delete;
        TransactionScope& operator=(const TransactionScope&) = delete;

        std::error_code commit();
        std::error_code abort();

    private:
        catalog::CatalogTransaction& transaction_;
        bool completed_ = false;
    };

    [[nodiscard]] HandlerFn* find_handler(const DdlCommand& command) noexcept;
    [[nodiscard]] DdlVerb command_verb(const DdlCommand& command) const noexcept;

    Config config_{};
    DdlCommandTelemetry telemetry_{};
    DdlTelemetryRegistry* registry_ = nullptr;
    std::string registry_identifier_{};
    std::array<HandlerFn, static_cast<std::size_t>(DdlVerb::Count)> handlers_{};
};

}  // namespace bored::ddl
