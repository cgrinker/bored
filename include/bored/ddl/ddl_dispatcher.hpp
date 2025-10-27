#pragma once

#include "bored/ddl/ddl_command.hpp"
#include "bored/ddl/ddl_telemetry.hpp"

#include <array>
#include <functional>
#include <memory>
#include <optional>
#include <string>

namespace bored::txn {
class TransactionManager;
class TransactionContext;
}  // namespace bored::txn

namespace bored::ddl {

class DdlCommandDispatcher final {
public:
    struct Config final {
        std::function<std::unique_ptr<catalog::CatalogTransaction>(txn::TransactionContext*)> transaction_factory;
        std::function<std::unique_ptr<catalog::CatalogMutator>(catalog::CatalogTransaction&)> mutator_factory;
        std::function<std::unique_ptr<catalog::CatalogAccessor>(catalog::CatalogTransaction&)> accessor_factory;
        catalog::CatalogIdentifierAllocator* identifier_allocator = nullptr;
        std::function<std::uint64_t()> commit_lsn_provider;
        txn::TransactionManager* transaction_manager = nullptr;
        DdlTelemetryRegistry* telemetry_registry = nullptr;
        std::string telemetry_identifier;
        DropTableCleanupHook drop_table_cleanup_hook{};
        CatalogDirtyRelationHook catalog_dirty_hook{};
        CreateIndexStorageHook create_index_storage_hook{};
        DropIndexCleanupHook drop_index_cleanup_hook{};
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
    TransactionScope(catalog::CatalogTransaction& transaction,
             txn::TransactionManager* manager,
             txn::TransactionContext* context);
        ~TransactionScope();

        TransactionScope(const TransactionScope&) = delete;
        TransactionScope& operator=(const TransactionScope&) = delete;

        std::error_code commit();
        std::error_code abort();

    private:
        catalog::CatalogTransaction& transaction_;
        txn::TransactionManager* manager_ = nullptr;
        txn::TransactionContext* context_ = nullptr;
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
