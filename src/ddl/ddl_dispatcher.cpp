#include "bored/ddl/ddl_dispatcher.hpp"

#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_mutator.hpp"
#include "bored/catalog/catalog_transaction.hpp"
#include "bored/txn/transaction_manager.hpp"

#include <algorithm>
#include <chrono>
#include <optional>
#include <span>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>

namespace bored::ddl {

namespace {

constexpr std::uint64_t default_commit_lsn() noexcept
{
    return 0U;
}

DdlCommandResponse make_internal_failure(std::string message,
                                         std::vector<std::string> hints = {"Inspect server logs for dispatcher failures."})
{
    return make_failure(make_error_code(DdlErrc::ExecutionFailed),
                        std::move(message),
                        DdlDiagnosticSeverity::Error,
                        std::move(hints));
}

}  // namespace

DdlCommandDispatcher::DdlCommandDispatcher(Config config)
    : config_{std::move(config)}
{
    if (!config_.commit_lsn_provider) {
        config_.commit_lsn_provider = [] { return default_commit_lsn(); };
    }

    if (config_.telemetry_registry && !config_.telemetry_identifier.empty()) {
        registry_ = config_.telemetry_registry;
        registry_identifier_ = config_.telemetry_identifier;
        registry_->register_sampler(registry_identifier_, [this] { return telemetry_.snapshot(); });
    }
}

DdlCommandDispatcher::~DdlCommandDispatcher()
{
    if (registry_) {
        registry_->unregister_sampler(registry_identifier_);
    }
}

DdlCommandDispatcher::TransactionScope::TransactionScope(catalog::CatalogTransaction& transaction,
                                                         txn::TransactionManager* manager,
                                                         txn::TransactionContext* context)
    : transaction_{transaction}
    , manager_{manager}
    , context_{context}
{
}

DdlCommandDispatcher::TransactionScope::~TransactionScope()
{
    if (!completed_ && transaction_.is_active()) {
        (void)abort();
    }
}

std::error_code DdlCommandDispatcher::TransactionScope::commit()
{
    if (completed_) {
        return {};
    }
    if (auto ec = transaction_.commit()) {
        if (manager_ && context_) {
            manager_->abort(*context_);
        }
        return ec;
    }
    if (manager_ && context_) {
        manager_->commit(*context_);
    }
    completed_ = true;
    return {};
}

std::error_code DdlCommandDispatcher::TransactionScope::abort()
{
    if (completed_) {
        return {};
    }
    if (manager_ && context_) {
        try {
            manager_->abort(*context_);
        } catch (...) {
            // ignore abort failures to preserve original error reporting contract
        }
    }
    completed_ = true;
    return transaction_.abort();
}

DdlCommandDispatcher::HandlerFn* DdlCommandDispatcher::find_handler(const DdlCommand& command) noexcept
{
    const auto index = command.index();
    if (index >= handlers_.size()) {
        return nullptr;
    }
    auto& handler = handlers_[index];
    if (!handler) {
        return nullptr;
    }
    return &handler;
}

DdlVerb DdlCommandDispatcher::command_verb(const DdlCommand& command) const noexcept
{
    return static_cast<DdlVerb>(command.index());
}

DdlCommandResponse DdlCommandDispatcher::dispatch(const DdlCommand& command)
{
    const auto verb = command_verb(command);
    telemetry_.record_attempt(verb);

    const auto start = std::chrono::steady_clock::now();
    const auto record_duration = [&](void) {
        const auto end = std::chrono::steady_clock::now();
        const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        telemetry_.record_duration(verb, static_cast<std::uint64_t>(duration < 0 ? 0 : duration));
    };

    if (config_.identifier_allocator == nullptr) {
        telemetry_.record_failure(verb, make_error_code(DdlErrc::ExecutionFailed));
        record_duration();
        return make_internal_failure("DDL dispatcher missing identifier allocator",
                                     {"Configure DdlCommandDispatcher::Config.identifier_allocator before dispatching commands."});
    }

    if (!config_.transaction_factory) {
        telemetry_.record_failure(verb, make_error_code(DdlErrc::ExecutionFailed));
        record_duration();
        return make_internal_failure("DDL dispatcher missing transaction factory",
                                     {"Provide DdlCommandDispatcher::Config.transaction_factory to create catalog transactions."});
    }

    std::optional<txn::TransactionContext> txn_context{};
    txn::TransactionContext* txn_context_ptr = nullptr;
    bool context_managed = false;
    if (config_.transaction_manager != nullptr) {
        txn_context.emplace(config_.transaction_manager->begin());
        txn_context_ptr = &*txn_context;
    }

    const auto abort_context_if_needed = [&]() {
        if (!context_managed && config_.transaction_manager != nullptr && txn_context_ptr != nullptr) {
            try {
                config_.transaction_manager->abort(*txn_context_ptr);
            } catch (...) {
                // ignore abort failures; dispatcher already reports the triggering error
            }
            context_managed = true;
        }
    };

    struct ContextAbortGuard final {
        txn::TransactionManager* manager = nullptr;
        txn::TransactionContext* context = nullptr;
        bool* managed = nullptr;

        ~ContextAbortGuard()
        {
            if (manager != nullptr && context != nullptr && managed != nullptr && !*managed) {
                try {
                    manager->abort(*context);
                } catch (...) {
                    // ignore guard abort failures to preserve existing error reporting behaviour
                }
            }
        }
    } context_guard{config_.transaction_manager, txn_context_ptr, &context_managed};

    auto transaction = config_.transaction_factory(txn_context_ptr);
    if (!transaction) {
        telemetry_.record_failure(verb, make_error_code(DdlErrc::ExecutionFailed));
        abort_context_if_needed();
        record_duration();
        return make_internal_failure("DDL dispatcher failed to acquire transaction",
                                     {"Ensure the transaction_factory returns an active CatalogTransaction instance."});
    }

    transaction->bind_transaction_context(config_.transaction_manager, txn_context_ptr);

    auto handler = find_handler(command);
    if (handler == nullptr) {
        telemetry_.record_failure(verb, make_error_code(DdlErrc::HandlerMissing));
        abort_context_if_needed();
        record_duration();
        return make_failure(make_error_code(DdlErrc::HandlerMissing),
                            "no handler registered for DDL verb",
                            DdlDiagnosticSeverity::Error,
                            {"Register a handler for this DDL verb before dispatching commands."});
    }

    std::unique_ptr<catalog::CatalogMutator> mutator;
    if (config_.mutator_factory) {
        mutator = config_.mutator_factory(*transaction);
    } else {
        catalog::CatalogMutatorConfig mutator_config{};
        mutator_config.transaction = transaction.get();
        mutator_config.commit_lsn_provider = config_.commit_lsn_provider;
        mutator = std::make_unique<catalog::CatalogMutator>(mutator_config);
    }

    if (mutator) {
        mutator->set_commit_lsn_provider(config_.commit_lsn_provider);
    }

    if (mutator && config_.catalog_dirty_hook) {
        auto hook = config_.catalog_dirty_hook;
        mutator->set_publish_listener([hook](const catalog::CatalogMutationBatch& batch) -> std::error_code {
            if (batch.mutations.empty()) {
                return {};
            }

            std::vector<catalog::RelationId> relations;
            relations.reserve(batch.mutations.size());
            for (const auto& mutation : batch.mutations) {
                const auto already_recorded = std::find(relations.begin(), relations.end(), mutation.relation_id) != relations.end();
                if (!already_recorded) {
                    relations.push_back(mutation.relation_id);
                }
            }

            std::span<const catalog::RelationId> relation_span(relations.data(), relations.size());
            return hook(relation_span, batch.commit_lsn);
        });
    }

    std::unique_ptr<catalog::CatalogAccessor> accessor;
    if (config_.accessor_factory) {
        accessor = config_.accessor_factory(*transaction);
    }

    DdlCommandContext context{*transaction, *config_.identifier_allocator};
    context.mutator = mutator.get();
    context.accessor = accessor.get();
    context.drop_table_cleanup = config_.drop_table_cleanup_hook;
    context.dirty_relation_notifier = config_.catalog_dirty_hook;
    context.create_index_storage = config_.create_index_storage_hook;
    context.drop_index_cleanup = config_.drop_index_cleanup_hook;

    TransactionScope scope{*transaction, config_.transaction_manager, txn_context_ptr};
    context_managed = true;

    DdlCommandResponse response{};
    try {
        response = (*handler)(context, command);
    } catch (const std::exception& ex) {
        telemetry_.record_failure(verb, make_error_code(DdlErrc::ExecutionFailed));
        (void)scope.abort();
        record_duration();
        return make_failure(make_error_code(DdlErrc::ExecutionFailed), ex.what());
    } catch (...) {
        telemetry_.record_failure(verb, make_error_code(DdlErrc::ExecutionFailed));
        (void)scope.abort();
        record_duration();
        return make_failure(make_error_code(DdlErrc::ExecutionFailed), "unknown DDL execution failure");
    }

    if (response.success) {
        if (auto ec = scope.commit()) {
            telemetry_.record_failure(verb, make_error_code(DdlErrc::ExecutionFailed));
            record_duration();
            return make_failure(make_error_code(DdlErrc::ExecutionFailed),
                                ec.message(),
                                DdlDiagnosticSeverity::Error,
                                {"Check catalog and storage logs for commit failures before retrying this DDL command."});
        }
        telemetry_.record_success(verb);
        record_duration();
        return response;
    }

    auto error = response.error;
    if (!error) {
        error = make_error_code(DdlErrc::ExecutionFailed);
        response.error = error;
    }
    telemetry_.record_failure(verb, error);
    (void)scope.abort();
    record_duration();
    return response;
}

}  // namespace bored::ddl
