#include "bored/ddl/ddl_dispatcher.hpp"

#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_mutator.hpp"
#include "bored/catalog/catalog_transaction.hpp"

#include <stdexcept>
#include <system_error>

namespace bored::ddl {

namespace {

constexpr std::uint64_t default_commit_lsn() noexcept
{
    return 0U;
}

DdlCommandResponse make_internal_failure(std::string message)
{
    return make_failure(make_error_code(DdlErrc::ExecutionFailed), std::move(message));
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

DdlCommandDispatcher::TransactionScope::TransactionScope(catalog::CatalogTransaction& transaction)
    : transaction_{transaction}
{
}

DdlCommandDispatcher::TransactionScope::~TransactionScope()
{
    if (!completed_ && transaction_.is_active()) {
        (void)transaction_.abort();
    }
}

std::error_code DdlCommandDispatcher::TransactionScope::commit()
{
    if (completed_) {
        return {};
    }
    if (auto ec = transaction_.commit()) {
        return ec;
    }
    completed_ = true;
    return {};
}

std::error_code DdlCommandDispatcher::TransactionScope::abort()
{
    if (completed_) {
        return {};
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

    if (config_.identifier_allocator == nullptr) {
        telemetry_.record_failure(verb, make_error_code(DdlErrc::ExecutionFailed));
        return make_internal_failure("DDL dispatcher missing identifier allocator");
    }

    if (!config_.transaction_factory) {
        telemetry_.record_failure(verb, make_error_code(DdlErrc::ExecutionFailed));
        return make_internal_failure("DDL dispatcher missing transaction factory");
    }

    auto transaction = config_.transaction_factory();
    if (!transaction) {
        telemetry_.record_failure(verb, make_error_code(DdlErrc::ExecutionFailed));
        return make_internal_failure("DDL dispatcher failed to acquire transaction");
    }

    auto handler = find_handler(command);
    if (handler == nullptr) {
        telemetry_.record_failure(verb, make_error_code(DdlErrc::HandlerMissing));
        return make_failure(make_error_code(DdlErrc::HandlerMissing), "no handler registered for DDL verb");
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

    std::unique_ptr<catalog::CatalogAccessor> accessor;
    if (config_.accessor_factory) {
        accessor = config_.accessor_factory(*transaction);
    }

    DdlCommandContext context{*transaction, *config_.identifier_allocator};
    context.mutator = mutator.get();
    context.accessor = accessor.get();
    context.drop_table_cleanup = config_.drop_table_cleanup_hook;

    TransactionScope scope{*transaction};

    DdlCommandResponse response{};
    try {
        response = (*handler)(context, command);
    } catch (const std::exception& ex) {
        telemetry_.record_failure(verb, make_error_code(DdlErrc::ExecutionFailed));
        (void)scope.abort();
        return make_failure(make_error_code(DdlErrc::ExecutionFailed), ex.what());
    } catch (...) {
        telemetry_.record_failure(verb, make_error_code(DdlErrc::ExecutionFailed));
        (void)scope.abort();
        return make_failure(make_error_code(DdlErrc::ExecutionFailed), "unknown DDL execution failure");
    }

    if (response.success) {
        if (auto ec = scope.commit()) {
            telemetry_.record_failure(verb, make_error_code(DdlErrc::ExecutionFailed));
            return make_failure(make_error_code(DdlErrc::ExecutionFailed), ec.message());
        }
        telemetry_.record_success(verb);
        return response;
    }

    auto error = response.error;
    if (!error) {
        error = make_error_code(DdlErrc::ExecutionFailed);
        response.error = error;
    }
    telemetry_.record_failure(verb, error);
    (void)scope.abort();
    return response;
}

}  // namespace bored::ddl
