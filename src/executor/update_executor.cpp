#include "bored/executor/update_executor.hpp"
#include "bored/executor/executor_context.hpp"

#include <stdexcept>

namespace bored::executor {
namespace {

constexpr std::size_t kMaxColumnsSafety = 1024U;

}  // namespace

UpdateExecutor::UpdateExecutor(ExecutorNodePtr child, Config config)
    : config_{std::move(config)}
{
    if (!child) {
        throw std::invalid_argument{"UpdateExecutor requires a child executor"};
    }
    if (config_.target == nullptr) {
        throw std::invalid_argument{"UpdateExecutor requires a storage target"};
    }
    add_child(std::move(child));
}

void UpdateExecutor::open(ExecutorContext& context)
{
    (void)context;
    ensure_child_available();
    child(0U)->open(context);
    child_open_ = true;
    drained_ = false;
    transaction_hooks_registered_ = false;
}

bool UpdateExecutor::next(ExecutorContext& context, TupleBuffer& buffer)
{
    ExecutorTelemetry::LatencyScope latency_scope{config_.telemetry, ExecutorTelemetry::Operator::Update};
    (void)buffer;
    ensure_child_available();
    if (!drained_) {
        drain_child(context);
    }
    return false;
}

void UpdateExecutor::close(ExecutorContext& context)
{
    if (child_open_) {
        if (!drained_) {
            drain_child(context);
        }
        child(0U)->close(context);
        child_open_ = false;
    }

    drained_ = true;

    finalize_target(context);
}

void UpdateExecutor::ensure_child_available() const
{
    if (child_count() != 1U) {
        throw std::logic_error{"UpdateExecutor expects exactly one child"};
    }
    if (child(0U) == nullptr) {
        throw std::logic_error{"UpdateExecutor child is null"};
    }
}

void UpdateExecutor::drain_child(ExecutorContext& context)
{
    if (drained_) {
        return;
    }

    auto* child_executor = child(0U);
    if (child_executor == nullptr) {
        throw std::logic_error{"UpdateExecutor child unavailable"};
    }

    while (child_executor->next(context, child_buffer_)) {
        auto tuple_view = TupleView::from_buffer(child_buffer_);
        if (!tuple_view.valid()) {
            throw std::runtime_error{"UpdateExecutor received invalid tuple"};
        }
        if (tuple_view.column_count() > kMaxColumnsSafety) {
            throw std::runtime_error{"UpdateExecutor tuple has excessive columns"};
        }

        apply_telemetry_attempt();

        UpdateStats stats{};
        if (auto ec = config_.target->update_tuple(tuple_view, context, stats); ec) {
            throw std::system_error(ec, "UpdateExecutor update failed");
        }

        apply_telemetry_success(stats);
        child_buffer_.reset();
    }

    drained_ = true;
}

void UpdateExecutor::apply_telemetry_attempt() const
{
    if (config_.telemetry != nullptr) {
        config_.telemetry->record_update_attempt();
    }
}

void UpdateExecutor::apply_telemetry_success(const UpdateStats& stats) const
{
    if (config_.telemetry != nullptr) {
        config_.telemetry->record_update_success(stats.new_payload_bytes,
                                                 stats.old_payload_bytes,
                                                 stats.wal_bytes);
    }
}

void UpdateExecutor::finalize_target(ExecutorContext& context)
{
    if (config_.target == nullptr) {
        return;
    }

    auto* txn = context.transaction_context();
    if (txn == nullptr) {
        if (auto ec = config_.target->flush(context); ec) {
            throw std::system_error(ec, "UpdateExecutor target flush failed");
        }
        return;
    }

    if (transaction_hooks_registered_) {
        return;
    }

    if (auto ec = config_.target->register_transaction_hooks(*txn, context); ec) {
        throw std::system_error(ec, "UpdateExecutor transaction hook registration failed");
    }

    transaction_hooks_registered_ = true;
}

}  // namespace bored::executor
