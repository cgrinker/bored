#include "bored/executor/insert_executor.hpp"
#include "bored/executor/executor_context.hpp"

#include <stdexcept>

namespace bored::executor {
namespace {

constexpr std::size_t kMaxColumnsSafety = 1024U;

}  // namespace

InsertExecutor::InsertExecutor(ExecutorNodePtr child, Config config)
    : config_{std::move(config)}
{
    if (!child) {
        throw std::invalid_argument{"InsertExecutor requires a child executor"};
    }
    if (config_.target == nullptr) {
        throw std::invalid_argument{"InsertExecutor requires a storage target"};
    }
    add_child(std::move(child));
}

void InsertExecutor::open(ExecutorContext& context)
{
    (void)context;
    ensure_child_available();
    child(0U)->open(context);
    child_open_ = true;
    drained_ = false;
    transaction_hooks_registered_ = false;
}

bool InsertExecutor::next(ExecutorContext& context, TupleBuffer& buffer)
{
    ExecutorTelemetry::LatencyScope latency_scope{config_.telemetry, ExecutorTelemetry::Operator::Insert};
    (void)buffer;
    ensure_child_available();
    if (!drained_) {
        drain_child(context);
    }
    return false;
}

void InsertExecutor::close(ExecutorContext& context)
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

void InsertExecutor::ensure_child_available() const
{
    if (child_count() != 1U) {
        throw std::logic_error{"InsertExecutor expects exactly one child"};
    }
    if (child(0U) == nullptr) {
        throw std::logic_error{"InsertExecutor child is null"};
    }
}

void InsertExecutor::drain_child(ExecutorContext& context)
{
    if (drained_) {
        return;
    }

    auto* child_executor = child(0U);
    if (child_executor == nullptr) {
        throw std::logic_error{"InsertExecutor child unavailable"};
    }

    while (child_executor->next(context, child_buffer_)) {
        auto tuple_view = TupleView::from_buffer(child_buffer_);
        if (!tuple_view.valid()) {
            throw std::runtime_error{"InsertExecutor received invalid tuple"};
        }
        if (tuple_view.column_count() > kMaxColumnsSafety) {
            throw std::runtime_error{"InsertExecutor tuple has excessive columns"};
        }

        apply_telemetry_attempt();

        InsertStats stats{};
        if (auto ec = config_.target->insert_tuple(tuple_view, context, stats); ec) {
            throw std::system_error(ec, "InsertExecutor insert failed");
        }

        apply_telemetry_success(stats);
        child_buffer_.reset();
    }

    drained_ = true;
}

void InsertExecutor::apply_telemetry_attempt() const
{
    if (config_.telemetry != nullptr) {
        config_.telemetry->record_insert_attempt();
    }
}

void InsertExecutor::apply_telemetry_success(const InsertStats& stats) const
{
    if (config_.telemetry != nullptr) {
        config_.telemetry->record_insert_success(stats.payload_bytes, stats.wal_bytes);
    }
}

void InsertExecutor::finalize_target(ExecutorContext& context)
{
    if (config_.target == nullptr) {
        return;
    }

    auto* txn = context.transaction_context();
    if (txn == nullptr) {
        if (auto ec = config_.target->flush(context); ec) {
            throw std::system_error(ec, "InsertExecutor target flush failed");
        }
        return;
    }

    if (transaction_hooks_registered_) {
        return;
    }

    if (auto ec = config_.target->register_transaction_hooks(*txn, context); ec) {
        throw std::system_error(ec, "InsertExecutor transaction hook registration failed");
    }

    transaction_hooks_registered_ = true;
}

}  // namespace bored::executor
