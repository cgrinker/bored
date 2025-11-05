#include "bored/executor/spool_executor.hpp"

#include "bored/executor/executor_context.hpp"
#include "bored/executor/worktable_registry.hpp"
#include "bored/txn/snapshot_utils.hpp"

#include <stdexcept>
#include <utility>

namespace bored::executor {

SpoolExecutor::SpoolExecutor(ExecutorNodePtr child, Config config)
    : config_{std::move(config)}
{
    if (!child) {
        throw std::invalid_argument{"SpoolExecutor requires a child executor"};
    }
    if (config_.reserve_rows != 0U) {
        materialized_rows_.reserve(config_.reserve_rows);
    }
    add_child(std::move(child));
}

void SpoolExecutor::open(ExecutorContext& context)
{
    (void)context;
    position_ = 0U;
}

bool SpoolExecutor::next(ExecutorContext& context, TupleBuffer& buffer)
{
    ExecutorTelemetry::LatencyScope latency_scope{config_.telemetry, ExecutorTelemetry::Operator::Spool};
    ensure_materialized(context);

    const auto* rows = shared_rows_ ? shared_rows_.get() : &materialized_rows_;
    if (position_ >= rows->size()) {
        return false;
    }

    buffer.reset();
    const auto& stored = (*rows)[position_++];
    buffer.write(stored.span());
    return true;
}

void SpoolExecutor::close(ExecutorContext& context)
{
    (void)context;
    const auto* rows = shared_rows_ ? shared_rows_.get() : &materialized_rows_;
    position_ = rows->size();
    child_buffer_.reset();
}

void SpoolExecutor::reset() noexcept
{
    materialized_rows_.clear();
    materialized_ = false;
    position_ = 0U;
    child_buffer_.reset();
    materialized_snapshot_ = {};
    shared_rows_.reset();
}

void SpoolExecutor::ensure_materialized(ExecutorContext& context)
{
    const auto& current_snapshot = context.snapshot();
    if (materialized_) {
        if (txn::snapshots_equal(materialized_snapshot_, current_snapshot)) {
            return;
        }

        materialized_rows_.clear();
        child_buffer_.reset();
        materialized_ = false;
        position_ = 0U;
        shared_rows_.reset();
        if (config_.reserve_rows != 0U && materialized_rows_.capacity() < config_.reserve_rows) {
            materialized_rows_.reserve(config_.reserve_rows);
        }
    }

    if (config_.worktable_registry != nullptr && config_.worktable_id.has_value()) {
        if (auto cached_rows = config_.worktable_registry->find(*config_.worktable_id, current_snapshot)) {
            shared_rows_ = cached_rows;
            materialized_rows_.clear();
            child_buffer_.reset();
            materialized_snapshot_ = current_snapshot;
            materialized_ = true;
            position_ = 0U;
            if (config_.reserve_rows != 0U && materialized_rows_.capacity() < config_.reserve_rows) {
                materialized_rows_.reserve(config_.reserve_rows);
            }
            return;
        }
    }

    if (child_count() != 1U) {
        throw std::logic_error{"SpoolExecutor expected exactly one child"};
    }
    auto* input = child(0U);
    if (input == nullptr) {
        throw std::logic_error{"SpoolExecutor missing child executor"};
    }

    input->open(context);
    while (input->next(context, child_buffer_)) {
        TupleBuffer stored{child_buffer_.size()};
        stored.write(child_buffer_.span());
        materialized_rows_.push_back(std::move(stored));
        child_buffer_.reset();
    }
    input->close(context);

    shared_rows_.reset();
    if (config_.worktable_registry != nullptr && config_.worktable_id.has_value()) {
        txn::Snapshot snapshot_copy = current_snapshot;
        auto cached_rows = config_.worktable_registry->publish(*config_.worktable_id,
                                                               std::move(snapshot_copy),
                                                               std::move(materialized_rows_));
        shared_rows_ = cached_rows;
        materialized_rows_.clear();
        if (config_.reserve_rows != 0U && materialized_rows_.capacity() < config_.reserve_rows) {
            materialized_rows_.reserve(config_.reserve_rows);
        }
    }

    materialized_snapshot_ = current_snapshot;
    materialized_ = true;
    position_ = 0U;
}

std::optional<WorkTableRegistry::SnapshotIterator> SpoolExecutor::snapshot_iterator() const
{
    if (!materialized_) {
        return std::nullopt;
    }
    if (config_.worktable_registry == nullptr || !config_.worktable_id.has_value()) {
        return std::nullopt;
    }
    return config_.worktable_registry->snapshot_iterator(*config_.worktable_id, materialized_snapshot_);
}

std::optional<WorkTableRegistry::RecursiveCursor> SpoolExecutor::recursive_cursor() const
{
    if (!materialized_) {
        return std::nullopt;
    }
    if (!config_.enable_recursive_cursor) {
        return std::nullopt;
    }
    if (config_.worktable_registry == nullptr || !config_.worktable_id.has_value()) {
        return std::nullopt;
    }
    return config_.worktable_registry->recursive_cursor(*config_.worktable_id, materialized_snapshot_);
}

}  // namespace bored::executor
