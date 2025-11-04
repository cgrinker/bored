#include "bored/executor/spool_executor.hpp"

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

    if (position_ >= materialized_rows_.size()) {
        return false;
    }

    buffer.reset();
    const auto& stored = materialized_rows_[position_++];
    buffer.write(stored.span());
    return true;
}

void SpoolExecutor::close(ExecutorContext& context)
{
    (void)context;
    position_ = materialized_rows_.size();
    child_buffer_.reset();
}

void SpoolExecutor::reset() noexcept
{
    materialized_rows_.clear();
    materialized_ = false;
    position_ = 0U;
    child_buffer_.reset();
}

void SpoolExecutor::ensure_materialized(ExecutorContext& context)
{
    if (materialized_) {
        return;
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

    materialized_ = true;
    position_ = 0U;
}

}  // namespace bored::executor
