#include "bored/executor/projection_executor.hpp"

#include <stdexcept>
#include <utility>

namespace bored::executor {

ProjectionExecutor::ProjectionExecutor(ExecutorNodePtr child, Config config)
    : config_{std::move(config)}
{
    if (!child) {
        throw std::invalid_argument{"ProjectionExecutor requires a child executor"};
    }
    if (config_.projections.empty()) {
        throw std::invalid_argument{"ProjectionExecutor requires at least one projection"};
    }
    add_child(std::move(child));
}

void ProjectionExecutor::open(ExecutorContext& context)
{
    if (child_count() != 1U) {
        throw std::logic_error{"ProjectionExecutor expected exactly one child"};
    }
    auto* input = child(0U);
    if (input == nullptr) {
        throw std::logic_error{"ProjectionExecutor missing child executor"};
    }
    input->open(context);
}

bool ProjectionExecutor::next(ExecutorContext& context, TupleBuffer& buffer)
{
    ExecutorTelemetry::LatencyScope latency_scope{config_.telemetry, ExecutorTelemetry::Operator::Projection};
    if (child_count() != 1U) {
        return false;
    }

    auto* input = child(0U);
    if (input == nullptr) {
        return false;
    }

    while (input->next(context, child_buffer_)) {
        auto input_view = TupleView::from_buffer(child_buffer_);
        TupleWriter writer{buffer};
        writer.reset();
        for (auto& projection : config_.projections) {
            projection(input_view, writer, context);
        }
        writer.finalize();
        if (config_.telemetry != nullptr) {
            config_.telemetry->record_projection_row();
        }
        return true;
    }

    return false;
}

void ProjectionExecutor::close(ExecutorContext& context)
{
    if (child_count() != 1U) {
        return;
    }
    auto* input = child(0U);
    if (input != nullptr) {
        input->close(context);
    }
    child_buffer_.reset();
}

}  // namespace bored::executor
