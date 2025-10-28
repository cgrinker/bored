#include "bored/executor/nested_loop_join_executor.hpp"

#include <stdexcept>
#include <utility>

namespace bored::executor {

namespace {

constexpr std::size_t kOuterChildIndex = 0U;
constexpr std::size_t kInnerChildIndex = 1U;

}  // namespace

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorNodePtr outer, ExecutorNodePtr inner, Config config)
    : config_{std::move(config)}
{
    if (!outer || !inner) {
        throw std::invalid_argument{"NestedLoopJoinExecutor requires both outer and inner children"};
    }
    if (config_.projections.empty()) {
        // Default to concatenating outer and inner columns when no explicit projection is provided.
        config_.projections.push_back([this](const TupleView& outer_view,
                                             const TupleView& inner_view,
                                             TupleWriter& writer,
                                             ExecutorContext&) {
            append_view_columns(outer_view, writer);
            append_view_columns(inner_view, writer);
        });
    }
    add_child(std::move(outer));
    add_child(std::move(inner));
}

void NestedLoopJoinExecutor::open(ExecutorContext& context)
{
    if (child_count() != 2U) {
        throw std::logic_error{"NestedLoopJoinExecutor expected exactly two children"};
    }

    outer_child()->open(context);
    inner_child()->open(context);
    inner_open_ = true;
    outer_view_valid_ = false;
    outer_view_ = {};
    outer_buffer_.reset();
    inner_buffer_.reset();
}

bool NestedLoopJoinExecutor::next(ExecutorContext& context, TupleBuffer& buffer)
{
    ExecutorTelemetry::LatencyScope latency_scope{config_.telemetry, ExecutorTelemetry::Operator::NestedLoopJoin};
    if (child_count() != 2U) {
        return false;
    }

    auto* outer = outer_child();
    auto* inner = inner_child();
    if (outer == nullptr || inner == nullptr) {
        return false;
    }

    while (true) {
        if (!outer_view_valid_) {
            if (!outer->next(context, outer_buffer_)) {
                return false;
            }

            outer_view_ = TupleView::from_buffer(outer_buffer_);
            if (!outer_view_.valid()) {
                throw std::runtime_error{"NestedLoopJoinExecutor received invalid outer tuple"};
            }

            prepare_inner(outer_view_, context);
            outer_view_valid_ = true;
        }

        while (inner->next(context, inner_buffer_)) {
            auto inner_view = TupleView::from_buffer(inner_buffer_);
            if (!inner_view.valid()) {
                throw std::runtime_error{"NestedLoopJoinExecutor received invalid inner tuple"};
            }

            bool matched = true;
            if (config_.predicate) {
                matched = config_.predicate(outer_view_, inner_view, context);
            }

            if (config_.telemetry != nullptr) {
                config_.telemetry->record_nested_loop_compare(matched);
            }

            if (!matched) {
                continue;
            }

            TupleWriter writer{buffer};
            writer.reset();
            for (auto& projection : config_.projections) {
                projection(outer_view_, inner_view, writer, context);
            }
            writer.finalize();

            if (config_.telemetry != nullptr) {
                config_.telemetry->record_nested_loop_emit();
            }

            return true;
        }

        outer_view_valid_ = false;
    }
}

void NestedLoopJoinExecutor::close(ExecutorContext& context)
{
    if (child_count() != 2U) {
        return;
    }

    if (auto* inner = inner_child(); inner != nullptr && inner_open_) {
        inner->close(context);
        inner_open_ = false;
    }

    if (auto* outer = outer_child(); outer != nullptr) {
        outer->close(context);
    }

    outer_view_valid_ = false;
    outer_view_ = {};
    outer_buffer_.reset();
    inner_buffer_.reset();
}

ExecutorNode* NestedLoopJoinExecutor::outer_child() const noexcept
{
    if (child_count() <= kOuterChildIndex) {
        return nullptr;
    }
    return child(kOuterChildIndex);
}

ExecutorNode* NestedLoopJoinExecutor::inner_child() const noexcept
{
    if (child_count() <= kInnerChildIndex) {
        return nullptr;
    }
    return child(kInnerChildIndex);
}

void NestedLoopJoinExecutor::prepare_inner(const TupleView& outer_view, ExecutorContext& context)
{
    auto* inner = inner_child();
    if (inner == nullptr) {
        return;
    }

    if (inner_open_) {
        inner->close(context);
        inner_open_ = false;
    }

    if (config_.rebind_probe) {
        config_.rebind_probe(outer_view, context);
    }

    inner->open(context);
    inner_open_ = true;
    inner_buffer_.reset();
}

void NestedLoopJoinExecutor::append_view_columns(const TupleView& view, TupleWriter& writer) const
{
    const auto column_count = view.column_count();
    for (std::size_t index = 0; index < column_count; ++index) {
        const auto column = view.column(index);
        writer.append_column(column.data, column.is_null);
    }
}

}  // namespace bored::executor
