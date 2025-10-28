#include "bored/executor/aggregation_executor.hpp"

#include <stdexcept>
#include <utility>

namespace bored::executor {

namespace {

constexpr std::size_t kChildIndex = 0U;

std::size_t align_offset(std::size_t offset, std::size_t alignment) noexcept
{
    if (alignment == 0U) {
        return offset;
    }
    const auto mask = alignment - 1U;
    return (offset + mask) & ~mask;
}

}  // namespace

AggregationExecutor::AggregationExecutor(ExecutorNodePtr child, Config config)
    : config_{std::move(config)}
{
    if (!child) {
        throw std::invalid_argument{"AggregationExecutor requires a child executor"};
    }
    if (!config_.group_key) {
        throw std::invalid_argument{"AggregationExecutor requires a group key extractor"};
    }
    if (config_.aggregates.empty()) {
        throw std::invalid_argument{"AggregationExecutor requires at least one aggregate"};
    }
    for (const auto& aggregate : config_.aggregates) {
        if (aggregate.state_size == 0U) {
            throw std::invalid_argument{"AggregationExecutor aggregate state size must be non-zero"};
        }
        if (!aggregate.initialize || !aggregate.accumulate || !aggregate.project) {
            throw std::invalid_argument{"AggregationExecutor aggregate requires initialize/accumulate/project callbacks"};
        }
    }

    add_child(std::move(child));
    initialize_layout();
}

void AggregationExecutor::open(ExecutorContext& context)
{
    if (child_count() != 1U) {
        throw std::logic_error{"AggregationExecutor expects exactly one child"};
    }

    reset_state();

    auto* child_exec = this->child_executor();
    if (child_exec == nullptr) {
        throw std::logic_error{"AggregationExecutor missing child executor"};
    }

    child_exec->open(context);
    child_open_ = true;

    build_groups(context);
}

bool AggregationExecutor::next(ExecutorContext& context, TupleBuffer& buffer)
{
    if (emission_index_ >= groups_.size()) {
        return false;
    }

    auto& entry = groups_[emission_index_++];
    const auto first_row_view = TupleView::from_buffer(entry.first_row);
    if (!first_row_view.valid()) {
        throw std::runtime_error{"AggregationExecutor stored invalid group row"};
    }

    TupleWriter writer{buffer};
    writer.reset();

    if (config_.group_projection) {
        config_.group_projection(first_row_view, writer, context);
    }

    for (std::size_t index = 0U; index < config_.aggregates.size(); ++index) {
        const auto state = immutable_state(entry, index);
        config_.aggregates[index].project(state, writer, context);
    }

    writer.finalize();

    if (config_.telemetry != nullptr) {
        config_.telemetry->record_aggregation_group_emitted();
    }

    return true;
}

void AggregationExecutor::close(ExecutorContext& context)
{
    if (child_open_) {
        if (auto* child_exec = this->child_executor(); child_exec != nullptr) {
            child_exec->close(context);
        }
        child_open_ = false;
    }

    reset_state();
}

ExecutorNode* AggregationExecutor::child_executor() const noexcept
{
    if (child_count() <= kChildIndex) {
        return nullptr;
    }
    return ExecutorNode::child(kChildIndex);
}

void AggregationExecutor::build_groups(ExecutorContext& context)
{
    auto* child_exec = this->child_executor();
    if (child_exec == nullptr) {
        return;
    }

    while (child_exec->next(context, input_buffer_)) {
        const auto input_view = TupleView::from_buffer(input_buffer_);
        if (!input_view.valid()) {
            throw std::runtime_error{"AggregationExecutor received invalid input tuple"};
        }

        if (config_.telemetry != nullptr) {
            config_.telemetry->record_aggregation_input_row();
        }

        const auto key = config_.group_key(input_view, context);
        GroupEntry* entry = nullptr;

        if (auto it = group_index_.find(key); it != group_index_.end()) {
            entry = &groups_[it->second];
        } else {
            group_index_.emplace(key, groups_.size());
            groups_.push_back(GroupEntry{});
            entry = &groups_.back();
            entry->key = key;
            entry->first_row.reset();
            entry->first_row.write(input_buffer_.span());
            entry->aggregate_storage.resize(state_stride_);

            for (std::size_t aggregate_index = 0U; aggregate_index < config_.aggregates.size(); ++aggregate_index) {
                auto state = mutable_state(*entry, aggregate_index);
                config_.aggregates[aggregate_index].initialize(state);
            }
        }

        for (std::size_t aggregate_index = 0U; aggregate_index < config_.aggregates.size(); ++aggregate_index) {
            auto state = mutable_state(*entry, aggregate_index);
            config_.aggregates[aggregate_index].accumulate(state, input_view, context);
        }

        input_buffer_.reset();
    }

    if (child_open_) {
        child_exec->close(context);
        child_open_ = false;
    }

    emission_index_ = 0U;
}

void AggregationExecutor::reset_state()
{
    groups_.clear();
    group_index_.clear();
    emission_index_ = 0U;
    input_buffer_.reset();
}

void AggregationExecutor::initialize_layout()
{
    state_offsets_.clear();
    state_offsets_.reserve(config_.aggregates.size());

    std::size_t offset = 0U;
    for (const auto& aggregate : config_.aggregates) {
        offset = align_offset(offset, aggregate.state_alignment);
        state_offsets_.push_back(offset);
        offset += aggregate.state_size;
    }

    state_stride_ = align_offset(offset, alignof(std::max_align_t));
}

std::span<std::byte> AggregationExecutor::mutable_state(GroupEntry& entry, std::size_t aggregate_index)
{
    const auto offset = state_offsets_[aggregate_index];
    const auto length = config_.aggregates[aggregate_index].state_size;
    return std::span<std::byte>(entry.aggregate_storage.data() + offset, length);
}

std::span<const std::byte> AggregationExecutor::immutable_state(const GroupEntry& entry, std::size_t aggregate_index) const
{
    const auto offset = state_offsets_[aggregate_index];
    const auto length = config_.aggregates[aggregate_index].state_size;
    return std::span<const std::byte>(entry.aggregate_storage.data() + offset, length);
}

}  // namespace bored::executor
