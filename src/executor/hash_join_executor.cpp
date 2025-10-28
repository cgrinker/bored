#include "bored/executor/hash_join_executor.hpp"

#include <stdexcept>
#include <utility>

namespace bored::executor {

namespace {

constexpr std::size_t kBuildChildIndex = 0U;
constexpr std::size_t kProbeChildIndex = 1U;

}  // namespace

HashJoinExecutor::HashJoinExecutor(ExecutorNodePtr build_child, ExecutorNodePtr probe_child, Config config)
    : config_{std::move(config)}
{
    if (!build_child || !probe_child) {
        throw std::invalid_argument{"HashJoinExecutor requires both build and probe children"};
    }
    if (!config_.build_key || !config_.probe_key) {
        throw std::invalid_argument{"HashJoinExecutor requires key extractors"};
    }
    if (config_.projections.empty()) {
        config_.projections.push_back([this](const TupleView& build_view,
                                             const TupleView& probe_view,
                                             TupleWriter& writer,
                                             ExecutorContext&) {
            append_view_columns(build_view, writer);
            append_view_columns(probe_view, writer);
        });
    }
    add_child(std::move(build_child));
    add_child(std::move(probe_child));
}

void HashJoinExecutor::open(ExecutorContext& context)
{
    if (child_count() != 2U) {
        throw std::logic_error{"HashJoinExecutor expected exactly two children"};
    }

    build_rows_.clear();
    hash_table_.clear();
    current_match_indices_.clear();
    current_match_position_ = 0U;
    probe_view_valid_ = false;
    probe_view_ = {};

    auto* build = build_child();
    auto* probe = probe_child();
    if (build == nullptr || probe == nullptr) {
        throw std::logic_error{"HashJoinExecutor missing child executors"};
    }

    build->open(context);
    build_open_ = true;
    probe->open(context);
    probe_open_ = true;

    build_side(context);
}

bool HashJoinExecutor::next(ExecutorContext& context, TupleBuffer& buffer)
{
    if (child_count() != 2U) {
        return false;
    }

    auto* probe = probe_child();
    if (probe == nullptr) {
        return false;
    }

    while (true) {
        if (probe_view_valid_ && current_match_position_ < current_match_indices_.size()) {
            const auto build_index = current_match_indices_[current_match_position_++];
            const auto& stored = build_rows_[build_index];
            auto build_view = TupleView::from_buffer(stored.buffer);
            if (!build_view.valid()) {
                throw std::runtime_error{"HashJoinExecutor stored invalid build tuple"};
            }

            TupleWriter writer{buffer};
            writer.reset();
            for (auto& projection : config_.projections) {
                projection(build_view, probe_view_, writer, context);
            }
            writer.finalize();
            return true;
        }

        if (!probe->next(context, probe_buffer_)) {
            probe_view_valid_ = false;
            return false;
        }

        probe_view_ = TupleView::from_buffer(probe_buffer_);
        if (!probe_view_.valid()) {
            throw std::runtime_error{"HashJoinExecutor received invalid probe tuple"};
        }

        const auto key = config_.probe_key(probe_view_, context);
        current_match_indices_.clear();
        if (auto it = hash_table_.find(key); it != hash_table_.end()) {
            current_match_indices_ = it->second;
        }

        if (config_.telemetry != nullptr) {
            config_.telemetry->record_hash_join_probe(current_match_indices_.size());
        }

        if (current_match_indices_.empty()) {
            probe_view_valid_ = false;
            continue;
        }

        probe_view_valid_ = true;
        current_match_position_ = 0U;
    }
}

void HashJoinExecutor::close(ExecutorContext& context)
{
    if (child_count() != 2U) {
        return;
    }

    if (auto* probe = probe_child(); probe != nullptr && probe_open_) {
        probe->close(context);
        probe_open_ = false;
    }

    if (auto* build = build_child(); build != nullptr && build_open_) {
        build->close(context);
        build_open_ = false;
    }

    reset_probe_state();
    build_rows_.clear();
    hash_table_.clear();
}

ExecutorNode* HashJoinExecutor::build_child() const noexcept
{
    if (child_count() <= kBuildChildIndex) {
        return nullptr;
    }
    return child(kBuildChildIndex);
}

ExecutorNode* HashJoinExecutor::probe_child() const noexcept
{
    if (child_count() <= kProbeChildIndex) {
        return nullptr;
    }
    return child(kProbeChildIndex);
}

void HashJoinExecutor::build_side(ExecutorContext& context)
{
    auto* build = build_child();
    if (build == nullptr) {
        return;
    }

    TupleBuffer local_buffer{};
    while (build->next(context, local_buffer)) {
        auto build_view = TupleView::from_buffer(local_buffer);
        if (!build_view.valid()) {
            throw std::runtime_error{"HashJoinExecutor received invalid build tuple"};
        }

        StoredBuildTuple stored{};
        stored.key = config_.build_key(build_view, context);
        auto payload = local_buffer.span();
        stored.buffer.write(payload);
        build_rows_.push_back(std::move(stored));

        if (config_.telemetry != nullptr) {
            config_.telemetry->record_hash_join_build_row();
        }

        auto& bucket = hash_table_[build_rows_.back().key];
        bucket.push_back(build_rows_.size() - 1U);

        local_buffer.reset();
    }

    build->close(context);
    build_open_ = false;
}

void HashJoinExecutor::reset_probe_state()
{
    probe_view_valid_ = false;
    probe_view_ = {};
    current_match_indices_.clear();
    current_match_position_ = 0U;
    probe_buffer_.reset();
}

void HashJoinExecutor::append_view_columns(const TupleView& view, TupleWriter& writer) const
{
    const auto column_count = view.column_count();
    for (std::size_t index = 0; index < column_count; ++index) {
        const auto column = view.column(index);
        writer.append_column(column.data, column.is_null);
    }
}

}  // namespace bored::executor
