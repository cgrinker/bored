#pragma once

#include "bored/executor/executor_node.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/tuple_buffer.hpp"
#include "bored/executor/tuple_format.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

namespace bored::executor {

class AggregationExecutor final : public ExecutorNode {
public:
    using GroupKeyExtractor = std::function<std::string(const TupleView&, ExecutorContext&)>;
    using GroupProjector = std::function<void(const TupleView&, TupleWriter&, ExecutorContext&)>;

    struct AggregateDefinition final {
        std::size_t state_size = 0U;
        std::size_t state_alignment = alignof(std::max_align_t);
        std::function<void(std::span<std::byte>)> initialize{};
        std::function<void(std::span<std::byte>, const TupleView&, ExecutorContext&)> accumulate{};
        std::function<void(std::span<const std::byte>, TupleWriter&, ExecutorContext&)> project{};
    };

    struct Config final {
        GroupKeyExtractor group_key{};
        GroupProjector group_projection{};
        std::vector<AggregateDefinition> aggregates{};
        ExecutorTelemetry* telemetry = nullptr;
        std::string telemetry_identifier{};
    };

    AggregationExecutor(ExecutorNodePtr child, Config config);

    void open(ExecutorContext& context) override;
    bool next(ExecutorContext& context, TupleBuffer& buffer) override;
    void close(ExecutorContext& context) override;

private:
    struct GroupEntry final {
        std::string key;
        TupleBuffer first_row;
        std::vector<std::byte> aggregate_storage;
    };

    [[nodiscard]] ExecutorNode* child_executor() const noexcept;
    void build_groups(ExecutorContext& context);
    void reset_state();
    void initialize_layout();
    std::span<std::byte> mutable_state(GroupEntry& entry, std::size_t aggregate_index);
    std::span<const std::byte> immutable_state(const GroupEntry& entry, std::size_t aggregate_index) const;

    Config config_{};
    TupleBuffer input_buffer_{};
    std::vector<GroupEntry> groups_{};
    std::unordered_map<std::string, std::size_t> group_index_{};
    std::vector<std::size_t> state_offsets_{};
    std::size_t state_stride_ = 0U;
    std::size_t emission_index_ = 0U;
    bool child_open_ = false;
};

}  // namespace bored::executor
