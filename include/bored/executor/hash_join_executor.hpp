#pragma once

#include "bored/executor/executor_node.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/tuple_buffer.hpp"
#include "bored/executor/tuple_format.hpp"

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace bored::executor {

class HashJoinExecutor final : public ExecutorNode {
public:
    using KeyExtractor = std::function<std::string(const TupleView&, ExecutorContext&)>;
    using Projection = std::function<void(const TupleView&, const TupleView&, TupleWriter&, ExecutorContext&)>;

    struct Config final {
        KeyExtractor build_key{};
        KeyExtractor probe_key{};
        std::vector<Projection> projections{};
        ExecutorTelemetry* telemetry = nullptr;
        std::string telemetry_identifier{};
    };

    HashJoinExecutor(ExecutorNodePtr build_child, ExecutorNodePtr probe_child, Config config);

    void open(ExecutorContext& context) override;
    bool next(ExecutorContext& context, TupleBuffer& buffer) override;
    void close(ExecutorContext& context) override;

private:
    struct StoredBuildTuple final {
        std::string key;
        TupleBuffer buffer;
    };

    [[nodiscard]] ExecutorNode* build_child() const noexcept;
    [[nodiscard]] ExecutorNode* probe_child() const noexcept;
    void build_side(ExecutorContext& context);
    void reset_probe_state();
    void append_view_columns(const TupleView& view, TupleWriter& writer) const;

    Config config_{};
    TupleBuffer probe_buffer_{};
    std::vector<StoredBuildTuple> build_rows_{};
    std::unordered_map<std::string, std::vector<std::size_t>> hash_table_{};
    std::vector<std::size_t> current_match_indices_{};
    std::size_t current_match_position_ = 0U;
    TupleView probe_view_{};
    bool probe_view_valid_ = false;
    bool probe_open_ = false;
    bool build_open_ = false;
};

}  // namespace bored::executor
