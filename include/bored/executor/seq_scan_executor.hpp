#pragma once

#include "bored/catalog/catalog_ids.hpp"
#include "bored/executor/executor_node.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/tuple_format.hpp"
#include "bored/storage/storage_reader.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

namespace bored::executor {

class SequentialScanExecutor final : public ExecutorNode {
public:
    struct IndexProbe final {
        bored::catalog::IndexId index_id{};
        std::vector<std::byte> key{};
        bool unique = true;
    };

    struct Config final {
        bored::storage::StorageReader* reader = nullptr;
        bored::catalog::RelationId relation_id{};
        std::uint32_t root_page_id = 0U;
        ExecutorTelemetry* telemetry = nullptr;
        std::string telemetry_identifier{};
        std::optional<IndexProbe> index_probe{};
        bool enable_heap_fallback = true;
    };

    explicit SequentialScanExecutor(Config config);

    void open(ExecutorContext& context) override;
    bool next(ExecutorContext& context, TupleBuffer& buffer) override;
    void close(ExecutorContext& context) override;

private:
    Config config_{};
    std::unique_ptr<bored::storage::TableScanCursor> cursor_{};
    std::vector<bored::storage::IndexProbeResult> index_hits_{};
    std::size_t index_position_ = 0U;
    std::unordered_set<std::uint64_t> yielded_row_ids_{};
};

}  // namespace bored::executor
