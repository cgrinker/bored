#pragma once

#include "bored/catalog/catalog_ids.hpp"
#include "bored/executor/executor_node.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/tuple_format.hpp"
#include "bored/storage/storage_reader.hpp"

#include <memory>
#include <string>

namespace bored::executor {

class SequentialScanExecutor final : public ExecutorNode {
public:
    struct Config final {
        bored::storage::StorageReader* reader = nullptr;
        bored::catalog::RelationId relation_id{};
        std::uint32_t root_page_id = 0U;
        ExecutorTelemetry* telemetry = nullptr;
        std::string telemetry_identifier{};
    };

    explicit SequentialScanExecutor(Config config);

    void open(ExecutorContext& context) override;
    bool next(ExecutorContext& context, TupleBuffer& buffer) override;
    void close(ExecutorContext& context) override;

private:
    Config config_{};
    std::unique_ptr<bored::storage::TableScanCursor> cursor_{};
};

}  // namespace bored::executor
