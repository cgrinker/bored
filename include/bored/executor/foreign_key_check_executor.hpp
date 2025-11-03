#pragma once

#include "bored/catalog/catalog_ids.hpp"
#include "bored/executor/executor_context.hpp"
#include "bored/executor/executor_node.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/tuple_format.hpp"
#include "bored/storage/storage_reader.hpp"

#include <cstddef>
#include <functional>
#include <string>
#include <vector>

namespace bored::executor {

class ForeignKeyCheckExecutor final : public ExecutorNode {
public:
    using KeyExtractor = std::function<bool(const TupleView&, ExecutorContext&, std::vector<std::byte>&, bool&)>;

    struct Config final {
        storage::StorageReader* reader = nullptr;
        catalog::RelationId referencing_relation_id{};
        catalog::RelationId referenced_relation_id{};
        catalog::IndexId referenced_index_id{};
        catalog::ConstraintId constraint_id{};
        std::string constraint_name{};
        bool skip_when_null = true;
        KeyExtractor key_extractor{};
        ExecutorTelemetry* telemetry = nullptr;
        std::string telemetry_identifier{};
    };

    ForeignKeyCheckExecutor(ExecutorNodePtr child, Config config);

    void open(ExecutorContext& context) override;
    bool next(ExecutorContext& context, TupleBuffer& buffer) override;
    void close(ExecutorContext& context) override;

private:
    [[nodiscard]] std::string violation_message() const;

    Config config_{};
    std::vector<std::byte> key_buffer_{};
    bool child_open_ = false;
};

}  // namespace bored::executor
