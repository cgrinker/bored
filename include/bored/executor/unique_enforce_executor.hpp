#pragma once

#include "bored/catalog/catalog_ids.hpp"
#include "bored/executor/executor_context.hpp"
#include "bored/executor/executor_node.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/tuple_format.hpp"
#include "bored/storage/storage_reader.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_set>
#include <vector>

namespace bored::executor {

class UniqueEnforceExecutor final : public ExecutorNode {
public:
    using KeyExtractor = std::function<bool(const TupleView&, ExecutorContext&, std::vector<std::byte>&, bool&)>;
    using IgnoreMatchPredicate = std::function<bool(const TupleView&, const storage::TableTuple&, ExecutorContext&)>;

    struct Config final {
        storage::StorageReader* reader = nullptr;
        catalog::RelationId relation_id{};
        catalog::IndexId index_id{};
        catalog::ConstraintId constraint_id{};
        std::string constraint_name{};
        bool is_primary_key = false;
        bool allow_null_keys = true;
        KeyExtractor key_extractor{};
        IgnoreMatchPredicate ignore_match{};
        ExecutorTelemetry* telemetry = nullptr;
        std::string telemetry_identifier{};
    };

    UniqueEnforceExecutor(ExecutorNodePtr child, Config config);

    void open(ExecutorContext& context) override;
    bool next(ExecutorContext& context, TupleBuffer& buffer) override;
    void close(ExecutorContext& context) override;

private:
    struct KeyHash final {
        std::size_t operator()(const std::vector<std::byte>& value) const noexcept
        {
            constexpr std::size_t kFnvOffset = 1469598103934665603ULL;
            constexpr std::size_t kFnvPrime = 1099511628211ULL;
            std::size_t hash = kFnvOffset;
            for (auto byte : value) {
                hash ^= static_cast<std::size_t>(std::to_integer<std::uint8_t>(byte));
                hash *= kFnvPrime;
            }
            return hash;
        }
    };

    struct KeyEqual final {
        bool operator()(const std::vector<std::byte>& left, const std::vector<std::byte>& right) const noexcept
        {
            return left == right;
        }
    };

    using SeenKeySet = std::unordered_set<std::vector<std::byte>, KeyHash, KeyEqual>;

    [[nodiscard]] std::string violation_message() const;

    Config config_{};
    SeenKeySet seen_keys_{};
    std::vector<std::byte> key_buffer_{};
    bool child_open_ = false;
};

}  // namespace bored::executor
