#pragma once

#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_mutator.hpp"
#include "bored/catalog/catalog_relations.hpp"
#include "bored/catalog/catalog_transaction.hpp"

#include <cstdint>
#include <system_error>
#include <unordered_map>
#include <vector>

namespace bored::catalog {

class SequenceAllocator final {
public:
    struct Config final {
        CatalogTransaction* transaction = nullptr;
        CatalogAccessor* accessor = nullptr;
        CatalogMutator* mutator = nullptr;
    };

    explicit SequenceAllocator(Config config);

    SequenceAllocator(const SequenceAllocator&) = delete;
    SequenceAllocator& operator=(const SequenceAllocator&) = delete;
    SequenceAllocator(SequenceAllocator&&) = delete;
    SequenceAllocator& operator=(SequenceAllocator&&) = delete;

    [[nodiscard]] std::uint64_t allocate(SequenceId sequence_id);
    void preload(SequenceId sequence_id);
    [[nodiscard]] bool has_pending_updates() const noexcept;

private:
    struct SequenceState final {
        CatalogSequenceDescriptor descriptor{};
        std::uint64_t next_value = 0U;
        std::uint64_t last_allocated = 0U;
        bool dirty = false;
    };

    SequenceState& ensure_state(SequenceId sequence_id);
    std::error_code stage_pending_updates() noexcept;
    void handle_abort() noexcept;

    static std::uint64_t compute_next_value(const SequenceState& state, std::uint64_t current);
    static std::uint64_t normalize_current(const SequenceState& state, std::uint64_t current);

    CatalogTransaction* transaction_ = nullptr;
    CatalogAccessor* accessor_ = nullptr;
    CatalogMutator* mutator_ = nullptr;
    std::unordered_map<std::uint64_t, SequenceState> states_{};
};

}  // namespace bored::catalog
