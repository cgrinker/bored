#include "bored/catalog/sequence_allocator.hpp"

#include "bored/catalog/catalog_encoding.hpp"
#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_mutator.hpp"
#include "bored/catalog/catalog_relations.hpp"
#include "bored/catalog/catalog_transaction.hpp"

#include <limits>
#include <stdexcept>

namespace bored::catalog {
namespace {

[[nodiscard]] bool supports_increment(const CatalogSequenceDescriptor& descriptor) noexcept
{
    return descriptor.increment > 0;
}

[[nodiscard]] std::uint64_t safe_add(std::uint64_t value, std::uint64_t increment)
{
    if (std::numeric_limits<std::uint64_t>::max() - value < increment) {
        throw std::system_error(std::make_error_code(std::errc::value_too_large), "Sequence next value overflow");
    }
    return value + increment;
}

}  // namespace

SequenceAllocator::SequenceAllocator(Config config)
    : transaction_{config.transaction}
    , accessor_{config.accessor}
    , mutator_{config.mutator}
{
    if (transaction_ == nullptr) {
        throw std::invalid_argument{"SequenceAllocator requires an active CatalogTransaction"};
    }
    if (accessor_ == nullptr) {
        throw std::invalid_argument{"SequenceAllocator requires a CatalogAccessor"};
    }
    if (mutator_ == nullptr) {
        throw std::invalid_argument{"SequenceAllocator requires a CatalogMutator"};
    }

    mutator_->register_pre_publish_hook([this]() -> std::error_code {
        return this->stage_pending_updates();
    });
    transaction_->register_abort_hook([this]() {
        this->handle_abort();
    });
}

std::uint64_t SequenceAllocator::allocate(SequenceId sequence_id)
{
    auto& state = ensure_state(sequence_id);

    if (!supports_increment(state.descriptor)) {
        throw std::system_error(std::make_error_code(std::errc::not_supported), "Descending sequences are not supported yet");
    }

    std::uint64_t current = normalize_current(state, state.next_value);
    const auto next = compute_next_value(state, current);

    state.next_value = next;
    state.last_allocated = current;
    state.dirty = true;
    return current;
}

void SequenceAllocator::preload(SequenceId sequence_id)
{
    (void)ensure_state(sequence_id);
}

bool SequenceAllocator::has_pending_updates() const noexcept
{
    for (const auto& entry : states_) {
        if (entry.second.dirty) {
            return true;
        }
    }
    return false;
}

SequenceAllocator::SequenceState& SequenceAllocator::ensure_state(SequenceId sequence_id)
{
    auto key = sequence_id.value;
    auto it = states_.find(key);
    if (it != states_.end()) {
        return it->second;
    }

    auto descriptor = accessor_->sequence(sequence_id);
    if (!descriptor) {
        throw std::out_of_range{"SequenceAllocator could not locate sequence descriptor"};
    }

    SequenceState state{};
    state.descriptor = *descriptor;
    state.next_value = descriptor->next_value;

    auto [emplaced, _] = states_.emplace(key, std::move(state));
    return emplaced->second;
}

std::error_code SequenceAllocator::stage_pending_updates() noexcept
{
    try {
        if (states_.empty()) {
            return {};
        }

        for (auto& [_, state] : states_) {
            if (!state.dirty) {
                continue;
            }

            CatalogSequenceDescriptor before = state.descriptor;
            auto before_payload = serialize_catalog_sequence(before);

            CatalogSequenceDescriptor after = before;
            after.tuple = CatalogTupleBuilder::for_update(*transaction_, before.tuple);
            after.next_value = state.next_value;

            auto after_payload = serialize_catalog_sequence(after);

            mutator_->stage_update(kCatalogSequencesRelationId,
                                   before.sequence_id.value,
                                   before.tuple,
                                   std::move(before_payload),
                                   after.tuple,
                                   std::move(after_payload));

            state.descriptor = after;
            state.dirty = false;
        }

        return {};
    } catch (const std::system_error& ex) {
        return ex.code();
    } catch (...) {
        return std::make_error_code(std::errc::invalid_argument);
    }
}

void SequenceAllocator::handle_abort() noexcept
{
    states_.clear();
}

std::uint64_t SequenceAllocator::compute_next_value(const SequenceState& state, std::uint64_t current)
{
    const auto increment = static_cast<std::uint64_t>(state.descriptor.increment);
    auto next_candidate = safe_add(current, increment);

    if (state.descriptor.cycle && next_candidate > state.descriptor.max_value) {
        return state.descriptor.min_value;
    }

    return next_candidate;
}

std::uint64_t SequenceAllocator::normalize_current(const SequenceState& state, std::uint64_t current)
{
    if (!supports_increment(state.descriptor)) {
        throw std::system_error(std::make_error_code(std::errc::not_supported), "Descending sequences are not supported yet");
    }

    if (current < state.descriptor.min_value) {
        current = state.descriptor.min_value;
    }

    if (current > state.descriptor.max_value) {
        if (!state.descriptor.cycle) {
            throw std::system_error(std::make_error_code(std::errc::result_out_of_range), "Sequence exhausted");
        }
        current = state.descriptor.min_value;
    }

    return current;
}

}  // namespace bored::catalog
