#include "bored/storage/wal_undo_walker.hpp"

#include "bored/storage/wal_payloads.hpp"

#include <algorithm>

namespace bored::storage {

namespace {

void add_unique_page(std::vector<std::uint32_t>& pages, std::uint32_t page_id)
{
    if (page_id == 0U) {
        return;
    }
    const auto it = std::find(pages.begin(), pages.end(), page_id);
    if (it == pages.end()) {
        pages.push_back(page_id);
    }
}

}  // namespace

WalUndoWalker::WalUndoWalker(const WalRecoveryPlan& plan) noexcept
    : plan_{&plan}
    , span_index_{0U}
{
}

void WalUndoWalker::reset() noexcept
{
    span_index_ = 0U;
}

std::optional<WalUndoWorkItem> WalUndoWalker::next()
{
    if (plan_ == nullptr || span_index_ >= plan_->undo_spans.size()) {
        return std::nullopt;
    }

    const auto& span = plan_->undo_spans[span_index_++];
    if (span.count == 0U) {
        return WalUndoWorkItem{span.owner_page_id, {}, {}};
    }

    auto begin_it = plan_->undo.begin() + static_cast<std::ptrdiff_t>(span.offset);
    auto end_it = begin_it + static_cast<std::ptrdiff_t>(span.count);
    WalUndoWorkItem item{};
    item.owner_page_id = span.owner_page_id;
    item.records = std::span<const WalRecoveryRecord>(begin_it, end_it);

    for (const auto& record : item.records) {
        const auto type = static_cast<WalRecordType>(record.header.type);
        auto payload = std::span<const std::byte>(record.payload.data(), record.payload.size());

        switch (type) {
        case WalRecordType::TupleBeforeImage: {
            auto before_view = decode_wal_tuple_before_image(payload);
            if (!before_view) {
                break;
            }
            for (const auto& chunk_view : before_view->overflow_chunks) {
                add_unique_page(item.overflow_page_ids, chunk_view.meta.overflow_page_id);
                add_unique_page(item.overflow_page_ids, chunk_view.meta.next_overflow_page_id);
            }
            break;
        }
        case WalRecordType::TupleOverflowChunk: {
            auto meta = decode_wal_overflow_chunk_meta(payload);
            if (!meta) {
                break;
            }
            add_unique_page(item.overflow_page_ids, meta->overflow_page_id);
            add_unique_page(item.overflow_page_ids, meta->next_overflow_page_id);
            break;
        }
        case WalRecordType::TupleOverflowTruncate: {
            auto meta = decode_wal_overflow_truncate_meta(payload);
            if (!meta) {
                break;
            }
            auto chunk_views = decode_wal_overflow_truncate_chunks(payload, *meta);
            if (!chunk_views) {
                break;
            }
            for (const auto& chunk_view : *chunk_views) {
                add_unique_page(item.overflow_page_ids, chunk_view.meta.overflow_page_id);
                add_unique_page(item.overflow_page_ids, chunk_view.meta.next_overflow_page_id);
            }
            break;
        }
        default:
            break;
        }
    }

    return item;
}

}  // namespace bored::storage
