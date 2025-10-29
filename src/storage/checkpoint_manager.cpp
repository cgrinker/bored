#include "bored/storage/checkpoint_manager.hpp"

#include <limits>
#include <utility>
#include <vector>

namespace bored::storage {

CheckpointManager::CheckpointManager(std::shared_ptr<WalWriter> wal_writer) noexcept
    : wal_writer_{std::move(wal_writer)}
{
}

std::error_code CheckpointManager::emit_checkpoint(std::uint64_t checkpoint_id,
                                                   std::uint64_t redo_lsn,
                                                   std::uint64_t undo_lsn,
                                                   std::span<const WalCheckpointDirtyPageEntry> dirty_pages,
                                                   std::span<const WalCheckpointTxnEntry> active_transactions,
                                                   std::span<const WalCheckpointIndexEntry> index_metadata,
                                                   WalAppendResult& out_result) const
{
    if (!wal_writer_) {
        return std::make_error_code(std::errc::not_connected);
    }

    if (dirty_pages.size() > std::numeric_limits<std::uint32_t>::max()) {
        return std::make_error_code(std::errc::value_too_large);
    }

    if (active_transactions.size() > std::numeric_limits<std::uint32_t>::max()) {
        return std::make_error_code(std::errc::value_too_large);
    }

    if (index_metadata.size() > std::numeric_limits<std::uint32_t>::max()) {
        return std::make_error_code(std::errc::value_too_large);
    }

    WalCheckpointHeader header{};
    header.checkpoint_id = checkpoint_id;
    header.redo_lsn = redo_lsn;
    header.undo_lsn = undo_lsn;
    header.dirty_page_count = static_cast<std::uint32_t>(dirty_pages.size());
    header.active_transaction_count = static_cast<std::uint32_t>(active_transactions.size());
    header.index_metadata_count = static_cast<std::uint32_t>(index_metadata.size());

    const auto payload_size = wal_checkpoint_payload_size(dirty_pages.size(),
                                                         active_transactions.size(),
                                                         index_metadata.size());
    std::vector<std::byte> payload(payload_size);
    if (!encode_wal_checkpoint(std::span<std::byte>(payload.data(), payload.size()),
                               header,
                               dirty_pages,
                               active_transactions,
                               index_metadata)) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    WalRecordDescriptor descriptor{};
    descriptor.type = WalRecordType::Checkpoint;
    descriptor.page_id = 0U;
    descriptor.flags = WalRecordFlag::HasPayload;
    descriptor.payload = std::span<const std::byte>(payload.data(), payload.size());

    if (auto ec = wal_writer_->append_record(descriptor, out_result); ec) {
        return ec;
    }

    return {};
}

std::shared_ptr<WalWriter> CheckpointManager::wal_writer() const noexcept
{
    return wal_writer_;
}

}  // namespace bored::storage
