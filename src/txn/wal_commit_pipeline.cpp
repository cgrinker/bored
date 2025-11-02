#include "bored/txn/wal_commit_pipeline.hpp"

#include "bored/storage/wal_payloads.hpp"

#include <system_error>

namespace bored::txn {

WalCommitPipeline::WalCommitPipeline(std::shared_ptr<storage::WalWriter> wal_writer)
    : WalCommitPipeline(std::move(wal_writer), Hooks{}, nullptr, {})
{
}

WalCommitPipeline::WalCommitPipeline(std::shared_ptr<storage::WalWriter> wal_writer,
                                     Hooks hooks,
                                     storage::StorageTelemetryRegistry* telemetry_registry,
                                     std::string telemetry_identifier)
    : wal_writer_{std::move(wal_writer)}
    , hooks_{std::move(hooks)}
    , telemetry_registry_{telemetry_registry}
    , telemetry_identifier_{std::move(telemetry_identifier)}
{
    if (telemetry_registry_ && !telemetry_identifier_.empty()) {
        telemetry_registry_->register_durability_horizon(telemetry_identifier_, [this] {
            return this->durability_telemetry_snapshot();
        });
    }
}

WalCommitPipeline::~WalCommitPipeline()
{
    if (telemetry_registry_ && !telemetry_identifier_.empty()) {
        telemetry_registry_->unregister_durability_horizon(telemetry_identifier_);
    }
}

void WalCommitPipeline::set_hooks(Hooks hooks)
{
    std::lock_guard guard{mutex_};
    hooks_ = std::move(hooks);
}

storage::DurabilityTelemetrySnapshot WalCommitPipeline::durability_telemetry_snapshot() const
{
    storage::DurabilityTelemetrySnapshot snapshot{};
    auto horizon = wal_writer_ ? wal_writer_->durability_horizon() : nullptr;
    if (horizon) {
        snapshot.last_commit_lsn = horizon->last_commit_lsn();
        snapshot.oldest_active_commit_lsn = horizon->oldest_active_commit_lsn();
        snapshot.last_commit_segment_id = horizon->last_commit_segment_id();
    }
    return snapshot;
}

std::error_code WalCommitPipeline::prepare_commit(const CommitRequest& request, CommitTicket& out_ticket)
{
    std::lock_guard guard{mutex_};

    if (!wal_writer_) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }

    if (pending_.find(request.transaction_id) != pending_.end()) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    PendingCommit pending{};
    pending.request = request;
    pending.header.transaction_id = request.transaction_id;
    pending.header.commit_lsn = wal_writer_->next_lsn();
    pending.header.next_transaction_id = request.next_transaction_id;
    pending.header.oldest_active_transaction_id = request.oldest_active_transaction_id;
    const auto retention_guard_lsn = request.oldest_snapshot_read_lsn != 0U
        ? request.oldest_snapshot_read_lsn
        : (request.snapshot.read_lsn != 0U ? request.snapshot.read_lsn : pending.header.commit_lsn);
    pending.header.oldest_active_commit_lsn = retention_guard_lsn;

    auto ec = wal_writer_->stage_commit_record(pending.header, pending.append_result, pending.stage);
    if (ec) {
        return ec;
    }

    out_ticket.transaction_id = request.transaction_id;
    out_ticket.commit_sequence = pending.append_result.lsn;

    pending_.emplace(request.transaction_id, std::move(pending));
    return {};
}

std::error_code WalCommitPipeline::flush_commit(const CommitTicket& ticket)
{
    std::lock_guard guard{mutex_};

    if (!wal_writer_) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }

    auto it = pending_.find(ticket.transaction_id);
    if (it == pending_.end()) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    auto ec = wal_writer_->flush();
    if (!ec) {
        it->second.flushed = true;
    }
    return ec;
}

void WalCommitPipeline::confirm_commit(const CommitTicket& ticket)
{
    std::unique_lock guard{mutex_};
    auto it = pending_.find(ticket.transaction_id);
    if (it == pending_.end()) {
        return;
    }

    const auto append_result = it->second.append_result;
    const auto header = it->second.header;
    pending_.erase(it);
    auto hooks = hooks_;
    auto horizon = wal_writer_ ? wal_writer_->durability_horizon() : nullptr;
    guard.unlock();

    if (horizon) {
        horizon->update(append_result.lsn, header.oldest_active_commit_lsn, append_result.segment_id);
    }

    if (hooks.on_commit_durable) {
        hooks.on_commit_durable(ticket, header, append_result);
    }
}

void WalCommitPipeline::rollback_commit(const CommitTicket& ticket) noexcept
{
    std::lock_guard guard{mutex_};

    if (!wal_writer_) {
        return;
    }

    auto it = pending_.find(ticket.transaction_id);
    if (it == pending_.end()) {
        return;
    }

    if (!it->second.flushed) {
        wal_writer_->rollback_staged_append(it->second.stage);
    }

    pending_.erase(it);
}

}  // namespace bored::txn
