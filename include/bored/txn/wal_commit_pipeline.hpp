#pragma once

#include "bored/storage/wal_writer.hpp"
#include "bored/txn/commit_pipeline.hpp"

#include <functional>
#include <mutex>
#include <unordered_map>

namespace bored::txn {

class WalCommitPipeline final : public CommitPipeline {
public:
    struct Hooks final {
        std::function<void(const CommitTicket&,
                           const storage::WalCommitHeader&,
                           const storage::WalAppendResult&)> on_commit_durable{};
    };

    explicit WalCommitPipeline(std::shared_ptr<storage::WalWriter> wal_writer);
    WalCommitPipeline(std::shared_ptr<storage::WalWriter> wal_writer, Hooks hooks);

    std::error_code prepare_commit(const CommitRequest& request, CommitTicket& out_ticket) override;
    std::error_code flush_commit(const CommitTicket& ticket) override;
    void confirm_commit(const CommitTicket& ticket) override;
    void rollback_commit(const CommitTicket& ticket) noexcept override;

    void set_hooks(Hooks hooks);

private:
    struct PendingCommit final {
        CommitRequest request{};
        storage::WalCommitHeader header{};
        storage::WalAppendResult append_result{};
        storage::WalStagedAppend stage{};
        bool flushed = false;
    };

    std::shared_ptr<storage::WalWriter> wal_writer_{};
    Hooks hooks_{};
    std::mutex mutex_{};
    std::unordered_map<TransactionId, PendingCommit> pending_{};
};

}  // namespace bored::txn
