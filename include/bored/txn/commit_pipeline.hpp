#pragma once

#include "bored/txn/transaction_types.hpp"

#include <system_error>

namespace bored::txn {

struct CommitRequest final {
    TransactionId transaction_id = 0U;
    Snapshot snapshot{};
    TransactionId next_transaction_id = 0U;
    TransactionId oldest_active_transaction_id = 0U;
    CommitSequence oldest_snapshot_read_lsn = 0U;
};

struct CommitTicket final {
    TransactionId transaction_id = 0U;
    CommitSequence commit_sequence = 0U;
};

class CommitPipeline {
public:
    virtual ~CommitPipeline() = default;

    virtual std::error_code prepare_commit(const CommitRequest& request, CommitTicket& out_ticket) = 0;
    virtual std::error_code flush_commit(const CommitTicket& ticket) = 0;
    virtual void confirm_commit(const CommitTicket& ticket) = 0;
    virtual void rollback_commit(const CommitTicket& ticket) noexcept = 0;
};

class NoopCommitPipeline final : public CommitPipeline {
public:
    std::error_code prepare_commit(const CommitRequest& request, CommitTicket& out_ticket) override
    {
        out_ticket.transaction_id = request.transaction_id;
        out_ticket.commit_sequence = 0U;
        return {};
    }

    std::error_code flush_commit(const CommitTicket&) override
    {
        return {};
    }

    void confirm_commit(const CommitTicket&) override
    {
    }

    void rollback_commit(const CommitTicket&) noexcept override
    {
    }

    static NoopCommitPipeline& instance()
    {
        static NoopCommitPipeline pipeline{};
        return pipeline;
    }
};

}  // namespace bored::txn
