#pragma once

#include "bored/ddl/ddl_telemetry.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/parser/parser_telemetry.hpp"
#include "bored/planner/planner_telemetry.hpp"
#include "bored/txn/transaction_telemetry.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>

namespace bored::storage {

struct OperationTelemetrySnapshot final {
    std::uint64_t attempts = 0U;
    std::uint64_t failures = 0U;
    std::uint64_t total_duration_ns = 0U;
    std::uint64_t last_duration_ns = 0U;
};

struct LatchTelemetrySnapshot final {
    std::uint64_t attempts = 0U;
    std::uint64_t failures = 0U;
    std::uint64_t total_wait_ns = 0U;
    std::uint64_t last_wait_ns = 0U;
};

struct PageManagerTelemetrySnapshot final {
    OperationTelemetrySnapshot initialize{};
    OperationTelemetrySnapshot insert{};
    OperationTelemetrySnapshot remove{};
    OperationTelemetrySnapshot update{};
    OperationTelemetrySnapshot compact{};
    LatchTelemetrySnapshot shared_latch{};
    LatchTelemetrySnapshot exclusive_latch{};
};

struct CheckpointTelemetrySnapshot final {
    std::uint64_t invocations = 0U;
    std::uint64_t forced_requests = 0U;
    std::uint64_t skipped_runs = 0U;
    std::uint64_t emitted_checkpoints = 0U;
    std::uint64_t emit_failures = 0U;
    std::uint64_t flush_failures = 0U;
    std::uint64_t retention_invocations = 0U;
    std::uint64_t retention_failures = 0U;
    std::uint64_t trigger_force = 0U;
    std::uint64_t trigger_first = 0U;
    std::uint64_t trigger_dirty = 0U;
    std::uint64_t trigger_active = 0U;
    std::uint64_t trigger_interval = 0U;
    std::uint64_t trigger_lsn_gap = 0U;
    std::uint64_t total_emit_duration_ns = 0U;
    std::uint64_t last_emit_duration_ns = 0U;
    std::uint64_t total_checkpoint_duration_ns = 0U;
    std::uint64_t last_checkpoint_duration_ns = 0U;
    std::uint64_t max_checkpoint_duration_ns = 0U;
    std::uint64_t total_flush_duration_ns = 0U;
    std::uint64_t last_flush_duration_ns = 0U;
    std::uint64_t total_retention_duration_ns = 0U;
    std::uint64_t last_retention_duration_ns = 0U;
    std::uint64_t total_fence_duration_ns = 0U;
    std::uint64_t last_fence_duration_ns = 0U;
    std::uint64_t max_fence_duration_ns = 0U;
    std::uint64_t coordinator_begin_calls = 0U;
    std::uint64_t coordinator_begin_failures = 0U;
    std::uint64_t coordinator_prepare_calls = 0U;
    std::uint64_t coordinator_prepare_failures = 0U;
    std::uint64_t coordinator_commit_calls = 0U;
    std::uint64_t coordinator_commit_failures = 0U;
    std::uint64_t coordinator_abort_calls = 0U;
    std::uint64_t coordinator_dry_runs = 0U;
    std::uint64_t coordinator_total_begin_duration_ns = 0U;
    std::uint64_t coordinator_last_begin_duration_ns = 0U;
    std::uint64_t coordinator_total_prepare_duration_ns = 0U;
    std::uint64_t coordinator_last_prepare_duration_ns = 0U;
    std::uint64_t coordinator_total_commit_duration_ns = 0U;
    std::uint64_t coordinator_last_commit_duration_ns = 0U;
    std::uint64_t last_checkpoint_id = 0U;
    std::uint64_t last_checkpoint_lsn = 0U;
    std::uint64_t last_checkpoint_timestamp_ns = 0U;
    std::uint64_t io_throttle_deferrals = 0U;
    std::uint64_t io_throttle_bytes_consumed = 0U;
    std::uint64_t io_throttle_budget = 0U;
    std::uint64_t queue_waits = 0U;
    std::uint64_t last_queue_depth = 0U;
    std::uint64_t max_queue_depth = 0U;
    std::uint64_t blocked_transactions = 0U;
    std::uint64_t total_blocked_duration_ns = 0U;
    std::uint64_t last_blocked_duration_ns = 0U;
    std::uint64_t max_blocked_duration_ns = 0U;
};

struct WalRetentionTelemetrySnapshot final {
    std::uint64_t invocations = 0U;
    std::uint64_t failures = 0U;
    std::uint64_t scanned_segments = 0U;
    std::uint64_t candidate_segments = 0U;
    std::uint64_t pruned_segments = 0U;
    std::uint64_t archived_segments = 0U;
    std::uint64_t total_duration_ns = 0U;
    std::uint64_t last_duration_ns = 0U;
};

struct RecoveryTelemetrySnapshot final {
    std::uint64_t plan_invocations = 0U;
    std::uint64_t plan_failures = 0U;
    std::uint64_t total_enumerate_duration_ns = 0U;
    std::uint64_t last_enumerate_duration_ns = 0U;
    std::uint64_t max_enumerate_duration_ns = 0U;
    std::uint64_t total_plan_duration_ns = 0U;
    std::uint64_t last_plan_duration_ns = 0U;
    std::uint64_t max_plan_duration_ns = 0U;
    std::uint64_t redo_invocations = 0U;
    std::uint64_t redo_failures = 0U;
    std::uint64_t total_redo_duration_ns = 0U;
    std::uint64_t last_redo_duration_ns = 0U;
    std::uint64_t max_redo_duration_ns = 0U;
    std::uint64_t undo_invocations = 0U;
    std::uint64_t undo_failures = 0U;
    std::uint64_t total_undo_duration_ns = 0U;
    std::uint64_t last_undo_duration_ns = 0U;
    std::uint64_t max_undo_duration_ns = 0U;
    std::uint64_t cleanup_invocations = 0U;
    std::uint64_t cleanup_failures = 0U;
    std::uint64_t total_cleanup_duration_ns = 0U;
    std::uint64_t last_cleanup_duration_ns = 0U;
    std::uint64_t max_cleanup_duration_ns = 0U;
    std::uint64_t last_replay_backlog_bytes = 0U;
    std::uint64_t max_replay_backlog_bytes = 0U;
};

struct IndexRetentionTelemetrySnapshot final {
    std::uint64_t scheduled_candidates = 0U;
    std::uint64_t dropped_candidates = 0U;
    std::uint64_t checkpoint_runs = 0U;
    std::uint64_t checkpoint_failures = 0U;
    std::uint64_t dispatch_batches = 0U;
    std::uint64_t dispatch_failures = 0U;
    std::uint64_t dispatched_candidates = 0U;
    std::uint64_t skipped_candidates = 0U;
    std::uint64_t pruned_candidates = 0U;
    std::uint64_t total_checkpoint_duration_ns = 0U;
    std::uint64_t last_checkpoint_duration_ns = 0U;
    std::uint64_t total_dispatch_duration_ns = 0U;
    std::uint64_t last_dispatch_duration_ns = 0U;
    std::uint64_t pending_candidates = 0U;
};

struct IndexTelemetrySnapshot final {
    OperationTelemetrySnapshot build{};
    OperationTelemetrySnapshot probe{};
    std::uint64_t mutation_attempts = 0U;
    std::uint64_t split_events = 0U;
};

struct TempCleanupTelemetrySnapshot final {
    std::uint64_t invocations = 0U;
    std::uint64_t failures = 0U;
    std::uint64_t removed_entries = 0U;
    std::uint64_t total_duration_ns = 0U;
    std::uint64_t last_duration_ns = 0U;
};

struct StorageControlTelemetrySnapshot final {
    OperationTelemetrySnapshot checkpoint{};
    OperationTelemetrySnapshot retention{};
    OperationTelemetrySnapshot recovery{};
};

struct DurabilityTelemetrySnapshot final {
    std::uint64_t last_commit_lsn = 0U;
    std::uint64_t oldest_active_commit_lsn = 0U;
    std::uint64_t last_commit_segment_id = 0U;
};

struct VacuumTelemetrySnapshot final {
    std::uint64_t scheduled_pages = 0U;
    std::uint64_t dropped_pages = 0U;
    std::uint64_t runs = 0U;
    std::uint64_t forced_runs = 0U;
    std::uint64_t skipped_runs = 0U;
    std::uint64_t batches_dispatched = 0U;
    std::uint64_t dispatch_failures = 0U;
    std::uint64_t pages_dispatched = 0U;
    std::uint64_t pending_pages = 0U;
    std::uint64_t total_dispatch_duration_ns = 0U;
    std::uint64_t last_dispatch_duration_ns = 0U;
    std::uint64_t last_safe_horizon = 0U;
};

struct CatalogTelemetrySnapshot final {
    std::uint64_t cache_hits = 0U;
    std::uint64_t cache_misses = 0U;
    std::size_t cache_relations = 0U;
    std::size_t cache_total_bytes = 0U;
    std::uint64_t published_batches = 0U;
    std::uint64_t published_mutations = 0U;
    std::uint64_t published_wal_records = 0U;
    std::uint64_t publish_failures = 0U;
    std::uint64_t aborted_batches = 0U;
    std::uint64_t aborted_mutations = 0U;
};

class StorageTelemetryRegistry final {
public:
    using PageManagerSampler = std::function<PageManagerTelemetrySnapshot()>;
    using PageManagerVisitor = std::function<void(const std::string&, const PageManagerTelemetrySnapshot&)>;

    using CheckpointSampler = std::function<CheckpointTelemetrySnapshot()>;
    using CheckpointVisitor = std::function<void(const std::string&, const CheckpointTelemetrySnapshot&)>;

    using WalRetentionSampler = std::function<WalRetentionTelemetrySnapshot()>;
    using WalRetentionVisitor = std::function<void(const std::string&, const WalRetentionTelemetrySnapshot&)>;

    using RecoverySampler = std::function<RecoveryTelemetrySnapshot()>;
    using RecoveryVisitor = std::function<void(const std::string&, const RecoveryTelemetrySnapshot&)>;

    using IndexRetentionSampler = std::function<IndexRetentionTelemetrySnapshot()>;
    using IndexRetentionVisitor = std::function<void(const std::string&, const IndexRetentionTelemetrySnapshot&)>;

    using TempCleanupSampler = std::function<TempCleanupTelemetrySnapshot()>;
    using TempCleanupVisitor = std::function<void(const std::string&, const TempCleanupTelemetrySnapshot&)>;

    using ControlSampler = std::function<StorageControlTelemetrySnapshot()>;
    using ControlVisitor = std::function<void(const std::string&, const StorageControlTelemetrySnapshot&)>;

    using DurabilitySampler = std::function<DurabilityTelemetrySnapshot()>;
    using DurabilityVisitor = std::function<void(const std::string&, const DurabilityTelemetrySnapshot&)>;

    using VacuumSampler = std::function<VacuumTelemetrySnapshot()>;
    using VacuumVisitor = std::function<void(const std::string&, const VacuumTelemetrySnapshot&)>;

    using CatalogSampler = std::function<CatalogTelemetrySnapshot()>;
    using CatalogVisitor = std::function<void(const std::string&, const CatalogTelemetrySnapshot&)>;

    using DdlSampler = std::function<ddl::DdlTelemetrySnapshot()>;
    using DdlVisitor = std::function<void(const std::string&, const ddl::DdlTelemetrySnapshot&)>;

    using ParserSampler = std::function<bored::parser::ParserTelemetrySnapshot()>;
    using ParserVisitor = std::function<void(const std::string&, const bored::parser::ParserTelemetrySnapshot&)>;

    using TransactionSampler = std::function<bored::txn::TransactionTelemetrySnapshot()>;
    using TransactionVisitor = std::function<void(const std::string&, const bored::txn::TransactionTelemetrySnapshot&)>;

    using PlannerSampler = std::function<bored::planner::PlannerTelemetrySnapshot()>;
    using PlannerVisitor = std::function<void(const std::string&, const bored::planner::PlannerTelemetrySnapshot&)>;

    using ExecutorSampler = std::function<bored::executor::ExecutorTelemetrySnapshot()>;
    using ExecutorVisitor = std::function<void(const std::string&, const bored::executor::ExecutorTelemetrySnapshot&)>;

    using IndexSampler = std::function<IndexTelemetrySnapshot()>;
    using IndexVisitor = std::function<void(const std::string&, const IndexTelemetrySnapshot&)>;

    void register_page_manager(std::string identifier, PageManagerSampler sampler);
    void unregister_page_manager(const std::string& identifier);
    PageManagerTelemetrySnapshot aggregate_page_managers() const;
    void visit_page_managers(const PageManagerVisitor& visitor) const;

    void register_checkpoint_scheduler(std::string identifier, CheckpointSampler sampler);
    void unregister_checkpoint_scheduler(const std::string& identifier);
    CheckpointTelemetrySnapshot aggregate_checkpoint_schedulers() const;
    void visit_checkpoint_schedulers(const CheckpointVisitor& visitor) const;

    void register_wal_retention(std::string identifier, WalRetentionSampler sampler);
    void unregister_wal_retention(const std::string& identifier);
    WalRetentionTelemetrySnapshot aggregate_wal_retention() const;
    void visit_wal_retention(const WalRetentionVisitor& visitor) const;

    void register_recovery(std::string identifier, RecoverySampler sampler);
    void unregister_recovery(const std::string& identifier);
    RecoveryTelemetrySnapshot aggregate_recovery() const;
    void visit_recovery(const RecoveryVisitor& visitor) const;

    void register_index_retention(std::string identifier, IndexRetentionSampler sampler);
    void unregister_index_retention(const std::string& identifier);
    IndexRetentionTelemetrySnapshot aggregate_index_retention() const;
    void visit_index_retention(const IndexRetentionVisitor& visitor) const;

    void register_temp_cleanup(std::string identifier, TempCleanupSampler sampler);
    void unregister_temp_cleanup(const std::string& identifier);
    TempCleanupTelemetrySnapshot aggregate_temp_cleanup() const;
    void visit_temp_cleanup(const TempCleanupVisitor& visitor) const;

    void register_control(std::string identifier, ControlSampler sampler);
    void unregister_control(const std::string& identifier);
    StorageControlTelemetrySnapshot aggregate_control() const;
    void visit_control(const ControlVisitor& visitor) const;

    void register_durability_horizon(std::string identifier, DurabilitySampler sampler);
    void unregister_durability_horizon(const std::string& identifier);
    DurabilityTelemetrySnapshot aggregate_durability_horizons() const;
    void visit_durability_horizons(const DurabilityVisitor& visitor) const;

    void register_vacuum(std::string identifier, VacuumSampler sampler);
    void unregister_vacuum(const std::string& identifier);
    VacuumTelemetrySnapshot aggregate_vacuums() const;
    void visit_vacuums(const VacuumVisitor& visitor) const;

    void register_catalog(std::string identifier, CatalogSampler sampler);
    void unregister_catalog(const std::string& identifier);
    CatalogTelemetrySnapshot aggregate_catalog() const;
    void visit_catalog(const CatalogVisitor& visitor) const;

    void register_index(std::string identifier, IndexSampler sampler);
    void unregister_index(const std::string& identifier);
    IndexTelemetrySnapshot aggregate_indexes() const;
    void visit_indexes(const IndexVisitor& visitor) const;

    void register_ddl(std::string identifier, DdlSampler sampler);
    void unregister_ddl(const std::string& identifier);
    ddl::DdlTelemetrySnapshot aggregate_ddl() const;
    void visit_ddl(const DdlVisitor& visitor) const;

    void register_parser(std::string identifier, ParserSampler sampler);
    void unregister_parser(const std::string& identifier);
    bored::parser::ParserTelemetrySnapshot aggregate_parser() const;
    void visit_parser(const ParserVisitor& visitor) const;

    void register_transaction(std::string identifier, TransactionSampler sampler);
    void unregister_transaction(const std::string& identifier);
    bored::txn::TransactionTelemetrySnapshot aggregate_transactions() const;
    void visit_transactions(const TransactionVisitor& visitor) const;

    void register_planner(std::string identifier, PlannerSampler sampler);
    void unregister_planner(const std::string& identifier);
    bored::planner::PlannerTelemetrySnapshot aggregate_planner() const;
    void visit_planner(const PlannerVisitor& visitor) const;

    void register_executor(std::string identifier, ExecutorSampler sampler);
    void unregister_executor(const std::string& identifier);
    bored::executor::ExecutorTelemetrySnapshot aggregate_executors() const;
    void visit_executors(const ExecutorVisitor& visitor) const;

private:
    mutable std::mutex mutex_{};
    std::unordered_map<std::string, PageManagerSampler> page_manager_samplers_{};
    std::unordered_map<std::string, CheckpointSampler> checkpoint_samplers_{};
    std::unordered_map<std::string, WalRetentionSampler> wal_retention_samplers_{};
    std::unordered_map<std::string, RecoverySampler> recovery_samplers_{};
    std::unordered_map<std::string, IndexRetentionSampler> index_retention_samplers_{};
    std::unordered_map<std::string, IndexSampler> index_samplers_{};
    std::unordered_map<std::string, TempCleanupSampler> temp_cleanup_samplers_{};
    std::unordered_map<std::string, ControlSampler> control_samplers_{};
    std::unordered_map<std::string, DurabilitySampler> durability_samplers_{};
    std::unordered_map<std::string, VacuumSampler> vacuum_samplers_{};
    std::unordered_map<std::string, CatalogSampler> catalog_samplers_{};
    std::unordered_map<std::string, DdlSampler> ddl_samplers_{};
    std::unordered_map<std::string, ParserSampler> parser_samplers_{};
    std::unordered_map<std::string, TransactionSampler> transaction_samplers_{};
    std::unordered_map<std::string, PlannerSampler> planner_samplers_{};
    std::unordered_map<std::string, ExecutorSampler> executor_samplers_{};
};

StorageTelemetryRegistry* get_global_storage_telemetry_registry() noexcept;
void set_global_storage_telemetry_registry(StorageTelemetryRegistry* registry) noexcept;

}  // namespace bored::storage
