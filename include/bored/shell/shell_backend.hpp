#pragma once

#include "bored/catalog/catalog_bootstrap_ids.hpp"
#include "bored/catalog/catalog_ids.hpp"
#include "bored/catalog/catalog_ddl.hpp"
#include "bored/catalog/catalog_introspection.hpp"
#include "bored/parser/ddl_script_executor.hpp"
#include "bored/planner/physical_plan.hpp"
#include "bored/shell/shell_engine.hpp"
#include "bored/ddl/ddl_command.hpp"
#include "bored/storage/async_io.hpp"
#include "bored/storage/storage_telemetry_registry.hpp"
#include "bored/storage/wal_retention.hpp"
#include "bored/storage/wal_telemetry_registry.hpp"
#include "bored/txn/transaction_manager.hpp"
#include "bored/txn/transaction_types.hpp"

#include <chrono>
#include <functional>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <filesystem>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

namespace bored::planner {
enum class LogicalOperatorType;
enum class PhysicalOperatorType;
}  // namespace bored::planner

namespace bored::catalog {
class CatalogAccessor;
}  // namespace bored::catalog

namespace bored::ddl {
class DdlCommandDispatcher;
}

namespace bored::parser::relational {
struct TableBinding;
}  // namespace bored::parser::relational

namespace bored::shell {

class ShellTableScanCursor;
class ShellStorageReader;
class ShellValuesExecutor;
class ShellInsertTarget;
class ShellUpdateTarget;
class ShellDeleteTarget;

class ShellBackend final {
public:
    struct Config final {
        std::string default_schema_name = "analytics";
        std::optional<catalog::SchemaId> default_schema_id{};
        std::filesystem::path storage_directory{};
        std::filesystem::path wal_directory{};
        std::size_t io_worker_threads = 1U;
        std::size_t io_queue_depth = 8U;
        storage::AsyncIoBackend io_backend = storage::AsyncIoBackend::Auto;
    bool io_use_full_fsync = storage::AsyncIoConfig{}.use_full_fsync;
        std::size_t wal_retention_segments = 0U;
        std::chrono::hours wal_retention_hours{0};
        std::filesystem::path wal_archive_directory{};
    };

    ShellBackend();
    explicit ShellBackend(Config config);
    ~ShellBackend();

    ShellBackend(const ShellBackend&) = delete;
    ShellBackend& operator=(const ShellBackend&) = delete;
    ShellBackend(ShellBackend&&) = delete;
    ShellBackend& operator=(ShellBackend&&) = delete;

    [[nodiscard]] ShellEngine::Config make_config();
    [[nodiscard]] catalog::SchemaId default_schema() const noexcept { return default_schema_id_; }
    [[nodiscard]] catalog::DatabaseId default_database() const noexcept { return default_database_id_; }

private:
    struct CatalogStorage;
    struct ColumnInfo final {
        std::string name;
        catalog::CatalogColumnType type = catalog::CatalogColumnType::Unknown;
        std::size_t ordinal = 0U;
    };

    using ScalarValue = std::variant<std::int64_t, std::string>;

    struct TableData final {
        catalog::RelationId relation_id{};
        catalog::SchemaId schema_id{};
        std::string schema_name{};
        std::string table_name{};
        std::vector<ColumnInfo> columns{};
        std::unordered_map<std::string, std::size_t> column_index{};
        std::uint64_t next_row_id = 1U;
    };

    friend void append_values_payload(const TableData& table,
                                      const std::vector<ScalarValue>& values,
                                      std::vector<std::byte>& buffer);
    friend void encode_values_payload(const TableData& table,
                                      const std::vector<ScalarValue>& values,
                                      std::vector<std::byte>& buffer);
    friend bool decode_values_payload(const TableData& table,
                                      std::span<const std::byte> payload,
                                      std::vector<ScalarValue>& out_values);
    friend bool decode_row_payload(const TableData& table,
                                   std::span<const std::byte> payload,
                                   std::uint64_t& row_id,
                                   std::vector<ScalarValue>& out_values);
    friend class ShellTableScanCursor;
    friend class ShellStorageReader;
    friend class ShellValuesExecutor;
    friend class ShellInsertTarget;
    friend class ShellUpdateTarget;
    friend class ShellDeleteTarget;

    struct PlannerPlanDetails final {
        std::string root_detail{};
        std::vector<std::string> detail_lines{};
        planner::PhysicalPlan plan{};
    };

    using RelationKey = std::uint64_t;
    [[nodiscard]] static RelationKey relation_key(catalog::RelationId id) noexcept { return id.value; }
    [[nodiscard]] static std::string normalize_identifier(std::string_view text);

    [[nodiscard]] catalog::CatalogIntrospectionSnapshot collect_catalog_snapshot();
    [[nodiscard]] std::optional<catalog::DatabaseId> lookup_database(std::string_view name) const;
    [[nodiscard]] std::optional<catalog::SchemaId> lookup_schema(catalog::DatabaseId database_id,
                                                                 std::string_view name) const;
    void refresh_table_cache();
    [[nodiscard]] TableData* find_table(std::string_view qualified_name);
    [[nodiscard]] TableData* find_table_or_default_schema(std::string_view name);
    [[nodiscard]] TableData* find_table(const parser::relational::TableBinding& binding);

    [[nodiscard]] static std::vector<std::string> collect_table_columns(const TableData& table);
    [[nodiscard]] std::variant<PlannerPlanDetails, CommandMetrics> plan_scan_operation(
        const std::string& sql,
        planner::LogicalOperatorType logical_type,
        planner::PhysicalOperatorType physical_type,
        const parser::relational::TableBinding& table_binding,
        const TableData& table,
        std::string_view root_label,
        std::string_view statement_label);
    [[nodiscard]] std::variant<PlannerPlanDetails, CommandMetrics> plan_select_operation(
        const std::string& sql,
        const parser::relational::TableBinding& table_binding,
        const TableData& table,
        std::vector<std::string> projection_columns);
    [[nodiscard]] std::vector<std::string> render_executor_plan(
        std::string_view statement_label,
        const planner::PhysicalPlan& plan);

    CommandMetrics execute_dml(const std::string& sql);
    CommandMetrics execute_insert(const std::string& sql);
    CommandMetrics execute_update(const std::string& sql);
    CommandMetrics execute_delete(const std::string& sql);
    CommandMetrics execute_select(const std::string& sql);
    CommandMetrics make_error_metrics(const std::string& sql,
                                     std::string message,
                                     std::vector<std::string> hints = {});

    ddl::DdlCommandResponse handle_create_database(ddl::DdlCommandContext& context,
                                                   const ddl::CreateDatabaseRequest& request);
    ddl::DdlCommandResponse handle_drop_database(ddl::DdlCommandContext& context,
                                                 const ddl::DropDatabaseRequest& request);

    [[nodiscard]] static std::string trim(std::string_view text);
    [[nodiscard]] static std::string uppercase(std::string_view text);

    Config config_{};
    txn::TransactionIdAllocatorStub txn_allocator_{1'000U};
    txn::SnapshotManagerStub snapshot_manager_{};
    txn::TransactionManager txn_manager_;
    std::unique_ptr<catalog::CatalogIdentifierAllocator> identifier_allocator_{};
    std::unique_ptr<ddl::DdlCommandDispatcher> dispatcher_{};
    storage::StorageTelemetryRegistry storage_registry_{};
    storage::WalTelemetryRegistry wal_registry_{};
    std::unique_ptr<CatalogStorage> storage_{};
    std::unique_ptr<parser::DdlScriptExecutor> ddl_executor_{};
    catalog::SchemaId default_schema_id_{};
    catalog::DatabaseId default_database_id_{catalog::kSystemDatabaseId};
    std::uint64_t next_database_id_{catalog::kSystemDatabaseId.value + 1U};
    bool registered_storage_registry_ = false;
    bool registered_catalog_sampler_ = false;
    std::unordered_map<RelationKey, TableData> table_cache_{};
    std::unordered_map<std::string, RelationKey> table_lookup_{};
};

}  // namespace bored::shell
