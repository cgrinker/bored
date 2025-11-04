#include "bored/shell/shell_backend.hpp"

#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_bootstrapper.hpp"
#include "bored/catalog/catalog_cache.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/catalog/catalog_introspection.hpp"
#include "bored/catalog/catalog_mutator.hpp"
#include "bored/catalog/catalog_transaction.hpp"
#include "bored/catalog/catalog_mvcc.hpp"
#include "bored/catalog/sequence_allocator.hpp"
#include "bored/ddl/ddl_dispatcher.hpp"
#include "bored/ddl/ddl_handlers.hpp"
#include "bored/ddl/ddl_validation.hpp"
#include "bored/parser/ddl_command_builder.hpp"
#include "bored/parser/grammar.hpp"
#include "bored/parser/relational/binder.hpp"
#include "bored/parser/relational/catalog_binder_adapter.hpp"
#include "bored/parser/relational/logical_lowering.hpp"
#include "bored/parser/relational/logical_plan_printer.hpp"
#include "bored/planner/plan_printer.hpp"
#include "bored/planner/planner.hpp"
#include "bored/executor/delete_executor.hpp"
#include "bored/executor/executor_context.hpp"
#include "bored/executor/executor_telemetry.hpp"
#include "bored/executor/filter_executor.hpp"
#include "bored/executor/insert_executor.hpp"
#include "bored/executor/projection_executor.hpp"
#include "bored/executor/foreign_key_check_executor.hpp"
#include "bored/executor/unique_enforce_executor.hpp"
#include "bored/executor/seq_scan_executor.hpp"
#include "bored/executor/tuple_buffer.hpp"
#include "bored/executor/tuple_format.hpp"
#include "bored/executor/update_executor.hpp"
#include "bored/storage/async_io.hpp"
#include "bored/storage/free_space_map.hpp"
#include "bored/storage/page_manager.hpp"
#include "bored/storage/storage_telemetry_registry.hpp"
#include "bored/storage/storage_reader.hpp"
#include "bored/storage/wal_writer.hpp"
#include "bored/storage/wal_telemetry_registry.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <cstddef>
#include <fstream>
#include <iomanip>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <numeric>
#include <cstring>
#include <iomanip>
#include <stdexcept>
#include <system_error>
#include <optional>
#include <sstream>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace bored::shell {

using ScalarValue = std::variant<std::int64_t, std::string>;

void encode_key_from_values(const ShellBackend::TableData& table,
                            const std::vector<std::size_t>& column_indexes,
                            const std::vector<ScalarValue>& values,
                            std::vector<std::byte>& buffer);

struct ShellBackendSessionAccess final {
    static txn::TransactionContext* session_transaction_context(ShellBackend& backend) noexcept
    {
        return backend.session_transaction_context();
    }

    static void mark_session_transaction_dirty(ShellBackend& backend)
    {
        backend.mark_session_transaction_dirty();
    }

    static void mark_session_transaction_failed(ShellBackend& backend)
    {
        backend.mark_session_transaction_failed();
    }
};

namespace {

constexpr std::string_view kInsertKeyword = "INSERT";
constexpr std::string_view kUpdateKeyword = "UPDATE";
constexpr std::string_view kDeleteKeyword = "DELETE";
constexpr std::string_view kSelectKeyword = "SELECT";
constexpr std::string_view kBeginKeyword = "BEGIN";
constexpr std::string_view kCommitKeyword = "COMMIT";
constexpr std::string_view kRollbackKeyword = "ROLLBACK";

std::string trim_copy(std::string_view text)
{
    std::size_t begin = 0U;
    while (begin < text.size() && std::isspace(static_cast<unsigned char>(text[begin])) != 0) {
        ++begin;
    }
    std::size_t end = text.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(text[end - 1U])) != 0) {
        --end;
    }
    return std::string{text.substr(begin, end - begin)};
}

std::string lowercase_copy(std::string_view text)
{
    std::string result;
    result.reserve(text.size());
    for (unsigned char ch : text) {
        result.push_back(static_cast<char>(std::tolower(ch)));
    }
    return result;
}

std::string uppercase_copy(std::string_view text)
{
    std::string result;
    result.reserve(text.size());
    for (unsigned char ch : text) {
        result.push_back(static_cast<char>(std::toupper(ch)));
    }
    return result;
}

bool starts_with_ci(std::string_view text, std::string_view prefix)
{
    if (prefix.size() > text.size()) {
        return false;
    }
    for (std::size_t index = 0; index < prefix.size(); ++index) {
        const auto left = static_cast<unsigned char>(text[index]);
        const auto right = static_cast<unsigned char>(prefix[index]);
        if (std::toupper(left) != std::toupper(right)) {
            return false;
        }
    }
    return true;
}

std::size_t find_keyword_ci(std::string_view text, std::string_view keyword)
{
    const auto haystack = uppercase_copy(text);
    const auto needle = uppercase_copy(keyword);
    return haystack.find(needle);
}

bool parse_int64(std::string_view text, std::int64_t& value)
{
    auto trimmed = trim_copy(text);
    if (trimmed.empty()) {
        return false;
    }
    const char* begin = trimmed.data();
    const char* end = begin + trimmed.size();
    std::int64_t parsed = 0;
    const auto result = std::from_chars(begin, end, parsed, 10);
    if (result.ec != std::errc{} || result.ptr != end) {
        return false;
    }
    value = parsed;
    return true;
}

std::string strip_leading_comments(std::string_view text)
{
    std::size_t position = 0U;
    while (position < text.size()) {
        while (position < text.size() && std::isspace(static_cast<unsigned char>(text[position])) != 0) {
            ++position;
        }

        if (position + 1U < text.size() && text[position] == '-' && text[position + 1U] == '-') {
            position += 2U;
            while (position < text.size() && text[position] != '\n' && text[position] != '\r') {
                ++position;
            }
            continue;
        }

        if (position + 1U < text.size() && text[position] == '/' && text[position + 1U] == '*') {
            position += 2U;
            while (position + 1U < text.size() && !(text[position] == '*' && text[position + 1U] == '/')) {
                ++position;
            }
            if (position + 1U < text.size()) {
                position += 2U;
            }
            continue;
        }

        break;
    }

    return trim_copy(text.substr(position));
}

std::string strip_trailing_semicolon(std::string_view text)
{
    auto stripped = trim_copy(text);
    if (!stripped.empty() && stripped.back() == ';') {
        stripped.pop_back();
        stripped = trim_copy(stripped);
    }
    return stripped;
}

class ShellTransactionGuard final {
public:
    ShellTransactionGuard(txn::TransactionManager& manager, const txn::TransactionOptions& options)
        : manager_{manager}
        , mode_{Mode::Owned}
        , owned_context_{manager.begin(options)}
        , active_{true}
    {
    }

    ShellTransactionGuard(txn::TransactionManager& manager, txn::TransactionContext& existing)
        : manager_{manager}
        , mode_{Mode::Borrowed}
        , borrowed_context_{&existing}
    {
    }

    ShellTransactionGuard(const ShellTransactionGuard&) = delete;
    ShellTransactionGuard& operator=(const ShellTransactionGuard&) = delete;
    ShellTransactionGuard(ShellTransactionGuard&&) = delete;
    ShellTransactionGuard& operator=(ShellTransactionGuard&&) = delete;

    ~ShellTransactionGuard()
    {
        if (mode_ == Mode::Owned && active_) {
            abort();
        }
    }

    [[nodiscard]] txn::TransactionContext& context() noexcept
    {
        return mode_ == Mode::Owned ? owned_context_ : *borrowed_context_;
    }

    [[nodiscard]] const txn::TransactionContext& context() const noexcept
    {
        return mode_ == Mode::Owned ? owned_context_ : *borrowed_context_;
    }

    [[nodiscard]] bool owns_context() const noexcept { return mode_ == Mode::Owned; }

    void commit()
    {
        if (mode_ != Mode::Owned || !active_) {
            return;
        }
        manager_.commit(owned_context_);
        active_ = false;
    }

    void abort() noexcept
    {
        if (mode_ != Mode::Owned || !active_) {
            return;
        }
        try {
            manager_.abort(owned_context_);
        } catch (...) {
        }
        active_ = false;
    }

private:
    enum class Mode {
        Owned,
        Borrowed
    };

    txn::TransactionManager& manager_;
    Mode mode_ = Mode::Owned;
    txn::TransactionContext owned_context_{};
    txn::TransactionContext* borrowed_context_ = nullptr;
    bool active_ = false;
};

class TransactionWorkScope final {
public:
    TransactionWorkScope(ShellBackend& backend,
                         txn::TransactionManager& manager,
                         const txn::TransactionOptions& options)
        : backend_{backend}
    {
        if (auto* session = ShellBackendSessionAccess::session_transaction_context(backend); session != nullptr) {
            context_ = session;
            owns_context_ = false;
        } else {
            guard_ = std::make_unique<ShellTransactionGuard>(manager, options);
            context_ = &guard_->context();
            owns_context_ = true;
        }
    }

    TransactionWorkScope(const TransactionWorkScope&) = delete;
    TransactionWorkScope& operator=(const TransactionWorkScope&) = delete;

    ~TransactionWorkScope()
    {
        if (!finalized_) {
            abort();
        }
    }

    [[nodiscard]] txn::TransactionContext& context() noexcept { return *context_; }
    [[nodiscard]] bool owns_context() const noexcept { return owns_context_; }

    void commit()
    {
        if (finalized_) {
            return;
        }
        if (owns_context_) {
            guard_->commit();
        } else {
            ShellBackendSessionAccess::mark_session_transaction_dirty(backend_);
        }
        finalized_ = true;
    }

    void commit_read_only()
    {
        if (finalized_) {
            return;
        }
        if (owns_context_) {
            guard_->commit();
        }
        finalized_ = true;
    }

    void abort()
    {
        if (finalized_) {
            return;
        }
        if (owns_context_) {
            guard_->abort();
        } else {
            ShellBackendSessionAccess::mark_session_transaction_failed(backend_);
        }
        finalized_ = true;
    }

    void release() noexcept
    {
        finalized_ = true;
    }

private:
    ShellBackend& backend_;
    std::unique_ptr<ShellTransactionGuard> guard_{};
    txn::TransactionContext* context_ = nullptr;
    bool owns_context_ = false;
    bool finalized_ = false;
};

class CatalogTransactionGuard final {
public:
    explicit CatalogTransactionGuard(catalog::CatalogTransaction& transaction) noexcept
        : transaction_{transaction}
    {
    }

    CatalogTransactionGuard(const CatalogTransactionGuard&) = delete;
    CatalogTransactionGuard& operator=(const CatalogTransactionGuard&) = delete;
    CatalogTransactionGuard(CatalogTransactionGuard&&) = delete;
    CatalogTransactionGuard& operator=(CatalogTransactionGuard&&) = delete;

    ~CatalogTransactionGuard()
    {
        if (!completed_) {
            (void)transaction_.abort();
        }
    }

    std::error_code commit()
    {
        auto ec = transaction_.commit();
        if (!ec) {
            completed_ = true;
        }
        return ec;
    }

    void release() noexcept { completed_ = true; }

private:
    catalog::CatalogTransaction& transaction_;
    bool completed_ = false;
};

std::optional<std::string> last_identifier_part(const parser::relational::QualifiedName& name)
{
    if (name.parts.empty()) {
        return std::nullopt;
    }
    return name.parts.back().value;
}

std::optional<std::string> leading_identifier_part(const parser::relational::QualifiedName& name)
{
    if (name.parts.size() < 2U) {
        return std::nullopt;
    }
    return name.parts.front().value;
}

struct SimpleSelectPlan final {
    std::optional<std::string> schema_name{};
    std::string table_name{};
    bool select_all = false;
    std::vector<std::string> column_names{};
    std::optional<std::string> order_by_column{};
};

std::optional<SimpleSelectPlan> build_simple_select_plan(const parser::relational::SelectStatement& statement,
                                                         std::string& error,
                                                         std::vector<std::string>& hints)
{
    const auto* query = statement.query;
    if (query == nullptr) {
        error = "SELECT query body is missing.";
        hints = {"Ensure the statement includes a valid SELECT projection and FROM clause."};
        return std::nullopt;
    }

    if (query->distinct) {
        error = "DISTINCT is not supported in bored_shell SELECT statements.";
        hints = {"Remove DISTINCT or wait until executor-backed queries are available."};
        return std::nullopt;
    }

    if (query->from == nullptr || query->from_tables.size() != 1U) {
        error = "SELECT requires exactly one FROM table.";
        hints = {"Rewrite the query to reference a single table without joins."};
        return std::nullopt;
    }

    if (!query->joins.empty()) {
        error = "JOIN clauses are not supported in bored_shell SELECT statements.";
        hints = {"Remove JOIN clauses or wait for planner/executor integration."};
        return std::nullopt;
    }

    if (query->where != nullptr) {
        error = "WHERE clauses are not yet supported in bored_shell SELECT statements.";
        hints = {"Remove the WHERE clause or defer to the executor-backed pipeline."};
        return std::nullopt;
    }

    if (!query->group_by.empty()) {
        error = "GROUP BY is not supported in bored_shell SELECT statements.";
        hints = {"Remove GROUP BY from the query."};
        return std::nullopt;
    }

    if (query->limit != nullptr) {
        error = "LIMIT/OFFSET are not supported in bored_shell SELECT statements.";
        hints = {"Omit LIMIT/OFFSET clauses for now."};
        return std::nullopt;
    }

    const auto* table_ref = query->from;
    if (table_ref == nullptr) {
        error = "SELECT requires a FROM clause.";
        hints = {"Specify a table after the FROM keyword."};
        return std::nullopt;
    }

    if (table_ref->name.parts.size() > 2U) {
        error = "Fully qualified tables must be specified as schema.table.";
        hints = {"Remove database-level qualifiers from the table reference."};
        return std::nullopt;
    }

    auto table_name = last_identifier_part(table_ref->name);
    if (!table_name.has_value()) {
        error = "FROM clause must reference a table name.";
        hints = {"Provide a table identifier after the FROM keyword."};
        return std::nullopt;
    }

    SimpleSelectPlan plan{};
    plan.table_name = std::move(*table_name);
    plan.schema_name = leading_identifier_part(table_ref->name);

    if (query->select_items.empty()) {
        error = "SELECT list is empty.";
        hints = {"List one or more columns or use * to select all columns."};
        return std::nullopt;
    }

    for (const auto* item : query->select_items) {
        if (item == nullptr || item->expression == nullptr) {
            error = "SELECT item is missing an expression.";
            hints = {"Specify a column name in the SELECT list."};
            return std::nullopt;
        }

        if (item->alias.has_value()) {
            error = "Column aliases are not supported in bored_shell SELECT statements.";
            hints = {"Remove AS clauses from the SELECT list."};
            return std::nullopt;
        }

        switch (item->expression->kind) {
        case parser::relational::NodeKind::StarExpression: {
            const auto& star = static_cast<const parser::relational::StarExpression&>(*item->expression);
            if (!star.qualifier.parts.empty()) {
                error = "Qualified STAR expressions (table.*) are not supported.";
                hints = {"Use SELECT * or list columns explicitly."};
                return std::nullopt;
            }
            if (!plan.column_names.empty()) {
                error = "Cannot mix '*' with explicit column names.";
                hints = {"Use either '*' or an explicit column list."};
                return std::nullopt;
            }
            plan.select_all = true;
            break;
        }
        case parser::relational::NodeKind::IdentifierExpression: {
            if (plan.select_all) {
                error = "Cannot mix '*' with explicit column names.";
                hints = {"Use either '*' or an explicit column list."};
                return std::nullopt;
            }
            const auto& identifier = static_cast<const parser::relational::IdentifierExpression&>(*item->expression);
            auto column_name = last_identifier_part(identifier.name);
            if (!column_name.has_value()) {
                error = "Column reference is empty.";
                hints = {"List a valid column name in the SELECT list."};
                return std::nullopt;
            }
            plan.column_names.push_back(std::move(*column_name));
            break;
        }
        default:
            error = "Only column names are supported in bored_shell SELECT statements.";
            hints = {"Replace expressions with direct column references."};
            return std::nullopt;
        }
    }

    if (!query->order_by.empty()) {
        if (query->order_by.size() > 1U) {
            error = "Only a single ORDER BY column is supported.";
            hints = {"Remove additional ORDER BY expressions."};
            return std::nullopt;
        }

        const auto* order_item = query->order_by.front();
        if (order_item == nullptr || order_item->expression == nullptr) {
            error = "ORDER BY column reference is missing.";
            hints = {"Specify a column name in the ORDER BY clause."};
            return std::nullopt;
        }
        if (order_item->direction != parser::relational::OrderByItem::Direction::Ascending) {
            error = "DESC ORDER BY is not supported.";
            hints = {"Use ascending order or omit ORDER BY."};
            return std::nullopt;
        }
        if (order_item->expression->kind != parser::relational::NodeKind::IdentifierExpression) {
            error = "ORDER BY must reference a column name.";
            hints = {"Replace the ORDER BY expression with a column reference."};
            return std::nullopt;
        }
        const auto& order_identifier = static_cast<const parser::relational::IdentifierExpression&>(*order_item->expression);
        auto order_column = last_identifier_part(order_identifier.name);
        if (!order_column.has_value()) {
            error = "ORDER BY column reference is empty.";
            hints = {"Specify a valid column name in ORDER BY."};
            return std::nullopt;
        }
        plan.order_by_column = std::move(*order_column);
    }

    return plan;
}

CommandMetrics make_parser_error_metrics(const std::string& sql,
                                         std::vector<parser::ParserDiagnostic> diagnostics,
                                         std::string summary)
{
    CommandMetrics metrics{};
    metrics.success = false;
    metrics.summary = std::move(summary);
    if (metrics.summary.empty() && !diagnostics.empty()) {
        metrics.summary = diagnostics.front().message;
    }
    if (metrics.summary.empty()) {
        metrics.summary = "Failed to parse SQL statement.";
    }
    metrics.diagnostics = std::move(diagnostics);
    (void)sql;
    return metrics;
}

CommandMetrics make_planner_error_metrics(const std::string& sql,
                                          std::vector<std::string> diagnostics,
                                          std::string summary)
{
    std::vector<parser::ParserDiagnostic> parser_diagnostics;
    parser_diagnostics.reserve(diagnostics.size());
    for (auto& message : diagnostics) {
        parser::ParserDiagnostic diagnostic{};
        diagnostic.severity = parser::ParserSeverity::Error;
        diagnostic.message = std::move(message);
        parser_diagnostics.push_back(std::move(diagnostic));
    }
    return make_parser_error_metrics(sql, std::move(parser_diagnostics), std::move(summary));
}

std::string format_relation_name(const parser::relational::TableBinding& binding)
{
    if (!binding.schema_name.empty()) {
        return binding.schema_name + "." + binding.table_name;
    }
    return binding.table_name;
}

std::string_view physical_operator_name(planner::PhysicalOperatorType type) noexcept
{
    using planner::PhysicalOperatorType;
    switch (type) {
    case PhysicalOperatorType::NoOp:
        return "NoOp";
    case PhysicalOperatorType::Projection:
        return "Projection";
    case PhysicalOperatorType::Filter:
        return "Filter";
    case PhysicalOperatorType::SeqScan:
        return "SeqScan";
    case PhysicalOperatorType::NestedLoopJoin:
        return "NestedLoopJoin";
    case PhysicalOperatorType::HashJoin:
        return "HashJoin";
    case PhysicalOperatorType::Values:
        return "Values";
    case PhysicalOperatorType::Insert:
        return "Insert";
    case PhysicalOperatorType::Update:
        return "Update";
    case PhysicalOperatorType::Delete:
        return "Delete";
    case PhysicalOperatorType::UniqueEnforce:
        return "UniqueEnforce";
    case PhysicalOperatorType::ForeignKeyCheck:
        return "ForeignKeyCheck";
    default:
        return "Unknown";
    }
}

std::string join_strings(const std::vector<std::string>& values, std::string_view separator)
{
    std::string result;
    bool first = true;
    for (const auto& value : values) {
        if (!first) {
            result.append(separator);
        }
        result.append(value);
        first = false;
    }
    return result;
}

std::string_view logical_operator_kind_name(parser::relational::LogicalOperatorKind kind) noexcept
{
    using parser::relational::LogicalOperatorKind;
    switch (kind) {
    case LogicalOperatorKind::Scan:
        return "Scan";
    case LogicalOperatorKind::Project:
        return "Project";
    case LogicalOperatorKind::Filter:
        return "Filter";
    case LogicalOperatorKind::Join:
        return "Join";
    case LogicalOperatorKind::Aggregate:
        return "Aggregate";
    case LogicalOperatorKind::Sort:
        return "Sort";
    case LogicalOperatorKind::Limit:
        return "Limit";
    default:
        return "Unknown";
    }
}

std::vector<std::string> render_logical_plan_lines(const parser::relational::LogicalOperator& root)
{
    std::vector<std::string> lines;
    lines.push_back("logical.root=" + std::string(logical_operator_kind_name(root.kind)));

    const auto plan_text = parser::relational::describe_plan(root);
    std::istringstream stream{plan_text};
    std::string line;
    while (std::getline(stream, line)) {
        if (line.empty()) {
            continue;
        }
        lines.push_back("logical.plan: " + line);
    }

    return lines;
}

}  // namespace

struct InMemoryIdentifierAllocator final : catalog::CatalogIdentifierAllocator {
    catalog::SchemaId allocate_schema_id() override { return catalog::SchemaId{++schema_ids_}; }
    catalog::RelationId allocate_table_id() override { return catalog::RelationId{++table_ids_}; }
    catalog::IndexId allocate_index_id() override { return catalog::IndexId{++index_ids_}; }
    catalog::ColumnId allocate_column_id() override { return catalog::ColumnId{++column_ids_}; }

    std::uint64_t schema_ids_ = 1'000U;
    std::uint64_t table_ids_ = 2'000U;
    std::uint64_t index_ids_ = 3'000U;
    std::uint64_t column_ids_ = 4'000U;
};

struct ShellBackend::CatalogStorage final {
    using Relation = std::map<std::uint64_t, std::vector<std::byte>>;

    CatalogStorage(const Config& config,
                   storage::StorageTelemetryRegistry* storage_registry,
                   storage::WalTelemetryRegistry* wal_registry)
        : config_{config}
        , storage_registry_{storage_registry}
        , wal_registry_{wal_registry}
    {}

    ~CatalogStorage()
    {
        shutdown();
    }

    std::error_code initialize()
    {
        if (initialized_) {
            return {};
        }

        if (config_.storage_directory.empty()) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        std::error_code ec;
        std::filesystem::create_directories(config_.storage_directory, ec);
        if (ec) {
            return ec;
        }

        wal_directory_ = config_.wal_directory.empty()
            ? (config_.storage_directory / "wal")
            : config_.wal_directory;

        std::filesystem::create_directories(wal_directory_, ec);
        if (ec) {
            return ec;
        }

        page_directory_ = config_.storage_directory / "catalog_pages";
        std::filesystem::create_directories(page_directory_, ec);
        if (ec) {
            return ec;
        }

        table_directory_ = config_.storage_directory / "table_data";
        std::filesystem::create_directories(table_directory_, ec);
        if (ec) {
            return ec;
        }

    storage::AsyncIoConfig io_config{};
    io_config.worker_threads = std::max<std::size_t>(1U, config_.io_worker_threads);
    io_config.queue_depth = std::max<std::size_t>(1U, config_.io_queue_depth);
    io_config.backend = config_.io_backend;
    io_config.use_full_fsync = config_.io_use_full_fsync;
        auto async = storage::create_async_io(io_config);
        if (!async) {
            return std::make_error_code(std::errc::not_enough_memory);
        }

        async_io_ = std::shared_ptr<storage::AsyncIo>(async.release(), [](storage::AsyncIo* io) {
            if (io != nullptr) {
                io->shutdown();
                delete io;
            }
        });

        storage::WalWriterConfig wal_config{};
        wal_config.directory = wal_directory_;
        wal_config.buffer_size = 4U * storage::kWalBlockSize;
        wal_config.segment_size = storage::kWalSegmentSize;
        wal_config.start_lsn = storage::kWalBlockSize;
        wal_config.telemetry_registry = wal_registry_;
        wal_config.storage_telemetry_registry = storage_registry_;
        wal_config.retention.retention_segments = config_.wal_retention_segments;
        wal_config.retention.retention_hours = config_.wal_retention_hours;
        if (!config_.wal_archive_directory.empty()) {
            wal_config.retention.archive_path = config_.wal_archive_directory;
        }
        wal_config.telemetry_identifier = "shell.catalog_wal";

        try {
            wal_writer_ = std::make_shared<storage::WalWriter>(async_io_, wal_config);
        } catch (const std::system_error& error) {
            return error.code();
        } catch (...) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        storage::PageManager::Config page_config{};
        page_config.telemetry_registry = storage_registry_;
        page_config.telemetry_identifier = "shell.catalog_page_manager";

        page_manager_ = std::make_unique<storage::PageManager>(&fsm_, wal_writer_, page_config);

        register_page_manager_sampler();

        relations_.clear();
        relation_pages_.clear();
        page_buffers_.clear();

        catalog::CatalogBootstrapper bootstrapper({page_manager_.get(), &fsm_, true});
        catalog::CatalogBootstrapArtifacts artifacts;
        if (auto bootstrap_ec = bootstrapper.run(artifacts); bootstrap_ec) {
            return bootstrap_ec;
        }

        for (const auto& entry : artifacts.pages) {
            page_buffers_[entry.first] = entry.second;
        }

        reload_catalog_relations();
        persist_all_pages();
        load_user_tables_from_disk();

        initialized_ = true;
        return {};
    }

    void shutdown() noexcept
    {
        if (storage_registry_ && page_manager_registered_) {
            storage_registry_->unregister_page_manager(page_manager_identifier_);
            page_manager_identifier_.clear();
            page_manager_registered_ = false;
        }

        persist_all_pages();
        persist_all_user_tables();

        if (wal_writer_) {
            (void)wal_writer_->close();
            wal_writer_.reset();
        }

        page_manager_.reset();
        async_io_.reset();
        relation_pages_.clear();
        page_buffers_.clear();
        page_directory_.clear();
        table_directory_.clear();
        initialized_ = false;
    }

    void apply(const catalog::CatalogMutationBatch& batch)
    {
        for (const auto& mutation : batch.mutations) {
            if (!is_catalog_relation(mutation.relation_id)) {
                continue;
            }
            auto page_it = relation_pages_.find(mutation.relation_id.value);
            if (page_it == relation_pages_.end()) {
                continue;
            }
            auto buffer_it = page_buffers_.find(page_it->second);
            if (buffer_it == page_buffers_.end()) {
                continue;
            }

            auto page_span = std::span<std::byte>(buffer_it->second.data(), buffer_it->second.size());

            switch (mutation.kind) {
            case catalog::CatalogMutationKind::Insert:
                if (mutation.after) {
                    storage::PageManager::TupleInsertResult insert_result{};
                    (void)page_manager_->insert_tuple(page_span,
                                                      std::span<const std::byte>(mutation.after->payload.data(),
                                                                                 mutation.after->payload.size()),
                                                      mutation.row_id,
                                                      insert_result);
                }
                break;
            case catalog::CatalogMutationKind::Update:
                if (mutation.after) {
                    if (auto slot = find_slot(mutation.relation_id, page_span, mutation.row_id)) {
                        storage::PageManager::TupleUpdateResult update_result{};
                        (void)page_manager_->update_tuple(page_span,
                                                          *slot,
                                                          std::span<const std::byte>(mutation.after->payload.data(),
                                                                                     mutation.after->payload.size()),
                                                          mutation.row_id,
                                                          update_result);
                    }
                }
                break;
            case catalog::CatalogMutationKind::Delete:
                if (auto slot = find_slot(mutation.relation_id, page_span, mutation.row_id)) {
                    storage::PageManager::TupleDeleteResult delete_result{};
                    (void)page_manager_->delete_tuple(page_span, *slot, mutation.row_id, delete_result);
                }
                break;
            }
            refresh_relation_cache(mutation.relation_id);
            persist_relation_page(mutation.relation_id);
        }
    }

    [[nodiscard]] catalog::CatalogAccessor::RelationScanner make_scanner() const
    {
        return [this](catalog::RelationId relation_id, const catalog::CatalogAccessor::TupleCallback& callback) {
            auto it = relations_.find(relation_id.value);
            if (it == relations_.end()) {
                return;
            }
            for (const auto& [row_id, payload] : it->second) {
                (void)row_id;
                callback(std::span<const std::byte>(payload.data(), payload.size()));
            }
        };
    }

    [[nodiscard]] std::optional<catalog::CatalogDatabaseView> database(std::string_view name) const
    {
        auto it = relations_.find(catalog::kCatalogDatabasesRelationId.value);
        if (it == relations_.end()) {
            return std::nullopt;
        }
        for (const auto& entry : it->second) {
            const auto view = catalog::decode_catalog_database(std::span<const std::byte>(entry.second.data(), entry.second.size()));
            if (view && view->name == name) {
                return view;
            }
        }
        return std::nullopt;
    }

    [[nodiscard]] std::vector<catalog::CatalogDatabaseView> list_databases() const
    {
        std::vector<catalog::CatalogDatabaseView> result;
        auto it = relations_.find(catalog::kCatalogDatabasesRelationId.value);
        if (it == relations_.end()) {
            return result;
        }
        for (const auto& entry : it->second) {
            auto view = catalog::decode_catalog_database(std::span<const std::byte>(entry.second.data(), entry.second.size()));
            if (view) {
                result.push_back(*view);
            }
        }
        return result;
    }

    [[nodiscard]] std::optional<catalog::CatalogSchemaView> schema(catalog::DatabaseId database_id,
                                                                   std::string_view name) const
    {
        auto it = relations_.find(catalog::kCatalogSchemasRelationId.value);
        if (it == relations_.end()) {
            return std::nullopt;
        }
        for (const auto& entry : it->second) {
            const auto view = catalog::decode_catalog_schema(std::span<const std::byte>(entry.second.data(), entry.second.size()));
            if (view && view->database_id == database_id && view->name == name) {
                return view;
            }
        }
        return std::nullopt;
    }

    [[nodiscard]] std::vector<catalog::CatalogSchemaView> list_schemas() const
    {
        std::vector<catalog::CatalogSchemaView> result;
        auto it = relations_.find(catalog::kCatalogSchemasRelationId.value);
        if (it == relations_.end()) {
            return result;
        }
        for (const auto& entry : it->second) {
            auto view = catalog::decode_catalog_schema(std::span<const std::byte>(entry.second.data(), entry.second.size()));
            if (view) {
                result.push_back(*view);
            }
        }
        return result;
    }

    [[nodiscard]] std::vector<catalog::CatalogTableView> list_tables() const
    {
        std::vector<catalog::CatalogTableView> result;
        auto it = relations_.find(catalog::kCatalogTablesRelationId.value);
        if (it == relations_.end()) {
            return result;
        }
        for (const auto& entry : it->second) {
            auto view = catalog::decode_catalog_table(std::span<const std::byte>(entry.second.data(), entry.second.size()));
            if (view) {
                result.push_back(*view);
            }
        }
        return result;
    }

    [[nodiscard]] std::vector<catalog::CatalogColumnView> list_columns() const
    {
        std::vector<catalog::CatalogColumnView> result;
        auto it = relations_.find(catalog::kCatalogColumnsRelationId.value);
        if (it == relations_.end()) {
            return result;
        }
        for (const auto& entry : it->second) {
            auto view = catalog::decode_catalog_column(std::span<const std::byte>(entry.second.data(), entry.second.size()));
            if (view) {
                result.push_back(*view);
            }
        }
        return result;
    }

    void erase_row(catalog::RelationId relation_id, std::uint64_t row_id)
    {
        auto it = relations_.find(relation_id.value);
        if (it == relations_.end()) {
            return;
        }
        it->second.erase(row_id);
        persist_table_rows(relation_id);
    }

    [[nodiscard]] const Relation* relation(catalog::RelationId relation_id) const
    {
        auto it = relations_.find(relation_id.value);
        if (it == relations_.end()) {
            return nullptr;
        }
        return &it->second;
    }

    void upsert_row(catalog::RelationId relation_id, std::uint64_t row_id, std::span<const std::byte> payload)
    {
        auto& relation = relations_[relation_id.value];
        relation[row_id] = std::vector<std::byte>(payload.begin(), payload.end());
        persist_table_rows(relation_id);
    }

    bool fetch_row(catalog::RelationId relation_id,
                   std::uint64_t row_id,
                   std::vector<std::byte>& out_payload) const
    {
        auto it = relations_.find(relation_id.value);
        if (it == relations_.end()) {
            return false;
        }
        auto row_it = it->second.find(row_id);
        if (row_it == it->second.end()) {
            return false;
        }
        out_payload = row_it->second;
        return true;
    }

    bool remove_row(catalog::RelationId relation_id,
                    std::uint64_t row_id,
                    std::vector<std::byte>* out_payload)
    {
        auto it = relations_.find(relation_id.value);
        if (it == relations_.end()) {
            return false;
        }
        auto row_it = it->second.find(row_id);
        if (row_it == it->second.end()) {
            return false;
        }
        if (out_payload != nullptr) {
            *out_payload = row_it->second;
        }
        it->second.erase(row_it);
        persist_table_rows(relation_id);
        return true;
    }

    [[nodiscard]] std::uint64_t max_row_id(catalog::RelationId relation_id) const
    {
        auto it = relations_.find(relation_id.value);
        if (it == relations_.end() || it->second.empty()) {
            return 0U;
        }
        return it->second.rbegin()->first;
    }

    void register_page_manager_sampler()
    {
        if (!storage_registry_ || page_manager_registered_) {
            return;
        }
        page_manager_identifier_ = "shell.catalog";
        storage_registry_->register_page_manager(page_manager_identifier_, [this]() {
            if (this->page_manager_) {
                return this->page_manager_->telemetry_snapshot();
            }
            return storage::PageManagerTelemetrySnapshot{};
        });
        page_manager_registered_ = true;
    }

    void reload_catalog_relations()
    {
        register_catalog_relation(catalog::kCatalogDatabasesRelationId, catalog::kCatalogDatabasesPageId);
        register_catalog_relation(catalog::kCatalogSchemasRelationId, catalog::kCatalogSchemasPageId);
        register_catalog_relation(catalog::kCatalogTablesRelationId, catalog::kCatalogTablesPageId);
        register_catalog_relation(catalog::kCatalogColumnsRelationId, catalog::kCatalogColumnsPageId);
        register_catalog_relation(catalog::kCatalogIndexesRelationId, catalog::kCatalogIndexesPageId);
    }

    void register_catalog_relation(catalog::RelationId relation_id, std::uint32_t page_id)
    {
        relation_pages_[relation_id.value] = page_id;
        auto buffer_it = page_buffers_.find(page_id);
        if (buffer_it == page_buffers_.end()) {
            std::array<std::byte, storage::kPageSize> buffer{};
            buffer.fill(std::byte{0});
            buffer_it = page_buffers_.emplace(page_id, buffer).first;
            auto span = std::span<std::byte>(buffer_it->second.data(), buffer_it->second.size());
            (void)page_manager_->initialize_page(span, storage::PageType::Meta, page_id);
        }

        const bool loaded = load_page_from_disk(page_id, buffer_it->second);
        if (!loaded) {
            auto span = std::span<std::byte>(buffer_it->second.data(), buffer_it->second.size());
            if (storage::page_header(span).page_id != page_id) {
                (void)page_manager_->initialize_page(span, storage::PageType::Meta, page_id);
            }
        }

        refresh_relation_cache(relation_id);
        if (!loaded) {
            persist_relation_page(relation_id);
        }
    }

    void refresh_relation_cache(catalog::RelationId relation_id)
    {
        auto page_it = relation_pages_.find(relation_id.value);
        if (page_it == relation_pages_.end()) {
            return;
        }
        auto buffer_it = page_buffers_.find(page_it->second);
        if (buffer_it == page_buffers_.end()) {
            return;
        }

        auto page_span = std::span<const std::byte>(buffer_it->second.data(), buffer_it->second.size());
        auto& relation = relations_[relation_id.value];
        relation.clear();

        const auto& header = storage::page_header(page_span);
        for (std::uint16_t slot = 0U; slot < header.tuple_count; ++slot) {
            auto tuple = storage::read_tuple(page_span, slot);
            if (tuple.empty()) {
                continue;
            }
            auto row_id = extract_row_id(relation_id, tuple);
            if (!row_id) {
                continue;
            }
            relation[*row_id] = std::vector<std::byte>(tuple.begin(), tuple.end());
        }
    }

    static bool is_catalog_relation(catalog::RelationId relation_id)
    {
        switch (relation_id.value) {
        case catalog::kCatalogDatabasesRelationId.value:
        case catalog::kCatalogSchemasRelationId.value:
        case catalog::kCatalogTablesRelationId.value:
        case catalog::kCatalogColumnsRelationId.value:
        case catalog::kCatalogIndexesRelationId.value:
            return true;
        default:
            return false;
        }
    }

    static std::optional<std::uint64_t> extract_row_id(catalog::RelationId relation_id,
                                                       std::span<const std::byte> tuple)
    {
        using namespace catalog;
        if (relation_id == kCatalogDatabasesRelationId) {
            auto view = decode_catalog_database(tuple);
            if (!view) {
                return std::nullopt;
            }
            return view->database_id.value;
        }
        if (relation_id == kCatalogSchemasRelationId) {
            auto view = decode_catalog_schema(tuple);
            if (!view) {
                return std::nullopt;
            }
            return view->schema_id.value;
        }
        if (relation_id == kCatalogTablesRelationId) {
            auto view = decode_catalog_table(tuple);
            if (!view) {
                return std::nullopt;
            }
            return view->relation_id.value;
        }
        if (relation_id == kCatalogColumnsRelationId) {
            auto view = decode_catalog_column(tuple);
            if (!view) {
                return std::nullopt;
            }
            return view->column_id.value;
        }
        if (relation_id == kCatalogIndexesRelationId) {
            auto view = decode_catalog_index(tuple);
            if (!view) {
                return std::nullopt;
            }
            return view->index_id.value;
        }
        return std::nullopt;
    }

    std::optional<std::uint16_t> find_slot(catalog::RelationId relation_id,
                                           std::span<const std::byte> page_span,
                                           std::uint64_t row_id) const
    {
        const auto& header = storage::page_header(page_span);
        for (std::uint16_t slot = 0U; slot < header.tuple_count; ++slot) {
            auto tuple = storage::read_tuple(page_span, slot);
            if (tuple.empty()) {
                continue;
            }
            auto candidate = extract_row_id(relation_id, tuple);
            if (candidate && *candidate == row_id) {
                return slot;
            }
        }
        return std::nullopt;
    }

    [[nodiscard]] std::filesystem::path page_file_path(std::uint32_t page_id) const
    {
        if (page_directory_.empty()) {
            return {};
        }
        return page_directory_ / ("page_" + std::to_string(page_id) + ".bin");
    }

    bool load_page_from_disk(std::uint32_t page_id, std::array<std::byte, storage::kPageSize>& buffer)
    {
        const auto path = page_file_path(page_id);
        if (path.empty()) {
            return false;
        }
        std::ifstream input{path, std::ios::binary};
        if (!input.is_open()) {
            return false;
        }
        input.read(reinterpret_cast<char*>(buffer.data()), static_cast<std::streamsize>(buffer.size()));
        if (input.gcount() != static_cast<std::streamsize>(buffer.size()) || input.fail()) {
            buffer.fill(std::byte{0});
            return false;
        }
        return true;
    }

    void persist_page_to_disk(std::uint32_t page_id, const std::array<std::byte, storage::kPageSize>& buffer) const
    {
        const auto path = page_file_path(page_id);
        if (path.empty()) {
            return;
        }
        std::ofstream output{path, std::ios::binary | std::ios::trunc};
        if (!output.is_open()) {
            return;
        }
        output.write(reinterpret_cast<const char*>(buffer.data()), static_cast<std::streamsize>(buffer.size()));
        output.flush();
    }

    void persist_relation_page(catalog::RelationId relation_id)
    {
        auto page_it = relation_pages_.find(relation_id.value);
        if (page_it == relation_pages_.end()) {
            return;
        }
        auto buffer_it = page_buffers_.find(page_it->second);
        if (buffer_it == page_buffers_.end()) {
            return;
        }
        persist_page_to_disk(page_it->second, buffer_it->second);
    }

    void persist_all_pages()
    {
        for (const auto& [page_id, buffer] : page_buffers_) {
            persist_page_to_disk(page_id, buffer);
        }
    }

    [[nodiscard]] std::filesystem::path table_file_path(catalog::RelationId relation_id) const
    {
        if (table_directory_.empty()) {
            return {};
        }
        return table_directory_ / ("table_" + std::to_string(relation_id.value) + ".bin");
    }

    void persist_table_rows(catalog::RelationId relation_id)
    {
        if (is_catalog_relation(relation_id)) {
            return;
        }

        const auto path = table_file_path(relation_id);
        if (path.empty()) {
            return;
        }

        auto relation_it = relations_.find(relation_id.value);
        if (relation_it == relations_.end() || relation_it->second.empty()) {
            std::error_code ec;
            std::filesystem::remove(path, ec);
            return;
        }

        std::ofstream output{path, std::ios::binary | std::ios::trunc};
        if (!output.is_open()) {
            return;
        }

        for (const auto& [row_id, payload] : relation_it->second) {
            const std::uint64_t id = row_id;
            const std::uint32_t size = static_cast<std::uint32_t>(payload.size());
            output.write(reinterpret_cast<const char*>(&id), sizeof(id));
            output.write(reinterpret_cast<const char*>(&size), sizeof(size));
            if (size > 0U) {
                output.write(reinterpret_cast<const char*>(payload.data()), static_cast<std::streamsize>(size));
            }
        }

        output.flush();
    }

    void persist_all_user_tables()
    {
        for (const auto& [relation_key, relation] : relations_) {
            (void)relation;
            catalog::RelationId relation_id{relation_key};
            if (is_catalog_relation(relation_id)) {
                continue;
            }
            persist_table_rows(relation_id);
        }
    }

    void load_table_from_disk(catalog::RelationId relation_id)
    {
        if (is_catalog_relation(relation_id)) {
            return;
        }

        const auto path = table_file_path(relation_id);
        if (path.empty()) {
            return;
        }

        std::ifstream input{path, std::ios::binary};
        if (!input.is_open()) {
            relations_.erase(relation_id.value);
            return;
        }

        Relation relation;
        while (true) {
            std::uint64_t row_id = 0U;
            std::uint32_t payload_size = 0U;
            if (!input.read(reinterpret_cast<char*>(&row_id), sizeof(row_id))) {
                break;
            }
            if (!input.read(reinterpret_cast<char*>(&payload_size), sizeof(payload_size))) {
                relation.clear();
                relations_.erase(relation_id.value);
                return;
            }
            std::vector<std::byte> payload(payload_size);
            if (payload_size > 0U) {
                if (!input.read(reinterpret_cast<char*>(payload.data()), static_cast<std::streamsize>(payload_size))) {
                    relation.clear();
                    relations_.erase(relation_id.value);
                    return;
                }
            }
            relation[row_id] = std::move(payload);
        }

        relations_[relation_id.value] = std::move(relation);
    }

    void load_user_tables_from_disk()
    {
        if (table_directory_.empty()) {
            return;
        }

        const auto tables = list_tables();
        for (const auto& table : tables) {
            load_table_from_disk(table.relation_id);
        }
    }

    Config config_{};
    storage::StorageTelemetryRegistry* storage_registry_ = nullptr;
    storage::WalTelemetryRegistry* wal_registry_ = nullptr;
    std::shared_ptr<storage::AsyncIo> async_io_{};
    std::shared_ptr<storage::WalWriter> wal_writer_{};
    storage::FreeSpaceMap fsm_{};
    std::unique_ptr<storage::PageManager> page_manager_{};
    std::unordered_map<std::uint64_t, Relation> relations_{};
    std::unordered_map<std::uint64_t, std::uint32_t> relation_pages_{};
    std::unordered_map<std::uint32_t, std::array<std::byte, storage::kPageSize>> page_buffers_{};
    std::filesystem::path wal_directory_{};
    std::filesystem::path page_directory_{};
    std::filesystem::path table_directory_{};
    bool initialized_ = false;
    bool page_manager_registered_ = false;
    std::string page_manager_identifier_{};
};

namespace {

[[nodiscard]] txn::Snapshot make_relaxed_snapshot() noexcept
{
    txn::Snapshot snapshot{};
    snapshot.xmin = 1U;
    snapshot.xmax = std::numeric_limits<std::uint64_t>::max();
    return snapshot;
}

std::string format_count(std::string_view noun, std::size_t count)
{
    std::ostringstream stream;
    stream << count << ' ' << noun;
    if (count != 1U) {
        stream << 's';
    }
    return stream.str();
}

std::string value_to_string(const ScalarValue& value)
{
    if (std::holds_alternative<std::int64_t>(value)) {
        return std::to_string(std::get<std::int64_t>(value));
    }
    return std::get<std::string>(value);
}

int compare_values(const ScalarValue& lhs, const ScalarValue& rhs)
{
    if (lhs.index() != rhs.index()) {
        return lhs.index() < rhs.index() ? -1 : 1;
    }
    if (std::holds_alternative<std::int64_t>(lhs)) {
        const auto left = std::get<std::int64_t>(lhs);
        const auto right = std::get<std::int64_t>(rhs);
        if (left < right) {
            return -1;
        }
        if (left > right) {
            return 1;
        }
        return 0;
    }
    const auto& left = std::get<std::string>(lhs);
    const auto& right = std::get<std::string>(rhs);
    if (left < right) {
        return -1;
    }
    if (left > right) {
        return 1;
    }
    return 0;
}

std::vector<std::string> split_column_list(std::string_view text)
{
    std::vector<std::string> columns;
    std::size_t offset = 0U;
    while (offset < text.size()) {
        const auto next = text.find(',', offset);
        const auto piece = text.substr(offset, next == std::string_view::npos ? text.size() - offset : next - offset);
        auto trimmed = trim_copy(piece);
        if (!trimmed.empty()) {
            columns.push_back(std::move(trimmed));
        }
        if (next == std::string_view::npos) {
            break;
        }
        offset = next + 1U;
    }
    return columns;
}

template <typename KeyExtractor>
KeyExtractor make_payload_key_extractor(const ShellBackend::TableData* table,
                                        std::vector<std::size_t> column_indexes,
                                        std::size_t payload_column)
{
    auto lambda = [table,
                   payload_column,
                   column_indexes = std::move(column_indexes),
                   decoded = std::vector<ScalarValue>{},
                   encoded = std::vector<std::byte>{}](const bored::executor::TupleView& view,
                                                       bored::executor::ExecutorContext&,
                                                       std::vector<std::byte>& out_key,
                                                       bool& has_null) mutable -> bool {
        if (payload_column >= view.column_count()) {
            return false;
        }

        const auto payload_column_view = view.column(payload_column);
        has_null = payload_column_view.is_null;
        if (payload_column_view.is_null) {
            out_key.clear();
            return true;
        }

        if (!decode_values_payload(*table, payload_column_view.data, decoded)) {
            return false;
        }

        encode_key_from_values(*table, column_indexes, decoded, encoded);
        out_key.assign(encoded.begin(), encoded.end());
        return true;
    };

    return KeyExtractor{lambda};
}

bored::executor::UniqueEnforceExecutor::IgnoreMatchPredicate make_row_id_ignore_predicate(std::size_t row_id_column_index)
{
    return [row_id_column_index](const bored::executor::TupleView& view,
                                 const bored::storage::TableTuple& tuple,
                                 bored::executor::ExecutorContext&) {
        if (row_id_column_index >= view.column_count()) {
            return false;
        }

        const auto row_id_column = view.column(row_id_column_index);
        if (row_id_column.is_null || row_id_column.data.size() != sizeof(std::uint64_t)) {
            return false;
        }

        std::uint64_t current_row_id = 0U;
        std::memcpy(&current_row_id, row_id_column.data.data(), sizeof(current_row_id));

        if (tuple.payload.size() < sizeof(std::uint64_t)) {
            return false;
        }

        std::uint64_t matched_row_id = 0U;
        std::memcpy(&matched_row_id, tuple.payload.data(), sizeof(matched_row_id));
        return current_row_id == matched_row_id;
    };
}

}  // namespace

constexpr std::size_t kNumericWidth = sizeof(std::int64_t);
constexpr std::size_t kLengthFieldWidth = sizeof(std::uint32_t);

void append_bytes(std::vector<std::byte>& buffer, const void* data, std::size_t size)
{
    const auto* bytes = static_cast<const std::byte*>(data);
    buffer.insert(buffer.end(), bytes, bytes + size);
}

void append_uint64(std::vector<std::byte>& buffer, std::uint64_t value)
{
    append_bytes(buffer, &value, sizeof(value));
}

void append_uint32(std::vector<std::byte>& buffer, std::uint32_t value)
{
    append_bytes(buffer, &value, sizeof(value));
}

void append_values_payload(const ShellBackend::TableData& table,
                           const std::vector<ScalarValue>& values,
                           std::vector<std::byte>& buffer)
{
    if (values.size() != table.columns.size()) {
        throw std::runtime_error{"Value count does not match table column count"};
    }

    for (std::size_t index = 0; index < table.columns.size(); ++index) {
        const auto& column = table.columns[index];
        const auto& value = values[index];
        switch (column.type) {
        case catalog::CatalogColumnType::Utf8: {
            if (!std::holds_alternative<std::string>(value)) {
                throw std::runtime_error{"Expected string value for UTF8 column"};
            }
            const auto& text = std::get<std::string>(value);
            if (text.size() > std::numeric_limits<std::uint32_t>::max()) {
                throw std::runtime_error{"String value exceeds maximum supported length"};
            }
            append_uint32(buffer, static_cast<std::uint32_t>(text.size()));
            append_bytes(buffer, text.data(), text.size());
            break;
        }
        case catalog::CatalogColumnType::Int64:
        case catalog::CatalogColumnType::UInt16:
        case catalog::CatalogColumnType::UInt32:
        case catalog::CatalogColumnType::Unknown:
        default: {
            if (!std::holds_alternative<std::int64_t>(value)) {
                throw std::runtime_error{"Expected numeric value for integer column"};
            }
            const auto numeric = std::get<std::int64_t>(value);
            append_bytes(buffer, &numeric, sizeof(numeric));
            break;
        }
        }
    }
}

void encode_key_from_values(const ShellBackend::TableData& table,
                            const std::vector<std::size_t>& column_indexes,
                            const std::vector<ScalarValue>& values,
                            std::vector<std::byte>& buffer)
{
    buffer.clear();
    for (auto index : column_indexes) {
        if (index >= table.columns.size() || index >= values.size()) {
            throw std::runtime_error{"Constraint key column index out of bounds"};
        }
        const auto& column = table.columns[index];
        const auto& value = values[index];
        switch (column.type) {
        case catalog::CatalogColumnType::Utf8: {
            if (!std::holds_alternative<std::string>(value)) {
                throw std::runtime_error{"Expected string value for constraint key column"};
            }
            const auto& text = std::get<std::string>(value);
            if (text.size() > std::numeric_limits<std::uint32_t>::max()) {
                throw std::runtime_error{"String value exceeds maximum supported length"};
            }
            append_uint32(buffer, static_cast<std::uint32_t>(text.size()));
            append_bytes(buffer, text.data(), text.size());
            break;
        }
        case catalog::CatalogColumnType::Int64:
        case catalog::CatalogColumnType::UInt16:
        case catalog::CatalogColumnType::UInt32:
        case catalog::CatalogColumnType::Unknown:
        default: {
            if (!std::holds_alternative<std::int64_t>(value)) {
                throw std::runtime_error{"Expected numeric value for constraint key column"};
            }
            const auto numeric = std::get<std::int64_t>(value);
            append_bytes(buffer, &numeric, sizeof(numeric));
            break;
        }
        }
    }
}

void encode_values_payload(const ShellBackend::TableData& table,
                           const std::vector<ScalarValue>& values,
                           std::vector<std::byte>& buffer)
{
    buffer.clear();
    append_values_payload(table, values, buffer);
}

bool decode_values_payload(const ShellBackend::TableData& table,
                           std::span<const std::byte> payload,
                           std::vector<ScalarValue>& out_values)
{
    out_values.clear();
    out_values.reserve(table.columns.size());

    std::size_t offset = 0U;
    for (const auto& column : table.columns) {
        if (offset >= payload.size()) {
            return false;
        }

        switch (column.type) {
        case catalog::CatalogColumnType::Utf8: {
            if (offset + kLengthFieldWidth > payload.size()) {
                return false;
            }
            std::uint32_t length = 0U;
            std::memcpy(&length, payload.data() + offset, kLengthFieldWidth);
            offset += kLengthFieldWidth;
            if (offset + length > payload.size()) {
                return false;
            }
            std::string text(length, '\0');
            if (length > 0U) {
                std::memcpy(text.data(), payload.data() + offset, length);
            }
            offset += length;
            out_values.emplace_back(std::move(text));
            break;
        }
        case catalog::CatalogColumnType::Int64:
        case catalog::CatalogColumnType::UInt16:
        case catalog::CatalogColumnType::UInt32:
        case catalog::CatalogColumnType::Unknown:
        default: {
            if (offset + kNumericWidth > payload.size()) {
                return false;
            }
            std::int64_t numeric = 0;
            std::memcpy(&numeric, payload.data() + offset, kNumericWidth);
            offset += kNumericWidth;
            out_values.emplace_back(numeric);
            break;
        }
        }
    }

    return offset <= payload.size();
}

bool decode_row_payload(const ShellBackend::TableData& table,
                        std::span<const std::byte> payload,
                        std::uint64_t& row_id,
                        std::vector<ScalarValue>& out_values)
{
    if (payload.size() < sizeof(std::uint64_t)) {
        return false;
    }
    std::memcpy(&row_id, payload.data(), sizeof(row_id));
    const auto values_payload = payload.subspan(sizeof(row_id));
    return decode_values_payload(table, values_payload, out_values);
}

class ShellTableScanCursor final : public bored::storage::TableScanCursor {
public:
    ShellTableScanCursor(const ShellBackend::TableData* table,
                         const ShellBackend::CatalogStorage* storage)
        : table_{table}
        , storage_{storage}
    {
        reset();
    }

    bool next(bored::storage::TableTuple& out_tuple) override
    {
        if (relation_ == nullptr) {
            return false;
        }
        if (iterator_ == end_iterator_) {
            return false;
        }

        const auto& [row_id, payload] = *iterator_;
        payload_buffer_.clear();
        append_uint64(payload_buffer_, row_id);
        payload_buffer_.insert(payload_buffer_.end(), payload.begin(), payload.end());

        current_header_ = {};
        current_header_.created_transaction_id = 1U;
        current_header_.deleted_transaction_id = 0U;

        out_tuple.header = current_header_;
        out_tuple.payload = std::span<const std::byte>(payload_buffer_.data(), payload_buffer_.size());
        out_tuple.page_id = 1U;
        out_tuple.slot_id = static_cast<std::uint16_t>((slot_index_ % std::numeric_limits<std::uint16_t>::max()) + 1U);

        ++iterator_;
        ++slot_index_;
        return true;
    }

    void reset() override
    {
        if (storage_ == nullptr || table_ == nullptr) {
            relation_ = nullptr;
            iterator_ = ShellBackend::CatalogStorage::Relation::const_iterator{};
            end_iterator_ = ShellBackend::CatalogStorage::Relation::const_iterator{};
            slot_index_ = 0U;
            return;
        }

        relation_ = storage_->relation(table_->relation_id);
        if (relation_ == nullptr) {
            iterator_ = ShellBackend::CatalogStorage::Relation::const_iterator{};
            end_iterator_ = ShellBackend::CatalogStorage::Relation::const_iterator{};
            slot_index_ = 0U;
            return;
        }

        iterator_ = relation_->begin();
        end_iterator_ = relation_->end();
        slot_index_ = 0U;
    }

private:
    const ShellBackend::TableData* table_ = nullptr;
    const ShellBackend::CatalogStorage* storage_ = nullptr;
    const ShellBackend::CatalogStorage::Relation* relation_ = nullptr;
    ShellBackend::CatalogStorage::Relation::const_iterator iterator_{};
    ShellBackend::CatalogStorage::Relation::const_iterator end_iterator_{};
    std::size_t slot_index_ = 0U;
    std::vector<std::byte> payload_buffer_{};
    bored::storage::TupleHeader current_header_{};
};

class ShellStorageReader final : public bored::storage::StorageReader {
public:
    struct IndexDefinition final {
        const ShellBackend::TableData* table = nullptr;
        std::vector<std::size_t> column_indexes{};
    };

    ShellStorageReader(const ShellBackend::TableData* table,
                       const ShellBackend::CatalogStorage* storage,
                       std::unordered_map<std::uint64_t, IndexDefinition> index_definitions = {})
        : table_{table}
        , storage_{storage}
        , index_definitions_{std::move(index_definitions)}
    {}

    [[nodiscard]] std::unique_ptr<bored::storage::TableScanCursor> create_table_scan(
        const bored::storage::TableScanConfig&) override
    {
        if (table_ == nullptr) {
            return std::make_unique<EmptyTableScanCursor>();
        }
        return std::make_unique<ShellTableScanCursor>(table_, storage_);
    }

    std::vector<bored::storage::IndexProbeResult> probe_index(
        const bored::storage::IndexProbeConfig& config) override
    {
        auto it = index_definitions_.find(config.index_id.value);
        if (it == index_definitions_.end()) {
            return {};
        }

        const auto& definition = it->second;
        if (definition.table == nullptr || storage_ == nullptr) {
            return {};
        }

        const auto* relation = storage_->relation(definition.table->relation_id);
        if (relation == nullptr) {
            return {};
        }

        result_payloads_.clear();
        std::vector<bored::storage::IndexProbeResult> results;

        for (const auto& [row_id, payload] : *relation) {
            decode_buffer_.clear();
            if (!decode_values_payload(*definition.table,
                                       std::span<const std::byte>(payload.data(), payload.size()),
                                       decode_buffer_)) {
                continue;
            }

            key_buffer_.clear();
            encode_key_from_values(*definition.table, definition.column_indexes, decode_buffer_, key_buffer_);
            if (key_buffer_.size() != config.key.size()) {
                continue;
            }
            if (!std::equal(key_buffer_.begin(), key_buffer_.end(), config.key.begin())) {
                continue;
            }

            result_payloads_.emplace_back();
            auto& stored = result_payloads_.back();
            stored.resize(sizeof(std::uint64_t) + payload.size());
            std::memcpy(stored.data(), &row_id, sizeof(row_id));
            if (!payload.empty()) {
                std::memcpy(stored.data() + sizeof(std::uint64_t), payload.data(), payload.size());
            }

            bored::storage::IndexProbeResult result{};
            result.tuple.header.created_transaction_id = 1U;
            result.tuple.header.deleted_transaction_id = 0U;
            result.tuple.payload = std::span<const std::byte>(stored.data(), stored.size());
            result.tuple.page_id = 0U;
            result.tuple.slot_id = 0U;
            results.push_back(result);
        }

        return results;
    }

private:
    class EmptyTableScanCursor final : public bored::storage::TableScanCursor {
    public:
        bool next(bored::storage::TableTuple&) override { return false; }
        void reset() override {}
    };

    const ShellBackend::TableData* table_ = nullptr;
    const ShellBackend::CatalogStorage* storage_ = nullptr;
    std::unordered_map<std::uint64_t, IndexDefinition> index_definitions_{};
    std::vector<ScalarValue> decode_buffer_{};
    std::vector<std::byte> key_buffer_{};
    std::vector<std::vector<std::byte>> result_payloads_{};
};

std::variant<ShellBackend::ConstraintEnforcementPlan, ShellBackend::ConstraintPlanError>
ShellBackend::build_constraint_enforcement_plan(ShellBackend::TableData& table,
                                                catalog::CatalogAccessor& accessor)
{
    ConstraintEnforcementPlan plan{};

    const auto descriptors = accessor.constraints(table.relation_id);
    if (descriptors.empty()) {
        return plan;
    }

    const auto resolve_index_id = [](const catalog::CatalogConstraintDescriptor& descriptor) {
        if (descriptor.backing_index_id.is_valid()) {
            return descriptor.backing_index_id;
        }
        return catalog::IndexId{descriptor.constraint_id.value};
    };

    const auto map_columns = [](const ShellBackend::TableData& target,
                                const std::vector<std::string>& columns,
                                std::string& missing) -> std::optional<std::vector<std::size_t>> {
        std::vector<std::size_t> indexes;
        indexes.reserve(columns.size());
        for (const auto& name : columns) {
            const auto key = lowercase_copy(trim_copy(name));
            auto it = target.column_index.find(key);
            if (it == target.column_index.end()) {
                missing = name;
                return std::nullopt;
            }
            indexes.push_back(it->second);
        }
        return indexes;
    };

    for (const auto& descriptor : descriptors) {
        switch (descriptor.constraint_type) {
        case catalog::CatalogConstraintType::PrimaryKey:
        case catalog::CatalogConstraintType::Unique: {
            const auto column_names = split_column_list(descriptor.key_columns);
            std::string missing_column;
            auto indexes = map_columns(table, column_names, missing_column);
            if (!indexes) {
                return ConstraintPlanError{
                    "Constraint '" + std::string(descriptor.name) + "' references unknown column '" + missing_column + "'.",
                    {"Ensure the constraint definition matches the table schema."}
                };
            }

            UniqueConstraintPlan unique{};
            unique.constraint_id = descriptor.constraint_id;
            unique.index_id = resolve_index_id(descriptor);
            unique.name = std::string(descriptor.name);
            unique.is_primary_key = descriptor.constraint_type == catalog::CatalogConstraintType::PrimaryKey;
            unique.allow_null_keys = !unique.is_primary_key;
            unique.column_indexes = std::move(*indexes);
            unique.telemetry_identifier = unique.name;

            ConstraintIndexDefinition definition{};
            definition.table = &table;
            definition.column_indexes = unique.column_indexes;
            auto [it, inserted] = plan.index_definitions.emplace(unique.index_id.value, definition);
            if (!inserted) {
                if (it->second.table != definition.table || it->second.column_indexes != definition.column_indexes) {
                    return ConstraintPlanError{
                        "Conflicting index definition for constraint '" + unique.name + "'.",
                        {}
                    };
                }
            }

            plan.unique_constraints.push_back(std::move(unique));
            break;
        }
        case catalog::CatalogConstraintType::ForeignKey: {
            const auto referencing_columns = split_column_list(descriptor.key_columns);
            const auto referenced_columns = split_column_list(descriptor.referenced_columns);
            if (referencing_columns.size() != referenced_columns.size()) {
                return ConstraintPlanError{
                    "Constraint '" + std::string(descriptor.name) + "' has mismatched referencing and referenced column counts.",
                    {"Update the foreign key definition to align column counts."}
                };
            }

            std::string missing_column;
            auto referencing_indexes = map_columns(table, referencing_columns, missing_column);
            if (!referencing_indexes) {
                return ConstraintPlanError{
                    "Constraint '" + std::string(descriptor.name) + "' references unknown column '" + missing_column + "'.",
                    {"Ensure the foreign key references existing columns on the table."}
                };
            }

            auto* referenced_table = find_table(descriptor.referenced_relation_id);
            if (referenced_table == nullptr) {
                return ConstraintPlanError{
                    "Constraint '" + std::string(descriptor.name) + "' targets an unknown referenced table.",
                    {"Refresh catalog metadata and retry the command."}
                };
            }

            auto referenced_indexes = map_columns(*referenced_table, referenced_columns, missing_column);
            if (!referenced_indexes) {
                return ConstraintPlanError{
                    "Constraint '" + std::string(descriptor.name) + "' references unknown column '" + missing_column + "' on the referenced table.",
                    {"Ensure the referenced table exposes the expected columns."}
                };
            }

            ForeignKeyConstraintPlan fk{};
            fk.constraint_id = descriptor.constraint_id;
            fk.index_id = resolve_index_id(descriptor);
            fk.name = std::string(descriptor.name);
            fk.referencing_column_indexes = std::move(*referencing_indexes);
            fk.referenced_table = referenced_table;
            fk.referenced_column_indexes = std::move(*referenced_indexes);
            fk.telemetry_identifier = fk.name;
            fk.skip_when_null = true;

            ConstraintIndexDefinition definition{};
            definition.table = referenced_table;
            definition.column_indexes = fk.referenced_column_indexes;
            auto [it, inserted] = plan.index_definitions.emplace(fk.index_id.value, definition);
            if (!inserted) {
                if (it->second.table != definition.table || it->second.column_indexes != definition.column_indexes) {
                    return ConstraintPlanError{
                        "Conflicting index definition for constraint '" + fk.name + "'.",
                        {}
                    };
                }
            }

            plan.foreign_key_constraints.push_back(std::move(fk));
            break;
        }
        default:
            break;
        }
    }

    const auto constraint_order = [](const auto& lhs, const auto& rhs) {
        return lhs.constraint_id.value < rhs.constraint_id.value;
    };
    std::sort(plan.foreign_key_constraints.begin(), plan.foreign_key_constraints.end(), constraint_order);
    std::sort(plan.unique_constraints.begin(), plan.unique_constraints.end(), constraint_order);

    return plan;
}

bored::executor::ExecutorNodePtr ShellBackend::apply_constraint_enforcers(
    bored::executor::ExecutorNodePtr child,
    const ConstraintEnforcementPlan& plan,
    ShellStorageReader* reader,
    TableData* table,
    bored::executor::ExecutorTelemetry& telemetry,
    std::size_t payload_column,
    std::optional<std::size_t> row_id_column)
{
    if (!child) {
        return child;
    }

    if ((!plan.foreign_key_constraints.empty() || !plan.unique_constraints.empty()) && reader == nullptr) {
        throw std::logic_error{"Constraint enforcement requires a storage reader"};
    }

    for (const auto& fk : plan.foreign_key_constraints) {
        bored::executor::ForeignKeyCheckExecutor::Config config{};
        config.reader = reader;
        config.referencing_relation_id = table->relation_id;
        config.referenced_relation_id = fk.referenced_table->relation_id;
        config.referenced_index_id = fk.index_id;
        config.constraint_id = fk.constraint_id;
        config.constraint_name = fk.name;
        config.skip_when_null = fk.skip_when_null;
        config.telemetry = &telemetry;
        config.telemetry_identifier = fk.telemetry_identifier;
        config.key_extractor = make_payload_key_extractor<bored::executor::ForeignKeyCheckExecutor::KeyExtractor>(
            table,
            fk.referencing_column_indexes,
            payload_column);
        child = std::make_unique<bored::executor::ForeignKeyCheckExecutor>(std::move(child), std::move(config));
    }

    for (const auto& unique : plan.unique_constraints) {
        bored::executor::UniqueEnforceExecutor::Config config{};
        config.reader = reader;
        config.relation_id = table->relation_id;
        config.index_id = unique.index_id;
        config.constraint_id = unique.constraint_id;
        config.constraint_name = unique.name;
        config.is_primary_key = unique.is_primary_key;
        config.allow_null_keys = unique.allow_null_keys;
        config.telemetry = &telemetry;
        config.telemetry_identifier = unique.telemetry_identifier;
        config.key_extractor = make_payload_key_extractor<bored::executor::UniqueEnforceExecutor::KeyExtractor>(
            table,
            unique.column_indexes,
            payload_column);
        if (row_id_column.has_value()) {
            config.ignore_match = make_row_id_ignore_predicate(*row_id_column);
        }
        child = std::make_unique<bored::executor::UniqueEnforceExecutor>(std::move(child), std::move(config));
    }

    return child;
}

class ShellValuesExecutor final : public bored::executor::ExecutorNode {
public:
    ShellValuesExecutor(const ShellBackend::TableData* table,
                        std::vector<std::vector<ScalarValue>> rows)
        : table_{table}
        , rows_{std::move(rows)}
    {}

    void open(bored::executor::ExecutorContext&) override { index_ = 0U; }

    bool next(bored::executor::ExecutorContext&, bored::executor::TupleBuffer& buffer) override
    {
        if (table_ == nullptr || index_ >= rows_.size()) {
            return false;
        }

        bored::executor::TupleWriter writer{buffer};
        writer.reset();

        encode_values_payload(*table_, rows_[index_++], payload_buffer_);
        writer.append_column(std::span<const std::byte>(payload_buffer_.data(), payload_buffer_.size()), false);
        writer.finalize();
        return true;
    }

    void close(bored::executor::ExecutorContext&) override { index_ = 0U; }

private:
    const ShellBackend::TableData* table_ = nullptr;
    std::vector<std::vector<ScalarValue>> rows_{};
    std::vector<std::byte> payload_buffer_{};
    std::size_t index_ = 0U;
};

class ShellInsertTarget final : public bored::executor::InsertExecutor::Target {
public:
    ShellInsertTarget(ShellBackend::TableData* table, ShellBackend::CatalogStorage* storage)
        : table_{table}
        , storage_{storage}
    {}

    std::error_code insert_tuple(const bored::executor::TupleView& tuple,
                                 bored::executor::ExecutorContext&,
                                 bored::executor::InsertExecutor::InsertStats& out_stats) override
    {
        if (table_ == nullptr || tuple.column_count() != 1U) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        const auto payload = tuple.column(0U);
        if (payload.is_null) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        if (!decode_values_payload(*table_, payload.data, decoded_values_)) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        if (storage_ == nullptr) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        const auto row_id = table_->next_row_id++;
        storage_->upsert_row(table_->relation_id, row_id, payload.data);

        out_stats.payload_bytes = payload.data.size();
        out_stats.wal_bytes = out_stats.payload_bytes;
        ++inserted_;
        return {};
    }

    [[nodiscard]] std::size_t inserted_count() const noexcept { return inserted_; }

private:
    ShellBackend::TableData* table_ = nullptr;
    ShellBackend::CatalogStorage* storage_ = nullptr;
    std::vector<ScalarValue> decoded_values_{};
    std::size_t inserted_ = 0U;
};

class ShellUpdateTarget final : public bored::executor::UpdateExecutor::Target {
public:
    ShellUpdateTarget(ShellBackend::TableData* table, ShellBackend::CatalogStorage* storage)
        : table_{table}
        , storage_{storage}
    {}

    std::error_code update_tuple(const bored::executor::TupleView& tuple,
                                 bored::executor::ExecutorContext&,
                                 bored::executor::UpdateExecutor::UpdateStats& out_stats) override
    {
        if (table_ == nullptr || tuple.column_count() < 2U) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        const auto row_id_view = tuple.column(0U);
        const auto new_payload_view = tuple.column(1U);
        if (row_id_view.is_null || new_payload_view.is_null ||
            row_id_view.data.size() != sizeof(std::uint64_t)) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        std::uint64_t row_id = 0U;
        std::memcpy(&row_id, row_id_view.data.data(), sizeof(row_id));

        if (storage_ == nullptr) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        if (!storage_->fetch_row(table_->relation_id, row_id, existing_payload_)) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        if (!decode_values_payload(*table_, new_payload_view.data, decoded_values_)) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        const auto old_size = existing_payload_.size();
        storage_->upsert_row(table_->relation_id, row_id, new_payload_view.data);

        out_stats.new_payload_bytes = new_payload_view.data.size();
        out_stats.old_payload_bytes = old_size;
        out_stats.wal_bytes = new_payload_view.data.size();
        ++updated_;
        wal_bytes_ += out_stats.wal_bytes;
        return {};
    }

    [[nodiscard]] std::size_t updated_count() const noexcept { return updated_; }
    [[nodiscard]] std::size_t wal_bytes() const noexcept { return wal_bytes_; }

private:
    ShellBackend::TableData* table_ = nullptr;
    ShellBackend::CatalogStorage* storage_ = nullptr;
    std::vector<ScalarValue> decoded_values_{};
    std::vector<std::byte> existing_payload_{};
    std::size_t updated_ = 0U;
    std::size_t wal_bytes_ = 0U;
};

class ShellDeleteTarget final : public bored::executor::DeleteExecutor::Target {
public:
    ShellDeleteTarget(ShellBackend::TableData* table, ShellBackend::CatalogStorage* storage)
        : table_{table}
        , storage_{storage}
    {}

    std::error_code delete_tuple(const bored::executor::TupleView& tuple,
                                 bored::executor::ExecutorContext&,
                                 bored::executor::DeleteExecutor::DeleteStats& out_stats) override
    {
        if (table_ == nullptr || tuple.column_count() < 1U) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        const auto row_id_view = tuple.column(0U);
        if (row_id_view.is_null || row_id_view.data.size() != sizeof(std::uint64_t)) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        std::uint64_t row_id = 0U;
        std::memcpy(&row_id, row_id_view.data.data(), sizeof(row_id));

        if (storage_ == nullptr) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        if (!storage_->remove_row(table_->relation_id, row_id, &existing_payload_)) {
            return std::make_error_code(std::errc::invalid_argument);
        }

        const auto reclaimed = existing_payload_.size();

        out_stats.reclaimed_bytes = reclaimed;
        out_stats.wal_bytes = reclaimed;
        ++deleted_;
        reclaimed_bytes_ += reclaimed;
        return {};
    }

    [[nodiscard]] std::size_t deleted_count() const noexcept { return deleted_; }
    [[nodiscard]] std::size_t reclaimed_bytes() const noexcept { return reclaimed_bytes_; }

private:
    ShellBackend::TableData* table_ = nullptr;
    ShellBackend::CatalogStorage* storage_ = nullptr;
    std::vector<std::byte> existing_payload_{};
    std::size_t deleted_ = 0U;
    std::size_t reclaimed_bytes_ = 0U;
};

std::string ShellBackend::normalize_identifier(std::string_view text)
{
    return lowercase_copy(trim_copy(text));
}

std::string ShellBackend::trim(std::string_view text)
{
    return trim_copy(text);
}

std::string ShellBackend::uppercase(std::string_view text)
{
    return uppercase_copy(text);
}

ShellBackend::ShellBackend()
    : ShellBackend(Config{})
{
}

ShellBackend::ShellBackend(Config config)
    : config_{std::move(config)}
    , snapshot_manager_{make_relaxed_snapshot()}
    , txn_manager_{txn_allocator_}
{
    if (config_.storage_directory.empty()) {
        config_.storage_directory = std::filesystem::current_path() / "bored_shell_data";
    }
    if (config_.wal_directory.empty()) {
        config_.wal_directory = config_.storage_directory / "wal";
    }

    catalog::CatalogCache::instance().reset();

    storage_ = std::make_unique<CatalogStorage>(config_, &storage_registry_, &wal_registry_);
    if (auto init_ec = storage_->initialize(); init_ec) {
        throw std::system_error(init_ec);
    }

    identifier_allocator_ = std::make_unique<InMemoryIdentifierAllocator>();

    default_schema_id_ = config_.default_schema_id.value_or(catalog::SchemaId{42U});
    if (!storage_->schema(default_database_id_, config_.default_schema_name)) {
        catalog::CatalogSchemaDescriptor descriptor{};
        descriptor.tuple.xmin = catalog::kCatalogBootstrapTxnId;
        descriptor.tuple.xmax = 0U;
        descriptor.tuple.visibility_flags = catalog::to_value(catalog::CatalogVisibilityFlag::Frozen);
        descriptor.schema_id = default_schema_id_;
        descriptor.database_id = default_database_id_;
        descriptor.name = config_.default_schema_name;

        catalog::CatalogMutationBatch bootstrap_batch{};
        catalog::CatalogStagedMutation mutation{};
        mutation.kind = catalog::CatalogMutationKind::Insert;
        mutation.relation_id = catalog::kCatalogSchemasRelationId;
        mutation.row_id = descriptor.schema_id.value;

        catalog::CatalogTupleVersion version{};
        version.descriptor = descriptor.tuple;
        version.payload = catalog::serialize_catalog_schema(descriptor);
        mutation.after = version;

        bootstrap_batch.mutations.push_back(std::move(mutation));
        storage_->apply(bootstrap_batch);
    }

    ddl::DdlCommandDispatcher::Config dispatcher_cfg{};
    dispatcher_cfg.transaction_factory = [this](txn::TransactionContext* context) {
        catalog::CatalogTransactionConfig tx_cfg{&txn_allocator_, &snapshot_manager_};
        auto transaction = std::make_unique<catalog::CatalogTransaction>(tx_cfg);
        transaction->bind_transaction_context(&txn_manager_, context);
        return transaction;
    };
    dispatcher_cfg.mutator_factory = [this](catalog::CatalogTransaction& transaction) {
        catalog::CatalogMutatorConfig mutator_cfg{};
        mutator_cfg.transaction = &transaction;
        mutator_cfg.commit_lsn_provider = [] { return 0ULL; };
        auto mutator = std::make_unique<catalog::CatalogMutator>(mutator_cfg);
        auto* raw = mutator.get();
        transaction.register_commit_hook([this, raw]() -> std::error_code {
            if (!raw->has_published_batch()) {
                return {};
            }
            auto batch = raw->consume_published_batch();
            storage_->apply(batch);
            refresh_table_cache();
            return {};
        });
        return mutator;
    };
    dispatcher_cfg.accessor_factory = [this](catalog::CatalogTransaction& transaction) {
        catalog::CatalogAccessor::Config accessor_cfg{};
        accessor_cfg.transaction = &transaction;
        accessor_cfg.scanner = storage_->make_scanner();
        return std::make_unique<catalog::CatalogAccessor>(accessor_cfg);
    };
    dispatcher_cfg.sequence_allocator_factory = [](catalog::CatalogTransaction& transaction,
                                                   catalog::CatalogAccessor& accessor,
                                                   catalog::CatalogMutator& mutator) {
        catalog::SequenceAllocator::Config seq_cfg{};
        seq_cfg.transaction = &transaction;
        seq_cfg.accessor = &accessor;
        seq_cfg.mutator = &mutator;
        return std::make_unique<catalog::SequenceAllocator>(seq_cfg);
    };
    dispatcher_cfg.identifier_allocator = identifier_allocator_.get();
    dispatcher_cfg.transaction_manager = &txn_manager_;
    dispatcher_cfg.commit_lsn_provider = [] { return 0ULL; };

    dispatcher_ = std::make_unique<ddl::DdlCommandDispatcher>(dispatcher_cfg);
    ddl::register_catalog_handlers(*dispatcher_);
    dispatcher_->register_handler<ddl::CreateDatabaseRequest>(
        [this](ddl::DdlCommandContext& context, const ddl::CreateDatabaseRequest& request) {
            return this->handle_create_database(context, request);
        });
    dispatcher_->register_handler<ddl::DropDatabaseRequest>(
        [this](ddl::DdlCommandContext& context, const ddl::DropDatabaseRequest& request) {
            return this->handle_drop_database(context, request);
        });

    parser::DdlCommandBuilderConfig builder_cfg{};
    builder_cfg.default_database_id = default_database_id_;
    builder_cfg.default_schema_id = default_schema_id_;
    builder_cfg.database_lookup = [this](std::string_view name) { return this->lookup_database(name); };
    builder_cfg.schema_lookup = [this](catalog::DatabaseId id, std::string_view name) {
        return this->lookup_schema(id, name);
    };

    parser::DdlScriptExecutor::Config executor_cfg{};
    executor_cfg.builder_config = builder_cfg;
    executor_cfg.dispatcher = dispatcher_.get();
    executor_cfg.telemetry_identifier = "shell/backend";
    executor_cfg.storage_registry = &storage_registry_;

    ddl_executor_ = std::make_unique<parser::DdlScriptExecutor>(executor_cfg);

    storage::set_global_storage_telemetry_registry(&storage_registry_);
    registered_storage_registry_ = true;

    catalog::set_global_catalog_introspection_sampler([this] { return this->collect_catalog_snapshot(); });
    registered_catalog_sampler_ = true;

    refresh_table_cache();
}

ShellBackend::~ShellBackend()
{
    ddl_executor_.reset();

    if (registered_catalog_sampler_) {
        auto sampler = catalog::get_global_catalog_introspection_sampler();
        if (sampler) {
            catalog::set_global_catalog_introspection_sampler(nullptr);
        }
        registered_catalog_sampler_ = false;
    }

    if (registered_storage_registry_) {
        if (storage::get_global_storage_telemetry_registry() == &storage_registry_) {
            storage::set_global_storage_telemetry_registry(nullptr);
        }
        registered_storage_registry_ = false;
    }
}

ShellEngine::Config ShellBackend::make_config()
{
    ShellEngine::Config config{};
    config.ddl_executor = ddl_executor_.get();
    config.dml_executor = [this](const std::string& sql) { return this->execute_dml(sql); };
    config.catalog_snapshot = [this]() { return this->collect_catalog_snapshot(); };
    return config;
}

catalog::CatalogIntrospectionSnapshot ShellBackend::collect_catalog_snapshot()
{
    txn::TransactionOptions txn_options{};
    ShellTransactionGuard txn_guard{txn_manager_, txn_options};
    auto& txn_context = txn_guard.context();

    catalog::CatalogTransactionConfig tx_cfg{};
    tx_cfg.id_allocator = &txn_allocator_;
    tx_cfg.snapshot_manager = &snapshot_manager_;
    tx_cfg.transaction_manager = &txn_manager_;
    tx_cfg.transaction_context = &txn_context;
    catalog::CatalogTransaction transaction{tx_cfg};
    CatalogTransactionGuard catalog_guard{transaction};

    catalog::CatalogAccessor::Config accessor_cfg{};
    accessor_cfg.transaction = &transaction;
    accessor_cfg.scanner = storage_->make_scanner();
    catalog::CatalogAccessor accessor{accessor_cfg};

    return catalog::collect_catalog_introspection(accessor);
}

std::optional<catalog::DatabaseId> ShellBackend::lookup_database(std::string_view name) const
{
    auto view = storage_->database(name);
    if (!view) {
        return std::nullopt;
    }
    return view->database_id;
}

std::optional<catalog::SchemaId> ShellBackend::lookup_schema(catalog::DatabaseId database_id,
                                                             std::string_view name) const
{
    auto view = storage_->schema(database_id, name);
    if (!view) {
        return std::nullopt;
    }
    return view->schema_id;
}

void ShellBackend::refresh_table_cache()
{
    std::unordered_map<RelationKey, TableData> refreshed;
    std::unordered_map<std::string, RelationKey> lookup;

    const auto schemas = storage_->list_schemas();
    std::unordered_map<std::uint64_t, std::string> schema_names;
    schema_names.reserve(schemas.size());
    for (const auto& schema : schemas) {
        schema_names.emplace(schema.schema_id.value, std::string(schema.name));
    }

    const auto columns = storage_->list_columns();
    std::unordered_map<RelationKey, std::vector<catalog::CatalogColumnView>> columns_by_relation;
    for (const auto& column : columns) {
        columns_by_relation[column.relation_id.value].push_back(column);
    }

    const auto tables = storage_->list_tables();
    for (const auto& table : tables) {
        const auto relation_id = relation_key(table.relation_id);
        TableData data{};
        data.relation_id = table.relation_id;
        data.schema_id = table.schema_id;
        data.table_name = std::string(table.name);
        auto schema_it = schema_names.find(table.schema_id.value);
        data.schema_name = (schema_it != schema_names.end()) ? schema_it->second : std::string{};

        auto column_it = columns_by_relation.find(relation_id);
        if (column_it != columns_by_relation.end()) {
            auto columns_copy = column_it->second;
            std::sort(columns_copy.begin(), columns_copy.end(), [](const auto& lhs, const auto& rhs) {
                return lhs.ordinal_position < rhs.ordinal_position;
            });
            data.columns.reserve(columns_copy.size());
            data.column_index.reserve(columns_copy.size());
            for (std::size_t index = 0; index < columns_copy.size(); ++index) {
                const auto& column_view = columns_copy[index];
                ColumnInfo info{};
                info.name = std::string(column_view.name);
                info.type = column_view.column_type;
                info.ordinal = index;
                data.column_index.emplace(normalize_identifier(info.name), index);
                data.columns.push_back(std::move(info));
            }
        }

        const auto key = normalize_identifier(data.schema_name) + "." + normalize_identifier(data.table_name);
        std::uint64_t next_row_id = storage_->max_row_id(data.relation_id);
        if (next_row_id < std::numeric_limits<std::uint64_t>::max()) {
            ++next_row_id;
        }
        if (next_row_id == 0U) {
            next_row_id = 1U;
        }

        auto existing = table_cache_.find(relation_id);
        if (existing != table_cache_.end()) {
            next_row_id = std::max(next_row_id, existing->second.next_row_id);
        }

        data.next_row_id = next_row_id;

        lookup.emplace(key, relation_id);
        refreshed.emplace(relation_id, std::move(data));
    }

    table_lookup_ = std::move(lookup);
    table_cache_ = std::move(refreshed);
}

ShellBackend::TableData* ShellBackend::find_table(std::string_view qualified_name)
{
    const auto key = normalize_identifier(qualified_name);
    auto it = table_lookup_.find(key);
    if (it == table_lookup_.end()) {
        return nullptr;
    }
    auto cache_it = table_cache_.find(it->second);
    if (cache_it == table_cache_.end()) {
        return nullptr;
    }
    return &cache_it->second;
}

ShellBackend::TableData* ShellBackend::find_table_or_default_schema(std::string_view name)
{
    if (name.find('.') != std::string_view::npos) {
        return find_table(name);
    }
    const auto qualified = normalize_identifier(config_.default_schema_name) + "." + normalize_identifier(name);
    return find_table(qualified);
}

ShellBackend::TableData* ShellBackend::find_table(const parser::relational::TableBinding& binding)
{
    if (!binding.schema_name.empty()) {
        return find_table(binding.schema_name + "." + binding.table_name);
    }
    return find_table_or_default_schema(binding.table_name);
}

ShellBackend::TableData* ShellBackend::find_table(catalog::RelationId relation_id)
{
    auto it = table_cache_.find(relation_key(relation_id));
    if (it == table_cache_.end()) {
        return nullptr;
    }
    return &it->second;
}

std::vector<std::string> ShellBackend::collect_table_columns(const TableData& table)
{
    std::vector<std::string> columns;
    columns.reserve(table.columns.size());
    for (const auto& column_info : table.columns) {
        columns.push_back(column_info.name);
    }
    return columns;
}

std::variant<ShellBackend::PlannerPlanDetails, CommandMetrics> ShellBackend::plan_scan_operation(
    const std::string& sql,
    planner::LogicalOperatorType logical_type,
    planner::PhysicalOperatorType physical_type,
    const parser::relational::TableBinding& table_binding,
    const TableData& table,
    std::string_view root_label,
    std::string_view statement_label,
    const catalog::CatalogAccessor& accessor,
    const txn::TransactionContext& txn_context)
{
    auto planner_columns = collect_table_columns(table);

    planner::LogicalProperties scan_props{};
    scan_props.relation_name = format_relation_name(table_binding);
    scan_props.relation_id = table.relation_id;
    scan_props.output_columns = planner_columns;

    auto scan_logical = planner::LogicalOperator::make(
        planner::LogicalOperatorType::TableScan,
        std::vector<planner::LogicalOperatorPtr>{},
        scan_props);

    planner::LogicalProperties root_props{};
    root_props.relation_name = scan_props.relation_name;
    root_props.relation_id = table.relation_id;
    root_props.output_columns = planner_columns;

    auto root_logical = planner::LogicalOperator::make(
        logical_type,
        std::vector<planner::LogicalOperatorPtr>{scan_logical},
        root_props);

    planner::LogicalPlan logical_plan{root_logical};
    planner::PlannerContextConfig planner_cfg{};
    planner_cfg.catalog = &accessor;
    planner_cfg.snapshot = txn_context.snapshot();
    planner::PlannerContext planner_context{planner_cfg};
    auto planner_result = planner::plan_query(planner_context, logical_plan);
    const auto plan_root = planner_result.plan.root();
    const auto statement = std::string(statement_label);
    if (!plan_root) {
        return make_planner_error_metrics(sql,
                                          std::move(planner_result.diagnostics),
                                          "Failed to plan " + statement + " statement.");
    }
    if (plan_root->type() != physical_type) {
        return make_planner_error_metrics(sql,
                                          std::move(planner_result.diagnostics),
                                          "Planner produced unexpected physical operator for " + statement + ".");
    }
    planner::PhysicalOperatorPtr leaf = plan_root->children().empty() ? planner::PhysicalOperatorPtr{}
                                                                      : plan_root->children().front();
    while (leaf &&
           (leaf->type() == planner::PhysicalOperatorType::UniqueEnforce ||
            leaf->type() == planner::PhysicalOperatorType::ForeignKeyCheck)) {
        if (leaf->children().empty()) {
            break;
        }
        leaf = leaf->children().front();
    }
    if (!leaf || leaf->type() != planner::PhysicalOperatorType::SeqScan) {
        return make_planner_error_metrics(sql,
                                          std::move(planner_result.diagnostics),
                                          "Planner " + statement + " plan is missing SeqScan child.");
    }

    PlannerPlanDetails details{};
    details.plan = std::move(planner_result.plan);
    details.root_detail = std::string(root_label) + " (children=" + std::to_string(plan_root->children().size()) + ")";
    details.detail_lines = std::move(planner_result.diagnostics);
    return details;
}

std::variant<ShellBackend::PlannerPlanDetails, CommandMetrics> ShellBackend::plan_select_operation(
    const std::string& sql,
    const parser::relational::TableBinding& table_binding,
    const TableData& table,
    std::vector<std::string> projection_columns,
    const catalog::CatalogAccessor& accessor,
    const txn::TransactionContext& txn_context)
{
    auto planner_columns = collect_table_columns(table);
    if (projection_columns.empty()) {
        projection_columns = planner_columns;
    }

    planner::LogicalProperties scan_props{};
    scan_props.relation_name = format_relation_name(table_binding);
    scan_props.relation_id = table.relation_id;
    scan_props.output_columns = planner_columns;

    auto scan_logical = planner::LogicalOperator::make(
        planner::LogicalOperatorType::TableScan,
        std::vector<planner::LogicalOperatorPtr>{},
        scan_props);

    planner::LogicalProperties projection_props{};
    projection_props.relation_name = scan_props.relation_name;
    projection_props.relation_id = table.relation_id;
    projection_props.output_columns = projection_columns;

    auto projection_logical = planner::LogicalOperator::make(
        planner::LogicalOperatorType::Projection,
        std::vector<planner::LogicalOperatorPtr>{scan_logical},
        projection_props);

    planner::LogicalPlan logical_plan{projection_logical};
    planner::PlannerContextConfig planner_cfg{};
    planner_cfg.catalog = &accessor;
    planner_cfg.snapshot = txn_context.snapshot();
    planner::PlannerContext planner_context{planner_cfg};
    auto planner_result = planner::plan_query(planner_context, logical_plan);
    const auto plan_root = planner_result.plan.root();
    if (!plan_root) {
        return make_planner_error_metrics(sql,
                                          std::move(planner_result.diagnostics),
                                          "Failed to plan SELECT statement.");
    }

    std::string root_detail;
    if (plan_root->type() == planner::PhysicalOperatorType::Projection) {
        if (plan_root->children().empty() ||
            plan_root->children().front()->type() != planner::PhysicalOperatorType::SeqScan) {
            return make_planner_error_metrics(sql,
                                              std::move(planner_result.diagnostics),
                                              "Planner SELECT plan is missing SeqScan child.");
        }
        root_detail = "Projection (children=" + std::to_string(plan_root->children().size()) + ")";
    } else if (plan_root->type() == planner::PhysicalOperatorType::SeqScan) {
        root_detail = "SeqScan (children=" + std::to_string(plan_root->children().size()) + ")";
    } else {
        return make_planner_error_metrics(sql,
                                          std::move(planner_result.diagnostics),
                                          "Planner produced unexpected physical operator for SELECT.");
    }

    PlannerPlanDetails details{};
    details.plan = std::move(planner_result.plan);
    details.root_detail = std::move(root_detail);
    details.detail_lines = std::move(planner_result.diagnostics);
    return details;
}

std::vector<std::string> ShellBackend::render_executor_plan(std::string_view statement_label,
                                                            const planner::PhysicalPlan& plan)
{
    std::vector<std::string> detail_lines;
    const auto root = plan.root();
    if (!root) {
        detail_lines.push_back("executor.plan: <empty>");
        return detail_lines;
    }

    std::string summary = "executor.plan=";
    summary.append(statement_label);
    summary.append(" root=");
    summary.append(physical_operator_name(root->type()));
    if (!root->children().empty() && root->children().front()) {
        summary.append(" first_child=");
        summary.append(physical_operator_name(root->children().front()->type()));
    }
    detail_lines.push_back(std::move(summary));

    planner::ExplainOptions explain{};
    explain.include_properties = true;
    explain.include_snapshot = false;
    const auto rendered_plan = planner::explain_plan(plan, explain);

    std::istringstream stream{rendered_plan};
    std::string line;
    while (std::getline(stream, line)) {
        if (line.empty()) {
            continue;
        }
        detail_lines.push_back("executor.plan: " + line);
    }

    return detail_lines;
}

CommandMetrics ShellBackend::make_error_metrics(const std::string& sql,
                                                std::string message,
                                                std::vector<std::string> hints)
{
    CommandMetrics metrics{};
    metrics.success = false;
    metrics.summary = std::move(message);

    parser::ParserDiagnostic diagnostic{};
    diagnostic.severity = parser::ParserSeverity::Error;
    diagnostic.message = metrics.summary;
    diagnostic.statement = sql;
    diagnostic.remediation_hints = std::move(hints);
    metrics.diagnostics.push_back(std::move(diagnostic));
    return metrics;
}

CommandMetrics ShellBackend::make_transaction_error(std::string message,
                                                    std::vector<std::string> hints)
{
    CommandMetrics metrics{};
    metrics.success = false;
    metrics.summary = std::move(message);

    parser::ParserDiagnostic diagnostic{};
    diagnostic.severity = parser::ParserSeverity::Error;
    diagnostic.message = metrics.summary;
    diagnostic.remediation_hints = std::move(hints);
    metrics.diagnostics.push_back(std::move(diagnostic));
    return metrics;
}

bool ShellBackend::has_active_transaction() const noexcept
{
    return session_transaction_.has_value() && session_transaction_->active();
}

bool ShellBackend::session_requires_rollback() const noexcept
{
    return session_transaction_.has_value() && session_transaction_->needs_rollback;
}

txn::TransactionContext* ShellBackend::session_transaction_context() noexcept
{
    if (!session_transaction_.has_value()) {
        return nullptr;
    }
    return &session_transaction_->context;
}

void ShellBackend::mark_session_transaction_dirty()
{
    if (session_transaction_) {
        session_transaction_->dirty = true;
    }
}

void ShellBackend::mark_session_transaction_failed()
{
    if (session_transaction_) {
        session_transaction_->needs_rollback = true;
    }
}

void ShellBackend::clear_session_transaction()
{
    session_transaction_.reset();
}

ddl::DdlCommandResponse ShellBackend::handle_create_database(ddl::DdlCommandContext&,
                                                             const ddl::CreateDatabaseRequest& request)
{
    if (auto ec = ddl::validate_identifier(request.name); ec) {
        return ddl::make_failure(ec, "Database name is invalid.");
    }

    if (auto existing = storage_->database(request.name); existing.has_value()) {
        if (request.if_not_exists) {
            return ddl::make_success();
        }
        return ddl::make_failure(ddl::make_error_code(ddl::DdlErrc::DatabaseAlreadyExists),
                                 "Database already exists.");
    }

    const auto databases = storage_->list_databases();
    std::uint64_t candidate = next_database_id_;
    bool reused = true;
    while (reused) {
        reused = false;
        for (const auto& database : databases) {
            if (database.database_id.value == candidate) {
                reused = true;
                ++candidate;
                break;
            }
        }
    }

    catalog::CatalogDatabaseDescriptor descriptor{};
    descriptor.tuple.xmin = catalog::kCatalogBootstrapTxnId;
    descriptor.tuple.xmax = 0U;
    descriptor.tuple.visibility_flags = catalog::to_value(catalog::CatalogVisibilityFlag::Frozen);
    descriptor.database_id = catalog::DatabaseId{candidate};
    descriptor.default_schema_id = default_schema_id_;
    descriptor.name = request.name;

    catalog::CatalogMutationBatch batch{};
    catalog::CatalogStagedMutation mutation{};
    mutation.kind = catalog::CatalogMutationKind::Insert;
    mutation.relation_id = catalog::kCatalogDatabasesRelationId;
    mutation.row_id = descriptor.database_id.value;

    catalog::CatalogTupleVersion version{};
    version.descriptor = descriptor.tuple;
    version.payload = catalog::serialize_catalog_database(descriptor);
    mutation.after = version;

    batch.mutations.push_back(std::move(mutation));
    storage_->apply(batch);
    refresh_table_cache();

    next_database_id_ = candidate + 1U;

    return ddl::make_success();
}

ddl::DdlCommandResponse ShellBackend::handle_drop_database(ddl::DdlCommandContext&,
                                                           const ddl::DropDatabaseRequest& request)
{
    if (auto ec = ddl::validate_identifier(request.name); ec) {
        return ddl::make_failure(ec, "Database name is invalid.");
    }

    auto existing = storage_->database(request.name);
    if (!existing) {
        if (request.if_exists) {
            return ddl::make_success();
        }
        return ddl::make_failure(ddl::make_error_code(ddl::DdlErrc::DatabaseNotFound),
                                 "Database not found.");
    }

    if (existing->database_id == default_database_id_) {
        return ddl::make_failure(ddl::make_error_code(ddl::DdlErrc::ExecutionFailed),
                                 "Dropping the default shell database is not supported.");
    }

    const auto database_id = existing->database_id;

    std::unordered_set<std::uint64_t> schema_ids;
    const auto schemas = storage_->list_schemas();
    for (const auto& schema : schemas) {
        if (schema.database_id == database_id) {
            schema_ids.insert(schema.schema_id.value);
        }
    }

    catalog::CatalogMutationBatch batch{};

    std::unordered_set<std::uint64_t> relation_ids;
    const auto tables = storage_->list_tables();
    for (const auto& table : tables) {
        if (schema_ids.count(table.schema_id.value) == 0U) {
            continue;
        }
        relation_ids.insert(table.relation_id.value);

        std::vector<std::byte> payload;
        if (!storage_->fetch_row(catalog::kCatalogTablesRelationId, table.relation_id.value, payload)) {
            continue;
        }

        catalog::CatalogStagedMutation mutation{};
        mutation.kind = catalog::CatalogMutationKind::Delete;
        mutation.relation_id = catalog::kCatalogTablesRelationId;
        mutation.row_id = table.relation_id.value;

        catalog::CatalogTupleVersion version{};
        version.descriptor = table.tuple;
        version.payload = std::move(payload);
        mutation.before = version;

        batch.mutations.push_back(std::move(mutation));
    }

    const auto columns = storage_->list_columns();
    for (const auto& column : columns) {
        if (relation_ids.count(column.relation_id.value) == 0U) {
            continue;
        }

        std::vector<std::byte> payload;
        if (!storage_->fetch_row(catalog::kCatalogColumnsRelationId, column.column_id.value, payload)) {
            continue;
        }

        catalog::CatalogStagedMutation mutation{};
        mutation.kind = catalog::CatalogMutationKind::Delete;
        mutation.relation_id = catalog::kCatalogColumnsRelationId;
        mutation.row_id = column.column_id.value;

        catalog::CatalogTupleVersion version{};
        version.descriptor = column.tuple;
        version.payload = std::move(payload);
        mutation.before = version;

        batch.mutations.push_back(std::move(mutation));
    }

    for (const auto& schema : schemas) {
        if (schema.database_id != database_id) {
            continue;
        }

        std::vector<std::byte> payload;
        if (!storage_->fetch_row(catalog::kCatalogSchemasRelationId, schema.schema_id.value, payload)) {
            continue;
        }

        catalog::CatalogStagedMutation mutation{};
        mutation.kind = catalog::CatalogMutationKind::Delete;
        mutation.relation_id = catalog::kCatalogSchemasRelationId;
        mutation.row_id = schema.schema_id.value;

        catalog::CatalogTupleVersion version{};
        version.descriptor = schema.tuple;
        version.payload = std::move(payload);
        mutation.before = version;

        batch.mutations.push_back(std::move(mutation));
    }

    std::vector<std::byte> database_payload;
    if (storage_->fetch_row(catalog::kCatalogDatabasesRelationId, database_id.value, database_payload)) {
        catalog::CatalogStagedMutation mutation{};
        mutation.kind = catalog::CatalogMutationKind::Delete;
        mutation.relation_id = catalog::kCatalogDatabasesRelationId;
        mutation.row_id = database_id.value;

        catalog::CatalogTupleVersion version{};
        version.descriptor = existing->tuple;
        version.payload = std::move(database_payload);
        mutation.before = version;

        batch.mutations.push_back(std::move(mutation));
    }

    if (!batch.mutations.empty()) {
        storage_->apply(batch);
        refresh_table_cache();
    }

    return ddl::make_success();
}

CommandMetrics ShellBackend::execute_dml(const std::string& sql)
{
    const auto normalized = strip_leading_comments(sql);
    if (normalized.empty()) {
        return make_error_metrics(sql, "DML command is empty.");
    }

    if (starts_with_ci(normalized, kBeginKeyword)) {
        return execute_begin();
    }
    if (starts_with_ci(normalized, kCommitKeyword)) {
        return execute_commit();
    }
    if (starts_with_ci(normalized, kRollbackKeyword)) {
        return execute_rollback();
    }

    if (starts_with_ci(normalized, kInsertKeyword)) {
        return execute_insert(normalized);
    }
    if (starts_with_ci(normalized, kUpdateKeyword)) {
        return execute_update(normalized);
    }
    if (starts_with_ci(normalized, kDeleteKeyword)) {
        return execute_delete(normalized);
    }
    if (starts_with_ci(normalized, kSelectKeyword)) {
        return execute_select(normalized);
    }

    return make_error_metrics(sql,
                              "Unsupported DML command.",
                              {"Supported commands: INSERT, UPDATE, DELETE, SELECT."});
}

CommandMetrics ShellBackend::execute_begin()
{
    if (has_active_transaction()) {
        return make_transaction_error("Transaction already active.",
                                      {"Use COMMIT or ROLLBACK before issuing BEGIN."});
    }

    txn::TransactionOptions txn_options{};
    session_transaction_.emplace();
    session_transaction_->context = txn_manager_.begin(txn_options);
    session_transaction_->dirty = false;
    session_transaction_->needs_rollback = false;

    CommandMetrics metrics{};
    metrics.success = true;
    metrics.summary = "Transaction started.";
    return metrics;
}

CommandMetrics ShellBackend::execute_commit()
{
    if (!has_active_transaction()) {
        return make_transaction_error("No active transaction to commit.");
    }

    if (session_requires_rollback()) {
        return make_transaction_error("Transaction is aborted; run ROLLBACK before COMMIT.");
    }

    try {
        txn_manager_.commit(session_transaction_->context);
    } catch (const std::exception& ex) {
        mark_session_transaction_failed();
        return make_transaction_error(std::string{"Commit failed: "} + ex.what(),
                                      {"Run ROLLBACK to clear the transaction before retrying."});
    } catch (...) {
        mark_session_transaction_failed();
        return make_transaction_error("Commit failed due to unknown error.",
                                      {"Run ROLLBACK to clear the transaction before retrying."});
    }

    clear_session_transaction();

    CommandMetrics metrics{};
    metrics.success = true;
    metrics.summary = "Transaction committed.";
    return metrics;
}

CommandMetrics ShellBackend::execute_rollback()
{
    if (!has_active_transaction()) {
        return make_transaction_error("No active transaction to roll back.");
    }

    try {
        txn_manager_.abort(session_transaction_->context);
    } catch (const std::exception& ex) {
        return make_transaction_error(std::string{"Rollback failed: "} + ex.what());
    } catch (...) {
        return make_transaction_error("Rollback failed due to unknown error.");
    }

    clear_session_transaction();

    CommandMetrics metrics{};
    metrics.success = true;
    metrics.summary = "Transaction rolled back.";
    return metrics;
}

CommandMetrics ShellBackend::execute_insert(const std::string& sql)
{
    if (session_requires_rollback()) {
        return make_error_metrics(sql,
                                  "Transaction is aborted; run ROLLBACK before executing statements.");
    }

    auto parse_result = parser::parse_insert(sql);
    if (!parse_result.success()) {
        return make_parser_error_metrics(sql,
                                         std::move(parse_result.diagnostics),
                                         "Failed to parse INSERT statement.");
    }

    txn::TransactionOptions txn_options{};
    TransactionWorkScope txn_scope{*this, txn_manager_, txn_options};
    auto& txn_context = txn_scope.context();

    catalog::CatalogTransactionConfig tx_cfg{};
    tx_cfg.id_allocator = &txn_allocator_;
    tx_cfg.snapshot_manager = &snapshot_manager_;
    tx_cfg.transaction_manager = &txn_manager_;
    tx_cfg.transaction_context = &txn_context;
    catalog::CatalogTransaction transaction{tx_cfg};
    CatalogTransactionGuard catalog_guard{transaction};

    catalog::CatalogAccessor::Config accessor_cfg{};
    accessor_cfg.transaction = &transaction;
    accessor_cfg.scanner = storage_->make_scanner();
    catalog::CatalogAccessor accessor{accessor_cfg};

    parser::relational::CatalogBinderAdapter binder_catalog{accessor};
    parser::relational::BinderConfig binder_cfg{};
    binder_cfg.catalog = &binder_catalog;
    if (!config_.default_schema_name.empty()) {
        binder_cfg.default_schema = config_.default_schema_name;
    }

    auto binding = parser::relational::bind_insert(binder_cfg, *parse_result.statement);
    if (!binding.success()) {
        txn_scope.release();
        return make_parser_error_metrics(sql,
                                         std::move(binding.diagnostics),
                                         "Failed to bind INSERT statement.");
    }

    const auto* target_ref = parse_result.statement->target;
    if (target_ref == nullptr || !target_ref->binding.has_value()) {
        txn_scope.release();
        return make_error_metrics(sql, "INSERT target table binding is missing.");
    }

    const auto* table_binding = &*target_ref->binding;
    auto* table = find_table(*table_binding);
    if (table == nullptr) {
        txn_scope.release();
        return make_error_metrics(sql, "Target table not found.", {"Verify the table exists."});
    }

    if (parse_result.statement->columns.size() != table->columns.size()) {
        txn_scope.release();
        return make_error_metrics(sql,
                                  "INSERT column count must match table definition.",
                                  {"Provide values for all columns in the table."});
    }

    std::vector<std::string> planner_columns;
    planner_columns.reserve(parse_result.statement->columns.size());
    std::vector<std::size_t> column_indexes;
    column_indexes.reserve(parse_result.statement->columns.size());
    for (const auto& column : parse_result.statement->columns) {
        if (!column.binding.has_value()) {
            txn_scope.release();
            return make_error_metrics(sql, "INSERT column binding is missing.");
        }
        planner_columns.push_back(column.binding->column_name);
        const auto key = normalize_identifier(column.binding->column_name);
        auto it = table->column_index.find(key);
        if (it == table->column_index.end()) {
            txn_scope.release();
            return make_error_metrics(sql,
                                      "Column '" + column.binding->column_name + "' not found in target table.");
        }
        column_indexes.push_back(it->second);
    }

    std::vector<std::string> logical_detail_lines;
    logical_detail_lines.push_back("logical.insert target=" + format_relation_name(*table_binding) +
                                   " columns=[" + join_strings(planner_columns, ", ") + "] rows=" +
                                   std::to_string(parse_result.statement->rows.size()));

    // Build a logical plan so planner integration can be validated prior to executor wiring.
    planner::LogicalProperties insert_props{};
    insert_props.relation_name = format_relation_name(*table_binding);
    insert_props.relation_id = table->relation_id;
    insert_props.output_columns = planner_columns;

    auto values_node = planner::LogicalOperator::make(planner::LogicalOperatorType::Values);
    auto insert_logical = planner::LogicalOperator::make(
        planner::LogicalOperatorType::Insert,
        std::vector<planner::LogicalOperatorPtr>{values_node},
        insert_props);

    planner::LogicalPlan logical_plan{insert_logical};
    planner::PlannerContextConfig planner_cfg{};
    planner_cfg.catalog = &accessor;
    planner_cfg.snapshot = txn_context.snapshot();
    planner::PlannerContext planner_context{planner_cfg};
    auto planner_result = planner::plan_query(planner_context, logical_plan);
    if (!planner_result.plan.root()) {
        txn_scope.release();
        return make_planner_error_metrics(sql,
                                          std::move(planner_result.diagnostics),
                                          "Failed to plan INSERT statement.");
    }

    const auto plan_root = planner_result.plan.root();
    if (plan_root->type() != planner::PhysicalOperatorType::Insert) {
        txn_scope.release();
        return make_planner_error_metrics(sql,
                                          std::move(planner_result.diagnostics),
                                          "Planner produced unexpected physical operator for INSERT.");
    }
    planner::PhysicalOperatorPtr values_leaf = plan_root->children().empty() ? planner::PhysicalOperatorPtr{}
                                                                            : plan_root->children().front();
    while (values_leaf &&
           (values_leaf->type() == planner::PhysicalOperatorType::UniqueEnforce ||
            values_leaf->type() == planner::PhysicalOperatorType::ForeignKeyCheck)) {
        if (values_leaf->children().empty()) {
            break;
        }
        values_leaf = values_leaf->children().front();
    }
    if (!values_leaf || values_leaf->type() != planner::PhysicalOperatorType::Values) {
        txn_scope.release();
        return make_planner_error_metrics(sql,
                                          std::move(planner_result.diagnostics),
                                          "Planner INSERT plan is missing VALUES child.");
    }

    const auto& planned_columns = plan_root->properties().output_columns;
    if (!planned_columns.empty() && planned_columns != planner_columns) {
        txn_scope.release();
        return make_planner_error_metrics(sql,
                                          std::move(planner_result.diagnostics),
                                          "Planner column projection does not match INSERT column list.");
    }

    PlannerPlanDetails plan_details{};
    plan_details.root_detail = "Insert (children=" + std::to_string(plan_root->children().size()) + ")";
    plan_details.detail_lines = std::move(planner_result.diagnostics);
    plan_details.plan = std::move(planner_result.plan);

    auto executor_detail_lines = render_executor_plan("INSERT", plan_details.plan);

    std::vector<std::vector<ScalarValue>> pending_rows;
    pending_rows.reserve(parse_result.statement->rows.size());
    for (const auto& row_node : parse_result.statement->rows) {
        if (row_node.values.size() != column_indexes.size()) {
            txn_scope.release();
            return make_error_metrics(sql, "VALUES list does not match column count.");
        }

        std::vector<ScalarValue> row_values(table->columns.size());
        for (std::size_t index = 0U; index < column_indexes.size(); ++index) {
            const auto column_index = column_indexes[index];
            const auto& column_info = table->columns[column_index];
            const auto* expression = row_node.values[index];
            if (expression == nullptr || expression->kind != parser::relational::NodeKind::LiteralExpression) {
                txn_scope.release();
                return make_error_metrics(sql, "INSERT values must be literals.");
            }

            const auto& literal = static_cast<const parser::relational::LiteralExpression&>(*expression);
            if (literal.tag == parser::relational::LiteralTag::Null) {
                txn_scope.release();
                return make_error_metrics(sql,
                                          "NULL literals are not supported in bored_shell INSERT statements.",
                                          {"Provide concrete values for each column."});
            }

            if (column_info.type == catalog::CatalogColumnType::Utf8) {
                row_values[column_index] = literal.text;
                continue;
            }

            std::int64_t numeric_value = 0;
            if (!parse_int64(literal.text, numeric_value)) {
                txn_scope.release();
                return make_error_metrics(sql,
                                          "Column '" + column_info.name + "' expects a numeric literal.");
            }
            row_values[column_index] = numeric_value;
        }

        pending_rows.push_back(std::move(row_values));
    }

    auto constraint_plan_result = build_constraint_enforcement_plan(*table, accessor);
    if (std::holds_alternative<ConstraintPlanError>(constraint_plan_result)) {
        txn_scope.release();
        auto error = std::get<ConstraintPlanError>(std::move(constraint_plan_result));
        return make_error_metrics(sql, std::move(error.message), std::move(error.hints));
    }

    auto constraint_plan = std::get<ConstraintEnforcementPlan>(std::move(constraint_plan_result));
    std::unique_ptr<ShellStorageReader> constraint_reader;
    if (!constraint_plan.index_definitions.empty()) {
        std::unordered_map<std::uint64_t, ShellStorageReader::IndexDefinition> reader_definitions;
        reader_definitions.reserve(constraint_plan.index_definitions.size());
        for (auto& [index_value, definition] : constraint_plan.index_definitions) {
            ShellStorageReader::IndexDefinition reader_definition{};
            reader_definition.table = definition.table;
            reader_definition.column_indexes = std::move(definition.column_indexes);
            reader_definitions.emplace(index_value, std::move(reader_definition));
        }
        constraint_reader = std::make_unique<ShellStorageReader>(nullptr, storage_.get(), std::move(reader_definitions));
    }

    bored::executor::ExecutorTelemetry insert_telemetry;
    ShellInsertTarget insert_target{table, storage_.get()};
    bored::executor::ExecutorNodePtr pipeline = std::make_unique<ShellValuesExecutor>(table, std::move(pending_rows));

    if (constraint_reader &&
        (!constraint_plan.foreign_key_constraints.empty() || !constraint_plan.unique_constraints.empty())) {
    pipeline = apply_constraint_enforcers(std::move(pipeline),
                          constraint_plan,
                          constraint_reader.get(),
                          table,
                          insert_telemetry,
                          0U,
                          std::nullopt);
    }

    bored::executor::InsertExecutor::Config insert_config{};
    insert_config.target = &insert_target;
    insert_config.telemetry = &insert_telemetry;

    bored::executor::InsertExecutor insert_executor{std::move(pipeline), insert_config};

    bored::executor::ExecutorContext executor_context{};
    executor_context.set_transaction_id(txn_context.id());
    executor_context.set_snapshot(txn_context.snapshot());
    executor_context.set_transaction_context(&txn_context);
    bored::executor::TupleBuffer executor_buffer{};
    insert_executor.open(executor_context);
    while (insert_executor.next(executor_context, executor_buffer)) {
        executor_buffer.reset();
    }
    insert_executor.close(executor_context);

    const std::size_t inserted = insert_target.inserted_count();
    const auto telemetry_snapshot = insert_telemetry.snapshot();

    CommandMetrics metrics{};
    metrics.success = true;
    metrics.summary = "Inserted " + format_count("row", inserted) + ".";
    metrics.rows_touched = inserted;
    metrics.wal_bytes = telemetry_snapshot.insert_wal_bytes;
    metrics.detail_lines.insert(metrics.detail_lines.end(),
                                logical_detail_lines.begin(),
                                logical_detail_lines.end());
    metrics.detail_lines.push_back("planner.root=" + plan_details.root_detail);
    metrics.detail_lines.insert(metrics.detail_lines.end(),
                                plan_details.detail_lines.begin(),
                                plan_details.detail_lines.end());
    metrics.detail_lines.insert(metrics.detail_lines.end(),
                                executor_detail_lines.begin(),
                                executor_detail_lines.end());

    if (auto ec = catalog_guard.commit()) {
        txn_scope.abort();
        return make_error_metrics(sql,
                                  "Catalog transaction commit failed.",
                                  {"Retry the INSERT command."});
    }

    txn_scope.commit();

    return metrics;
}

CommandMetrics ShellBackend::execute_update(const std::string& sql)
{
    if (session_requires_rollback()) {
        return make_error_metrics(sql,
                                  "Transaction is aborted; run ROLLBACK before executing statements.");
    }

    auto parse_result = parser::parse_update(sql);
    if (!parse_result.success()) {
        return make_parser_error_metrics(sql,
                                         std::move(parse_result.diagnostics),
                                         "Failed to parse UPDATE statement.");
    }

    txn::TransactionOptions txn_options{};
    TransactionWorkScope txn_scope{*this, txn_manager_, txn_options};
    auto& txn_context = txn_scope.context();

    catalog::CatalogTransactionConfig tx_cfg{};
    tx_cfg.id_allocator = &txn_allocator_;
    tx_cfg.snapshot_manager = &snapshot_manager_;
    tx_cfg.transaction_manager = &txn_manager_;
    tx_cfg.transaction_context = &txn_context;
    catalog::CatalogTransaction transaction{tx_cfg};
    CatalogTransactionGuard catalog_guard{transaction};

    catalog::CatalogAccessor::Config accessor_cfg{};
    accessor_cfg.transaction = &transaction;
    accessor_cfg.scanner = storage_->make_scanner();
    catalog::CatalogAccessor accessor{accessor_cfg};

    parser::relational::CatalogBinderAdapter binder_catalog{accessor};
    parser::relational::BinderConfig binder_cfg{};
    binder_cfg.catalog = &binder_catalog;
    if (!config_.default_schema_name.empty()) {
        binder_cfg.default_schema = config_.default_schema_name;
    }

    auto binding = parser::relational::bind_update(binder_cfg, *parse_result.statement);
    if (!binding.success()) {
        txn_scope.release();
        return make_parser_error_metrics(sql,
                                         std::move(binding.diagnostics),
                                         "Failed to bind UPDATE statement.");
    }

    const auto* target_ref = parse_result.statement->target;
    if (target_ref == nullptr || !target_ref->binding.has_value()) {
        txn_scope.release();
        return make_error_metrics(sql, "UPDATE target table binding is missing.");
    }

    const auto* table_binding = &*target_ref->binding;
    auto* table = find_table(*table_binding);
    if (table == nullptr) {
        txn_scope.release();
        return make_error_metrics(sql, "Target table not found.");
    }

    std::vector<std::string> logical_detail_lines;

    if (parse_result.statement->assignments.empty()) {
        txn_scope.release();
        return make_error_metrics(sql, "UPDATE statement is missing a SET assignment.");
    }

    const auto& assignment = parse_result.statement->assignments.front();
    if (!assignment.binding.has_value() || assignment.value == nullptr) {
        txn_scope.release();
        return make_error_metrics(sql, "UPDATE assignment binding is missing.");
    }

    const auto key = normalize_identifier(assignment.binding->column_name);
    auto target_it = table->column_index.find(key);
    if (target_it == table->column_index.end()) {
        txn_scope.release();
        return make_error_metrics(sql,
                                  "Column '" + assignment.binding->column_name + "' not found in target table.");
    }

    const auto target_index = target_it->second;
    const auto& target_column = table->columns[target_index];

    auto plan_result = plan_scan_operation(sql,
                                           planner::LogicalOperatorType::Update,
                                           planner::PhysicalOperatorType::Update,
                                           *table_binding,
                                           *table,
                                           "Update",
                                           "UPDATE",
                                           accessor,
                                           txn_context);
    if (std::holds_alternative<CommandMetrics>(plan_result)) {
        txn_scope.release();
        return std::get<CommandMetrics>(std::move(plan_result));
    }
    auto plan_details = std::get<PlannerPlanDetails>(std::move(plan_result));

    auto executor_detail_lines = render_executor_plan("UPDATE", plan_details.plan);

    enum class AssignmentKind {
        NumericConstant,
        StringConstant,
        AddInteger,
        SubtractInteger
    };

    AssignmentKind assignment_kind = AssignmentKind::NumericConstant;
    std::int64_t numeric_constant = 0;
    std::int64_t delta_value = 0;
    std::string string_constant;

    const auto* value_expression = assignment.value;
    if (value_expression->kind == parser::relational::NodeKind::LiteralExpression) {
        const auto& literal = static_cast<const parser::relational::LiteralExpression&>(*value_expression);
        if (target_column.type == catalog::CatalogColumnType::Utf8) {
            assignment_kind = AssignmentKind::StringConstant;
            string_constant = literal.text;
        } else {
            if (!parse_int64(literal.text, numeric_constant)) {
                txn_scope.release();
                return make_error_metrics(sql,
                                          "Column '" + target_column.name + "' expects a numeric literal.");
            }
            assignment_kind = AssignmentKind::NumericConstant;
        }
    } else if (value_expression->kind == parser::relational::NodeKind::BinaryExpression) {
        const auto& binary = static_cast<const parser::relational::BinaryExpression&>(*value_expression);
        if (binary.left == nullptr || binary.right == nullptr ||
            binary.left->kind != parser::relational::NodeKind::IdentifierExpression ||
            binary.right->kind != parser::relational::NodeKind::LiteralExpression) {
            txn_scope.release();
            return make_error_metrics(sql,
                                      "Unsupported SET expression.",
                                      {"Supported forms: column = column +/- integer, column = integer, column = 'text'."});
        }

        if (binary.op != parser::relational::BinaryOperator::Add &&
            binary.op != parser::relational::BinaryOperator::Subtract) {
            txn_scope.release();
            return make_error_metrics(sql,
                                      "Unsupported SET expression.",
                                      {"Supported forms: column = column +/- integer, column = integer, column = 'text'."});
        }

        const auto& left_identifier = static_cast<const parser::relational::IdentifierExpression&>(*binary.left);
        if (!left_identifier.binding.has_value()) {
            txn_scope.release();
            return make_error_metrics(sql, "SET expression column binding is missing.");
        }
        const auto left_key = normalize_identifier(left_identifier.binding->column_name);
        if (left_key != key) {
            txn_scope.release();
            return make_error_metrics(sql,
                                      "SET operations currently require the same source and target column.");
        }

        const auto& right_literal = static_cast<const parser::relational::LiteralExpression&>(*binary.right);
        if (!parse_int64(right_literal.text, delta_value)) {
            txn_scope.release();
            return make_error_metrics(sql,
                                      "SET arithmetic requires an integer literal operand.");
        }

        assignment_kind = (binary.op == parser::relational::BinaryOperator::Add) ?
                              AssignmentKind::AddInteger :
                              AssignmentKind::SubtractInteger;
    } else {
        txn_scope.release();
        return make_error_metrics(sql,
                                  "Unsupported SET expression.",
                                  {"Supported forms: column = column +/- integer, column = integer, column = 'text'."});
    }

    const auto* where_expression = parse_result.statement->where;
    if (where_expression == nullptr ||
        where_expression->kind != parser::relational::NodeKind::BinaryExpression) {
        txn_scope.release();
        return make_error_metrics(sql,
                                  "Unsupported WHERE clause.",
                                  {"Only equality against integer literals is supported."});
    }

    const auto& predicate = static_cast<const parser::relational::BinaryExpression&>(*where_expression);
    if (predicate.op != parser::relational::BinaryOperator::Equal || predicate.left == nullptr ||
        predicate.right == nullptr ||
        predicate.left->kind != parser::relational::NodeKind::IdentifierExpression ||
        predicate.right->kind != parser::relational::NodeKind::LiteralExpression) {
        txn_scope.release();
        return make_error_metrics(sql,
                                  "Unsupported WHERE clause.",
                                  {"Only equality against integer literals is supported."});
    }

    const auto& where_identifier = static_cast<const parser::relational::IdentifierExpression&>(*predicate.left);
    if (!where_identifier.binding.has_value()) {
        txn_scope.release();
        return make_error_metrics(sql, "WHERE clause column binding is missing.");
    }
    const auto where_key = normalize_identifier(where_identifier.binding->column_name);
    auto where_it = table->column_index.find(where_key);
    if (where_it == table->column_index.end()) {
        txn_scope.release();
        return make_error_metrics(sql,
                                  "Column '" + where_identifier.binding->column_name + "' not found in target table.");
    }

    const auto where_index = where_it->second;
    const auto& where_column = table->columns[where_index];
    if (where_column.type != catalog::CatalogColumnType::Int64 &&
        where_column.type != catalog::CatalogColumnType::UInt32 &&
        where_column.type != catalog::CatalogColumnType::UInt16) {
        txn_scope.release();
        return make_error_metrics(sql, "WHERE clause must reference a numeric column.");
    }

    const auto& where_literal = static_cast<const parser::relational::LiteralExpression&>(*predicate.right);
    std::int64_t where_value = 0;
    if (!parse_int64(where_literal.text, where_value)) {
        txn_scope.release();
        return make_error_metrics(sql,
                                  "WHERE clause literal must be an integer value.");
    }

    if (assignment_kind == AssignmentKind::StringConstant &&
        target_column.type != catalog::CatalogColumnType::Utf8) {
        txn_scope.release();
        return make_error_metrics(sql,
                                  "Cannot assign text literal to non-text column '" + target_column.name + "'.");
    }

    if (assignment_kind != AssignmentKind::StringConstant &&
        target_column.type == catalog::CatalogColumnType::Utf8) {
        txn_scope.release();
        return make_error_metrics(sql,
                                  "Cannot assign numeric expression to text column '" + target_column.name + "'.");
    }

    std::string set_expression;
    const auto set_column = assignment.binding->column_name;
    switch (assignment_kind) {
    case AssignmentKind::StringConstant:
        set_expression = set_column + " = '" + string_constant + "'";
        break;
    case AssignmentKind::NumericConstant:
        set_expression = set_column + " = " + std::to_string(numeric_constant);
        break;
    case AssignmentKind::AddInteger:
        set_expression = set_column + " = " + set_column + " + " + std::to_string(delta_value);
        break;
    case AssignmentKind::SubtractInteger:
        set_expression = set_column + " = " + set_column + " - " + std::to_string(delta_value);
        break;
    }

    std::string predicate_expression = where_identifier.binding->column_name + " = " + where_literal.text;
    logical_detail_lines.push_back("logical.update target=" + format_relation_name(*table_binding) +
                                   " set=" + set_expression + " predicate=" + predicate_expression);

    auto constraint_plan_result = build_constraint_enforcement_plan(*table, accessor);
    if (std::holds_alternative<ConstraintPlanError>(constraint_plan_result)) {
        txn_scope.release();
        auto error = std::get<ConstraintPlanError>(std::move(constraint_plan_result));
        return make_error_metrics(sql, std::move(error.message), std::move(error.hints));
    }

    auto constraint_plan = std::get<ConstraintEnforcementPlan>(std::move(constraint_plan_result));
    std::unique_ptr<ShellStorageReader> constraint_reader;
    if (!constraint_plan.index_definitions.empty()) {
        std::unordered_map<std::uint64_t, ShellStorageReader::IndexDefinition> reader_definitions;
        reader_definitions.reserve(constraint_plan.index_definitions.size());
        for (auto& [index_value, definition] : constraint_plan.index_definitions) {
            ShellStorageReader::IndexDefinition reader_definition{};
            reader_definition.table = definition.table;
            reader_definition.column_indexes = std::move(definition.column_indexes);
            reader_definitions.emplace(index_value, std::move(reader_definition));
        }
        constraint_reader = std::make_unique<ShellStorageReader>(nullptr, storage_.get(), std::move(reader_definitions));
    }

    bored::executor::ExecutorTelemetry update_telemetry;

    ShellStorageReader storage_reader{table, storage_.get()};
    bored::executor::SequentialScanExecutor::Config scan_config{};
    scan_config.reader = &storage_reader;
    scan_config.relation_id = table->relation_id;
    scan_config.telemetry = &update_telemetry;

    auto scan_node = std::make_unique<bored::executor::SequentialScanExecutor>(scan_config);

    bored::executor::FilterExecutor::Config filter_config{};
    filter_config.telemetry = &update_telemetry;
    filter_config.predicate = [table, where_index, where_value, decoded = std::vector<ScalarValue>{},
                               row_id = std::uint64_t{}](const bored::executor::TupleView& view,
                                                         bored::executor::ExecutorContext&) mutable {
        if (view.column_count() < 2U) {
            return false;
        }
        const auto payload_column = view.column(1U);
        if (payload_column.is_null) {
            return false;
        }
        if (!decode_row_payload(*table, payload_column.data, row_id, decoded)) {
            return false;
        }
        if (decoded.size() <= where_index) {
            return false;
        }
        if (!std::holds_alternative<std::int64_t>(decoded[where_index])) {
            return false;
        }
        return std::get<std::int64_t>(decoded[where_index]) == where_value;
    };

    auto filter_node = std::make_unique<bored::executor::FilterExecutor>(std::move(scan_node), std::move(filter_config));

    bored::executor::ProjectionExecutor::Config projection_config{};
    projection_config.telemetry = &update_telemetry;
    projection_config.projections.push_back([table,
                                             assignment_kind,
                                             target_index,
                                             numeric_constant,
                                             delta_value,
                                             string_constant,
                                             decoded = std::vector<ScalarValue>{},
                                             updated_values = std::vector<ScalarValue>{},
                                             payload_buffer = std::vector<std::byte>{}](
                                                const bored::executor::TupleView& input,
                                                bored::executor::TupleWriter& writer,
                                                bored::executor::ExecutorContext&) mutable {
        if (input.column_count() < 2U) {
            throw std::runtime_error{"Update projection missing payload column"};
        }
        const auto payload_column = input.column(1U);
        if (payload_column.is_null) {
            throw std::runtime_error{"Update projection encountered null payload"};
        }

        std::uint64_t row_id = 0U;
        if (!decode_row_payload(*table, payload_column.data, row_id, decoded)) {
            throw std::runtime_error{"Failed to decode row payload for update"};
        }

        updated_values = decoded;
        if (updated_values.size() <= target_index) {
            throw std::runtime_error{"Update projection missing target column"};
        }
        switch (assignment_kind) {
        case AssignmentKind::StringConstant:
            updated_values[target_index] = string_constant;
            break;
        case AssignmentKind::NumericConstant:
            updated_values[target_index] = numeric_constant;
            break;
        case AssignmentKind::AddInteger:
        case AssignmentKind::SubtractInteger: {
            if (!std::holds_alternative<std::int64_t>(updated_values[target_index])) {
                throw std::runtime_error{"Cannot apply numeric assignment to non-numeric column"};
            }
            auto current_value = std::get<std::int64_t>(updated_values[target_index]);
            current_value += (assignment_kind == AssignmentKind::AddInteger) ? delta_value : -delta_value;
            updated_values[target_index] = current_value;
            break;
        }
        }

        payload_buffer.clear();
        encode_values_payload(*table, updated_values, payload_buffer);

        std::array<std::byte, sizeof(std::uint64_t)> row_id_bytes{};
        std::memcpy(row_id_bytes.data(), &row_id, sizeof(row_id));

        writer.append_column(std::span<const std::byte>(row_id_bytes.data(), row_id_bytes.size()), false);
        writer.append_column(std::span<const std::byte>(payload_buffer.data(), payload_buffer.size()), false);
    });

    auto projection_node = std::make_unique<bored::executor::ProjectionExecutor>(std::move(filter_node), std::move(projection_config));

    bored::executor::ExecutorNodePtr update_input = std::move(projection_node);
    if (constraint_reader &&
        (!constraint_plan.foreign_key_constraints.empty() || !constraint_plan.unique_constraints.empty())) {
        update_input = apply_constraint_enforcers(std::move(update_input),
                                                  constraint_plan,
                                                  constraint_reader.get(),
                                                  table,
                                                  update_telemetry,
                                                  1U,
                                                  std::optional<std::size_t>{0U});
    }

    ShellUpdateTarget update_target{table, storage_.get()};
    bored::executor::UpdateExecutor::Config update_config{};
    update_config.target = &update_target;
    update_config.telemetry = &update_telemetry;

    bored::executor::UpdateExecutor update_executor{std::move(update_input), update_config};

    bored::executor::ExecutorContext executor_context{};
    executor_context.set_transaction_id(txn_context.id());
    executor_context.set_snapshot(txn_context.snapshot());
    executor_context.set_transaction_context(&txn_context);
    bored::executor::TupleBuffer executor_buffer{};
    update_executor.open(executor_context);
    while (update_executor.next(executor_context, executor_buffer)) {
        executor_buffer.reset();
    }
    update_executor.close(executor_context);

    const std::size_t updated = update_target.updated_count();
    const auto telemetry_snapshot = update_telemetry.snapshot();

    CommandMetrics metrics{};
    metrics.success = true;
    metrics.summary = "Updated " + format_count("row", updated) + ".";
    metrics.rows_touched = updated;
    metrics.wal_bytes = telemetry_snapshot.update_wal_bytes;
    metrics.detail_lines.insert(metrics.detail_lines.end(),
                                logical_detail_lines.begin(),
                                logical_detail_lines.end());
    metrics.detail_lines.push_back("planner.root=" + plan_details.root_detail);
    metrics.detail_lines.insert(metrics.detail_lines.end(),
                                plan_details.detail_lines.begin(),
                                plan_details.detail_lines.end());
    metrics.detail_lines.insert(metrics.detail_lines.end(),
                                executor_detail_lines.begin(),
                                executor_detail_lines.end());

    if (auto ec = catalog_guard.commit()) {
        txn_scope.abort();
        return make_error_metrics(sql,
                                  "Catalog transaction commit failed.",
                                  {"Retry the UPDATE command."});
    }

    txn_scope.commit();

    return metrics;
}

CommandMetrics ShellBackend::execute_delete(const std::string& sql)
{
    if (session_requires_rollback()) {
        return make_error_metrics(sql,
                                  "Transaction is aborted; run ROLLBACK before executing statements.");
    }

    auto parse_result = parser::parse_delete(sql);
    if (!parse_result.success()) {
        return make_parser_error_metrics(sql,
                                         std::move(parse_result.diagnostics),
                                         "Failed to parse DELETE statement.");
    }

    txn::TransactionOptions txn_options{};
    TransactionWorkScope txn_scope{*this, txn_manager_, txn_options};
    auto& txn_context = txn_scope.context();

    auto release_and_return = [&](CommandMetrics metrics) {
        txn_scope.release();
        return metrics;
    };

    catalog::CatalogTransactionConfig tx_cfg{};
    tx_cfg.id_allocator = &txn_allocator_;
    tx_cfg.snapshot_manager = &snapshot_manager_;
    tx_cfg.transaction_manager = &txn_manager_;
    tx_cfg.transaction_context = &txn_context;
    catalog::CatalogTransaction transaction{tx_cfg};
    CatalogTransactionGuard catalog_guard{transaction};

    catalog::CatalogAccessor::Config accessor_cfg{};
    accessor_cfg.transaction = &transaction;
    accessor_cfg.scanner = storage_->make_scanner();
    catalog::CatalogAccessor accessor{accessor_cfg};

    parser::relational::CatalogBinderAdapter binder_catalog{accessor};
    parser::relational::BinderConfig binder_cfg{};
    binder_cfg.catalog = &binder_catalog;
    if (!config_.default_schema_name.empty()) {
        binder_cfg.default_schema = config_.default_schema_name;
    }

    auto binding = parser::relational::bind_delete(binder_cfg, *parse_result.statement);
    if (!binding.success()) {
        return release_and_return(make_parser_error_metrics(sql,
                                                            std::move(binding.diagnostics),
                                                            "Failed to bind DELETE statement."));
    }

    const auto* target_ref = parse_result.statement->target;
    if (target_ref == nullptr || !target_ref->binding.has_value()) {
        return release_and_return(make_error_metrics(sql, "DELETE target table binding is missing."));
    }

    const auto* table_binding = &*target_ref->binding;
    auto* table = find_table(*table_binding);
    if (table == nullptr) {
        return release_and_return(make_error_metrics(sql, "Target table not found."));
    }

    std::vector<std::string> logical_detail_lines;

    auto plan_result = plan_scan_operation(sql,
                                           planner::LogicalOperatorType::Delete,
                                           planner::PhysicalOperatorType::Delete,
                                           *table_binding,
                                           *table,
                                           "Delete",
                                           "DELETE",
                                           accessor,
                                           txn_context);
    if (std::holds_alternative<CommandMetrics>(plan_result)) {
        return release_and_return(std::get<CommandMetrics>(std::move(plan_result)));
    }
    auto plan_details = std::get<PlannerPlanDetails>(std::move(plan_result));

    auto executor_detail_lines = render_executor_plan("DELETE", plan_details.plan);

    const auto* where_expression = parse_result.statement->where;
    if (where_expression == nullptr ||
        where_expression->kind != parser::relational::NodeKind::BinaryExpression) {
        return release_and_return(make_error_metrics(sql,
                                                     "Unsupported WHERE clause.",
                                                     {"Only equality against integer literals is supported."}));
    }

    const auto& predicate = static_cast<const parser::relational::BinaryExpression&>(*where_expression);
    if (predicate.op != parser::relational::BinaryOperator::Equal || predicate.left == nullptr ||
        predicate.right == nullptr ||
        predicate.left->kind != parser::relational::NodeKind::IdentifierExpression ||
        predicate.right->kind != parser::relational::NodeKind::LiteralExpression) {
        return release_and_return(make_error_metrics(sql,
                                                     "Unsupported WHERE clause.",
                                                     {"Only equality against integer literals is supported."}));
    }

    const auto& where_identifier = static_cast<const parser::relational::IdentifierExpression&>(*predicate.left);
    if (!where_identifier.binding.has_value()) {
        return release_and_return(make_error_metrics(sql, "WHERE clause column binding is missing."));
    }
    const auto where_key = normalize_identifier(where_identifier.binding->column_name);
    auto where_it = table->column_index.find(where_key);
    if (where_it == table->column_index.end()) {
        return release_and_return(make_error_metrics(sql,
                                                     "Column '" + where_identifier.binding->column_name +
                                                         "' not found in target table."));
    }

    const auto where_index = where_it->second;
    const auto& where_column = table->columns[where_index];
    if (where_column.type != catalog::CatalogColumnType::Int64 &&
        where_column.type != catalog::CatalogColumnType::UInt32 &&
        where_column.type != catalog::CatalogColumnType::UInt16) {
        return release_and_return(make_error_metrics(sql, "WHERE clause must reference a numeric column."));
    }

    const auto& where_literal = static_cast<const parser::relational::LiteralExpression&>(*predicate.right);
    std::int64_t where_value = 0;
    if (!parse_int64(where_literal.text, where_value)) {
        return release_and_return(make_error_metrics(sql,
                                                     "WHERE clause literal must be an integer value."));
    }

    const std::string predicate_expression = where_identifier.binding->column_name + " = " + where_literal.text;
    logical_detail_lines.push_back("logical.delete target=" + format_relation_name(*table_binding) +
                                   " predicate=" + predicate_expression);

    bored::executor::ExecutorTelemetry delete_telemetry;

    ShellStorageReader storage_reader{table, storage_.get()};
    bored::executor::SequentialScanExecutor::Config scan_config{};
    scan_config.reader = &storage_reader;
    scan_config.relation_id = table->relation_id;
    scan_config.telemetry = &delete_telemetry;

    auto scan_node = std::make_unique<bored::executor::SequentialScanExecutor>(scan_config);

    bored::executor::FilterExecutor::Config filter_config{};
    filter_config.telemetry = &delete_telemetry;
    filter_config.predicate = [table,
                               where_index,
                               where_value,
                               decoded = std::vector<ScalarValue>{},
                               row_id = std::uint64_t{}](const bored::executor::TupleView& input,
                                                         bored::executor::ExecutorContext&) mutable {
        if (input.column_count() < 2U) {
            return false;
        }
        const auto payload_column = input.column(1U);
        if (payload_column.is_null) {
            return false;
        }
        if (!decode_row_payload(*table, payload_column.data, row_id, decoded)) {
            return false;
        }
        if (decoded.size() <= where_index) {
            return false;
        }
        if (!std::holds_alternative<std::int64_t>(decoded[where_index])) {
            return false;
        }
        return std::get<std::int64_t>(decoded[where_index]) == where_value;
    };

    auto filter_node = std::make_unique<bored::executor::FilterExecutor>(std::move(scan_node), std::move(filter_config));

    bored::executor::ProjectionExecutor::Config projection_config{};
    projection_config.telemetry = &delete_telemetry;
    projection_config.projections.push_back(
        [table,
         decoded = std::vector<ScalarValue>{},
         row_id = std::uint64_t{},
         row_id_bytes = std::array<std::byte, sizeof(std::uint64_t)>{}](
            const bored::executor::TupleView& input,
            bored::executor::TupleWriter& writer,
            bored::executor::ExecutorContext&) mutable {
            if (input.column_count() < 2U) {
                throw std::runtime_error{"Delete projection missing payload column"};
            }
            const auto payload_column = input.column(1U);
            if (payload_column.is_null) {
                throw std::runtime_error{"Delete projection encountered null payload"};
            }
            if (!decode_row_payload(*table, payload_column.data, row_id, decoded)) {
                throw std::runtime_error{"Failed to decode row payload for delete"};
            }

            std::memcpy(row_id_bytes.data(), &row_id, sizeof(row_id));
            writer.append_column(std::span<const std::byte>(row_id_bytes.data(), row_id_bytes.size()), false);
        });

    auto projection_node = std::make_unique<bored::executor::ProjectionExecutor>(std::move(filter_node),
                                                                                  std::move(projection_config));

    ShellDeleteTarget delete_target{table, storage_.get()};
    bored::executor::DeleteExecutor::Config delete_config{};
    delete_config.target = &delete_target;
    delete_config.telemetry = &delete_telemetry;

    bored::executor::DeleteExecutor delete_executor{std::move(projection_node), delete_config};

    bored::executor::ExecutorContext executor_context{};
    executor_context.set_transaction_id(txn_context.id());
    executor_context.set_snapshot(txn_context.snapshot());
    executor_context.set_transaction_context(&txn_context);
    bored::executor::TupleBuffer executor_buffer{};
    delete_executor.open(executor_context);
    delete_executor.next(executor_context, executor_buffer);
    delete_executor.close(executor_context);

    const std::size_t deleted = delete_target.deleted_count();
    const auto telemetry_snapshot = delete_telemetry.snapshot();

    CommandMetrics metrics{};
    metrics.success = true;
    metrics.summary = "Deleted " + format_count("row", deleted) + ".";
    metrics.rows_touched = deleted;
    metrics.wal_bytes = telemetry_snapshot.delete_wal_bytes;
    metrics.detail_lines.insert(metrics.detail_lines.end(),
                                logical_detail_lines.begin(),
                                logical_detail_lines.end());
    metrics.detail_lines.push_back("planner.root=" + plan_details.root_detail);
    metrics.detail_lines.insert(metrics.detail_lines.end(),
                                plan_details.detail_lines.begin(),
                                plan_details.detail_lines.end());
    metrics.detail_lines.insert(metrics.detail_lines.end(),
                                executor_detail_lines.begin(),
                                executor_detail_lines.end());

    if (auto ec = catalog_guard.commit()) {
        txn_scope.abort();
        return make_error_metrics(sql,
                                  "Catalog transaction commit failed.",
                                  {"Retry the DELETE command."});
    }

    txn_scope.commit();

    return metrics;
}

CommandMetrics ShellBackend::execute_select(const std::string& sql)
{
    const auto statement = strip_trailing_semicolon(strip_leading_comments(sql));
    if (statement.empty()) {
        return make_error_metrics(sql, "SELECT statement is empty.");
    }

    if (session_requires_rollback()) {
        return make_error_metrics(sql,
                                  "Transaction is aborted; run ROLLBACK before executing statements.");
    }

    auto parse_result = parser::parse_select(statement);
    if (!parse_result.success()) {
        return make_parser_error_metrics(sql, std::move(parse_result.diagnostics), "Failed to parse SELECT statement.");
    }

    txn::TransactionOptions txn_options{};
    txn_options.read_only = true;
    TransactionWorkScope txn_scope{*this, txn_manager_, txn_options};
    auto& txn_context = txn_scope.context();

    auto release_and_return = [&](CommandMetrics metrics) {
        txn_scope.release();
        return metrics;
    };

    catalog::CatalogTransactionConfig tx_cfg{};
    tx_cfg.id_allocator = &txn_allocator_;
    tx_cfg.snapshot_manager = &snapshot_manager_;
    tx_cfg.transaction_manager = &txn_manager_;
    tx_cfg.transaction_context = &txn_context;
    catalog::CatalogTransaction transaction{tx_cfg};
    CatalogTransactionGuard catalog_guard{transaction};

    catalog::CatalogAccessor::Config accessor_cfg{};
    accessor_cfg.transaction = &transaction;
    accessor_cfg.scanner = storage_->make_scanner();
    catalog::CatalogAccessor accessor{accessor_cfg};

    parser::relational::CatalogBinderAdapter binder_catalog{accessor};
    parser::relational::BinderConfig binder_cfg{};
    binder_cfg.catalog = &binder_catalog;
    if (!config_.default_schema_name.empty()) {
        binder_cfg.default_schema = config_.default_schema_name;
    }

    auto binding = parser::relational::bind_select(binder_cfg, *parse_result.statement);
    if (!binding.success()) {
        return release_and_return(make_parser_error_metrics(sql,
                                                            std::move(binding.diagnostics),
                                                            "Failed to bind SELECT statement."));
    }

    auto lowering = parser::relational::lower_select(*parse_result.statement);
    if (!lowering.success()) {
        return release_and_return(make_parser_error_metrics(sql,
                                                            std::move(lowering.diagnostics),
                                                            "Failed to lower SELECT statement."));
    }

    std::vector<std::string> logical_detail_lines;
    if (lowering.plan != nullptr) {
        logical_detail_lines = render_logical_plan_lines(*lowering.plan);
    }

    std::string plan_error;
    std::vector<std::string> plan_hints;
    auto plan = build_simple_select_plan(*parse_result.statement, plan_error, plan_hints);
    if (!plan.has_value()) {
        return release_and_return(make_error_metrics(sql, std::move(plan_error), std::move(plan_hints)));
    }

    const auto* query = parse_result.statement->query;
    if (query == nullptr || query->from == nullptr || !query->from->binding.has_value()) {
        return release_and_return(make_error_metrics(sql, "SELECT target table binding is missing."));
    }
    const auto* table_binding = &*query->from->binding;

    TableData* table = nullptr;
    table = find_table(*table_binding);
    if (table == nullptr) {
        return release_and_return(make_error_metrics(sql, "Target table not found."));
    }

    std::vector<std::size_t> column_indexes;
    std::vector<std::string> selected_columns;
    if (plan->select_all) {
        column_indexes.reserve(table->columns.size());
        selected_columns.reserve(table->columns.size());
        for (const auto& column : table->columns) {
            column_indexes.push_back(column.ordinal);
            selected_columns.push_back(column.name);
        }
    } else {
        column_indexes.reserve(plan->column_names.size());
        selected_columns.reserve(plan->column_names.size());
        for (const auto& column_name : plan->column_names) {
            const auto key = normalize_identifier(column_name);
            auto it = table->column_index.find(key);
            if (it == table->column_index.end()) {
                return release_and_return(make_error_metrics(sql, "Column '" + column_name + "' not found in target table."));
            }
            column_indexes.push_back(it->second);
            selected_columns.push_back(table->columns[it->second].name);
        }
    }

    auto plan_result = plan_select_operation(sql, *table_binding, *table, selected_columns, accessor, txn_context);
    if (std::holds_alternative<CommandMetrics>(plan_result)) {
        return release_and_return(std::get<CommandMetrics>(std::move(plan_result)));
    }
    auto plan_details = std::get<PlannerPlanDetails>(std::move(plan_result));

    auto executor_detail_lines = render_executor_plan("SELECT", plan_details.plan);

    std::vector<std::string> header_names = selected_columns;

    std::optional<std::size_t> order_index;
    if (plan->order_by_column.has_value()) {
        const auto key = normalize_identifier(*plan->order_by_column);
        auto it = table->column_index.find(key);
        if (it == table->column_index.end()) {
            return release_and_return(make_error_metrics(sql, "ORDER BY column '" + *plan->order_by_column + "' not found."));
        }
        order_index = it->second;
    }

    bored::executor::ExecutorTelemetry select_telemetry;

    ShellStorageReader storage_reader{table, storage_.get()};
    bored::executor::SequentialScanExecutor::Config scan_config{};
    scan_config.reader = &storage_reader;
    scan_config.relation_id = table->relation_id;
    scan_config.telemetry = &select_telemetry;

    bored::executor::SequentialScanExecutor scan_executor{scan_config};
    bored::executor::ExecutorContext executor_context{};
    executor_context.set_transaction_id(txn_context.id());
    executor_context.set_snapshot(txn_context.snapshot());
    executor_context.set_transaction_context(&txn_context);
    bored::executor::TupleBuffer executor_buffer{};

    std::vector<std::vector<ScalarValue>> materialized_rows;
    scan_executor.open(executor_context);
    while (scan_executor.next(executor_context, executor_buffer)) {
        auto tuple_view = bored::executor::TupleView::from_buffer(executor_buffer);
        if (!tuple_view.valid() || tuple_view.column_count() < 2U) {
            scan_executor.close(executor_context);
            return release_and_return(make_error_metrics(sql, "Executor returned invalid tuple payload."));
        }

        const auto payload_column = tuple_view.column(1U);
        if (payload_column.is_null) {
            scan_executor.close(executor_context);
            return release_and_return(make_error_metrics(sql, "Executor returned null tuple payload."));
        }

        std::uint64_t row_id = 0U;
        std::vector<ScalarValue> decoded_values;
        if (!decode_row_payload(*table, payload_column.data, row_id, decoded_values)) {
            scan_executor.close(executor_context);
            return release_and_return(make_error_metrics(sql, "Failed to decode tuple payload."));
        }

        materialized_rows.push_back(std::move(decoded_values));
        executor_buffer.reset();
    }
    scan_executor.close(executor_context);

    std::vector<std::size_t> order(materialized_rows.size());
    std::iota(order.begin(), order.end(), 0U);

    if (order_index.has_value()) {
        std::stable_sort(order.begin(), order.end(), [&](std::size_t lhs, std::size_t rhs) {
            const auto& left_row = materialized_rows[lhs];
            const auto& right_row = materialized_rows[rhs];
            if (left_row.size() <= *order_index || right_row.size() <= *order_index) {
                return false;
            }
            return compare_values(left_row[*order_index], right_row[*order_index]) < 0;
        });
    }

    std::vector<std::size_t> widths(header_names.size(), 0U);
    for (std::size_t index = 0; index < header_names.size(); ++index) {
        widths[index] = header_names[index].size();
    }

    std::vector<std::vector<std::string>> rendered_rows;
    rendered_rows.reserve(order.size());
    for (auto row_index : order) {
        const auto& row_values = materialized_rows[row_index];
        std::vector<std::string> rendered;
        rendered.reserve(column_indexes.size());
        for (std::size_t index = 0; index < column_indexes.size(); ++index) {
            const auto column_index = column_indexes[index];
            if (row_values.size() <= column_index) {
                return release_and_return(make_error_metrics(sql, "Executor row payload is missing projected column."));
            }
            const auto value_text = value_to_string(row_values[column_index]);
            widths[index] = std::max(widths[index], value_text.size());
            rendered.push_back(value_text);
        }
        rendered_rows.push_back(std::move(rendered));
    }

    const auto telemetry_snapshot = select_telemetry.snapshot();

    CommandMetrics metrics{};
    metrics.success = true;
    metrics.summary = "Selected " + format_count("row", rendered_rows.size()) + ".";
    metrics.rows_touched = static_cast<std::size_t>(telemetry_snapshot.seq_scan_rows_visible);

    metrics.detail_lines.insert(metrics.detail_lines.end(),
                                logical_detail_lines.begin(),
                                logical_detail_lines.end());
    if (!plan_details.root_detail.empty()) {
        metrics.detail_lines.push_back("planner.root=" + plan_details.root_detail);
    }
    metrics.detail_lines.insert(metrics.detail_lines.end(),
                                plan_details.detail_lines.begin(),
                                plan_details.detail_lines.end());
    metrics.detail_lines.insert(metrics.detail_lines.end(),
                                executor_detail_lines.begin(),
                                executor_detail_lines.end());

    if (!header_names.empty()) {
        std::ostringstream header_stream;
        header_stream << std::left;
        for (std::size_t index = 0; index < header_names.size(); ++index) {
            if (index > 0U) {
                header_stream << " | ";
            }
            header_stream << std::setw(static_cast<int>(widths[index])) << header_names[index];
        }
        metrics.detail_lines.push_back(header_stream.str());

        std::ostringstream separator_stream;
        for (std::size_t index = 0; index < widths.size(); ++index) {
            if (index > 0U) {
                separator_stream << "-+-";
            }
            separator_stream << std::string(widths[index], '-');
        }
        metrics.detail_lines.push_back(separator_stream.str());
    }

    for (const auto& rendered_row : rendered_rows) {
        std::ostringstream row_stream;
        row_stream << std::left;
        for (std::size_t index = 0; index < rendered_row.size(); ++index) {
            if (index > 0U) {
                row_stream << " | ";
            }
            row_stream << std::setw(static_cast<int>(widths[index])) << rendered_row[index];
        }
        metrics.detail_lines.push_back(row_stream.str());
    }

    if (auto ec = catalog_guard.commit()) {
        txn_scope.abort();
        return make_error_metrics(sql,
                                  "Catalog transaction commit failed.",
                                  {"Retry the SELECT command."});
    }

    txn_scope.commit_read_only();

    return metrics;
}

}  // namespace bored::shell
