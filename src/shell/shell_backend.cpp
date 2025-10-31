#include "bored/shell/shell_backend.hpp"

#include "bored/catalog/catalog_accessor.hpp"
#include "bored/catalog/catalog_cache.hpp"
#include "bored/catalog/catalog_encoding.hpp"
#include "bored/catalog/catalog_introspection.hpp"
#include "bored/catalog/catalog_mutator.hpp"
#include "bored/catalog/catalog_transaction.hpp"
#include "bored/ddl/ddl_dispatcher.hpp"
#include "bored/ddl/ddl_handlers.hpp"
#include "bored/ddl/ddl_validation.hpp"
#include "bored/parser/ddl_command_builder.hpp"
#include "bored/parser/grammar.hpp"
#include "bored/parser/relational/binder.hpp"
#include "bored/parser/relational/catalog_binder_adapter.hpp"
#include "bored/planner/plan_printer.hpp"
#include "bored/planner/planner.hpp"
#include "bored/storage/storage_telemetry_registry.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <iomanip>
#include <limits>
#include <map>
#include <optional>
#include <sstream>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace bored::shell {

namespace {

constexpr std::string_view kInsertKeyword = "INSERT";
constexpr std::string_view kUpdateKeyword = "UPDATE";
constexpr std::string_view kDeleteKeyword = "DELETE";
constexpr std::string_view kSelectKeyword = "SELECT";

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
    }
    return "Unknown";
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
 
struct ShellBackend::InMemoryCatalogStorage final {
    using Relation = std::map<std::uint64_t, std::vector<std::byte>>;

    void seed(catalog::RelationId relation_id, std::uint64_t row_id, std::vector<std::byte> payload)
    {
        relations_[relation_id.value][row_id] = std::move(payload);
    }

    void apply(const catalog::CatalogMutationBatch& batch)
    {
        for (const auto& mutation : batch.mutations) {
            auto& relation = relations_[mutation.relation_id.value];
            switch (mutation.kind) {
            case catalog::CatalogMutationKind::Insert:
            case catalog::CatalogMutationKind::Update:
                if (mutation.after) {
                    relation[mutation.row_id] = mutation.after->payload;
                }
                break;
            case catalog::CatalogMutationKind::Delete:
                relation.erase(mutation.row_id);
                break;
            }
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
            if (!view) {
                continue;
            }
            if (view->database_id == database_id && view->name == name) {
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
    }

    std::unordered_map<std::uint64_t, Relation> relations_{};
};

namespace {

using ScalarValue = std::variant<std::int64_t, std::string>;

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

}  // namespace

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
    , storage_{std::make_unique<InMemoryCatalogStorage>()}
    , snapshot_manager_{make_relaxed_snapshot()}
    , txn_manager_{txn_allocator_}
{
    catalog::CatalogCache::instance().reset();

    identifier_allocator_ = std::make_unique<InMemoryIdentifierAllocator>();

    {
        catalog::CatalogDatabaseDescriptor descriptor{};
        descriptor.tuple.xmin = catalog::kCatalogBootstrapTxnId;
        descriptor.tuple.xmax = 0U;
        descriptor.database_id = catalog::kSystemDatabaseId;
        descriptor.default_schema_id = catalog::kSystemSchemaId;
        descriptor.name = "system";
        storage_->seed(catalog::kCatalogDatabasesRelationId,
                       descriptor.database_id.value,
                       catalog::serialize_catalog_database(descriptor));
    }

    {
        catalog::CatalogSchemaDescriptor descriptor{};
        descriptor.tuple.xmin = catalog::kCatalogBootstrapTxnId;
        descriptor.tuple.xmax = 0U;
        descriptor.schema_id = catalog::kSystemSchemaId;
        descriptor.database_id = catalog::kSystemDatabaseId;
        descriptor.name = "system";
        storage_->seed(catalog::kCatalogSchemasRelationId,
                       descriptor.schema_id.value,
                       catalog::serialize_catalog_schema(descriptor));
    }

    default_schema_id_ = config_.default_schema_id.value_or(catalog::SchemaId{42U});

    {
        catalog::CatalogSchemaDescriptor descriptor{};
        descriptor.tuple.xmin = catalog::kCatalogBootstrapTxnId;
        descriptor.tuple.xmax = 0U;
        descriptor.schema_id = default_schema_id_;
        descriptor.database_id = default_database_id_;
        descriptor.name = config_.default_schema_name;
        storage_->seed(catalog::kCatalogSchemasRelationId,
                       descriptor.schema_id.value,
                       catalog::serialize_catalog_schema(descriptor));
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
    catalog::CatalogTransactionConfig tx_cfg{&txn_allocator_, &snapshot_manager_};
    catalog::CatalogTransaction transaction{tx_cfg};
    transaction.bind_transaction_context(&txn_manager_, nullptr);

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
        auto existing = table_cache_.find(relation_id);
        if (existing != table_cache_.end()) {
            const auto& existing_table = existing->second;
            if (existing_table.columns.size() == data.columns.size()) {
                bool compatible = true;
                for (std::size_t index = 0; index < data.columns.size(); ++index) {
                    const auto& new_column = data.columns[index];
                    const auto& old_column = existing_table.columns[index];
                    if (normalize_identifier(new_column.name) != normalize_identifier(old_column.name) ||
                        new_column.type != old_column.type) {
                        compatible = false;
                        break;
                    }
                }
                if (compatible) {
                    data.rows = existing_table.rows;
                }
            }
        }

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
    std::string_view statement_label)
{
    auto planner_columns = collect_table_columns(table);

    planner::LogicalProperties scan_props{};
    scan_props.relation_name = format_relation_name(table_binding);
    scan_props.output_columns = planner_columns;

    auto scan_logical = planner::LogicalOperator::make(
        planner::LogicalOperatorType::TableScan,
        std::vector<planner::LogicalOperatorPtr>{},
        scan_props);

    planner::LogicalProperties root_props{};
    root_props.relation_name = scan_props.relation_name;
    root_props.output_columns = planner_columns;

    auto root_logical = planner::LogicalOperator::make(
        logical_type,
        std::vector<planner::LogicalOperatorPtr>{scan_logical},
        root_props);

    planner::LogicalPlan logical_plan{root_logical};
    planner::PlannerContext planner_context{};
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
    if (plan_root->children().empty() ||
        plan_root->children().front()->type() != planner::PhysicalOperatorType::SeqScan) {
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
    std::vector<std::string> projection_columns)
{
    auto planner_columns = collect_table_columns(table);
    if (projection_columns.empty()) {
        projection_columns = planner_columns;
    }

    planner::LogicalProperties scan_props{};
    scan_props.relation_name = format_relation_name(table_binding);
    scan_props.output_columns = planner_columns;

    auto scan_logical = planner::LogicalOperator::make(
        planner::LogicalOperatorType::TableScan,
        std::vector<planner::LogicalOperatorPtr>{},
        scan_props);

    planner::LogicalProperties projection_props{};
    projection_props.relation_name = scan_props.relation_name;
    projection_props.output_columns = projection_columns;

    auto projection_logical = planner::LogicalOperator::make(
        planner::LogicalOperatorType::Projection,
        std::vector<planner::LogicalOperatorPtr>{scan_logical},
        projection_props);

    planner::LogicalPlan logical_plan{projection_logical};
    planner::PlannerContext planner_context{};
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

std::variant<std::vector<std::string>, CommandMetrics> ShellBackend::simulate_executor_plan(
    const std::string& sql,
    std::string_view statement_label,
    planner::PhysicalPlan plan)
{
    const auto root = plan.root();
    if (!root) {
        return make_error_metrics(sql,
                                  "Planner produced empty physical plan for " + std::string(statement_label) + ".",
                                  {"Report this to the bored_shell maintainers."});
    }

    std::vector<std::string> detail_lines;
    std::string summary = "executor.stub=";
    summary.append(statement_label);
    summary.append(" pipeline ready (root=");
    summary.append(physical_operator_name(root->type()));
    if (!root->children().empty() && root->children().front()) {
        summary.append(", first_child=");
        summary.append(physical_operator_name(root->children().front()->type()));
    }
    summary.append(")");
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
    descriptor.database_id = catalog::DatabaseId{candidate};
    descriptor.default_schema_id = default_schema_id_;
    descriptor.name = request.name;

    storage_->seed(catalog::kCatalogDatabasesRelationId,
                   descriptor.database_id.value,
                   catalog::serialize_catalog_database(descriptor));

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

    std::unordered_set<std::uint64_t> relation_ids;
    const auto tables = storage_->list_tables();
    for (const auto& table : tables) {
        if (schema_ids.count(table.schema_id.value) != 0U) {
            relation_ids.insert(table.relation_id.value);
            storage_->erase_row(catalog::kCatalogTablesRelationId, table.relation_id.value);
        }
    }

    const auto columns = storage_->list_columns();
    for (const auto& column : columns) {
        if (relation_ids.count(column.relation_id.value) != 0U) {
            storage_->erase_row(catalog::kCatalogColumnsRelationId, column.column_id.value);
        }
    }

    for (const auto& schema : schemas) {
        if (schema.database_id == database_id) {
            storage_->erase_row(catalog::kCatalogSchemasRelationId, schema.schema_id.value);
        }
    }

    storage_->erase_row(catalog::kCatalogDatabasesRelationId, database_id.value);

    refresh_table_cache();

    return ddl::make_success();
}

CommandMetrics ShellBackend::execute_dml(const std::string& sql)
{
    const auto normalized = strip_leading_comments(sql);
    if (normalized.empty()) {
        return make_error_metrics(sql, "DML command is empty.");
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

CommandMetrics ShellBackend::execute_insert(const std::string& sql)
{
    auto parse_result = parser::parse_insert(sql);
    if (!parse_result.success()) {
        return make_parser_error_metrics(sql,
                                         std::move(parse_result.diagnostics),
                                         "Failed to parse INSERT statement.");
    }

    catalog::CatalogTransactionConfig tx_cfg{&txn_allocator_, &snapshot_manager_};
    catalog::CatalogTransaction transaction{tx_cfg};
    transaction.bind_transaction_context(&txn_manager_, nullptr);

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
        return make_parser_error_metrics(sql,
                                         std::move(binding.diagnostics),
                                         "Failed to bind INSERT statement.");
    }

    const auto* target_ref = parse_result.statement->target;
    if (target_ref == nullptr || !target_ref->binding.has_value()) {
        return make_error_metrics(sql, "INSERT target table binding is missing.");
    }

    const auto* table_binding = &*target_ref->binding;
    auto* table = table_binding->schema_name.empty() ?
                      find_table_or_default_schema(table_binding->table_name) :
                      find_table(table_binding->schema_name + "." + table_binding->table_name);
    if (table == nullptr) {
        return make_error_metrics(sql, "Target table not found.", {"Verify the table exists."});
    }

    if (parse_result.statement->columns.size() != table->columns.size()) {
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
            return make_error_metrics(sql, "INSERT column binding is missing.");
        }
        planner_columns.push_back(column.binding->column_name);
        const auto key = normalize_identifier(column.binding->column_name);
        auto it = table->column_index.find(key);
        if (it == table->column_index.end()) {
            return make_error_metrics(sql,
                                      "Column '" + column.binding->column_name + "' not found in target table.");
        }
        column_indexes.push_back(it->second);
    }

    // Build a logical plan so planner integration can be validated prior to executor wiring.
    planner::LogicalProperties insert_props{};
    insert_props.relation_name = format_relation_name(*table_binding);
    insert_props.output_columns = planner_columns;

    auto values_node = planner::LogicalOperator::make(planner::LogicalOperatorType::Values);
    auto insert_logical = planner::LogicalOperator::make(
        planner::LogicalOperatorType::Insert,
        std::vector<planner::LogicalOperatorPtr>{values_node},
        insert_props);

    planner::LogicalPlan logical_plan{insert_logical};
    planner::PlannerContext planner_context{};
    auto planner_result = planner::plan_query(planner_context, logical_plan);
    if (!planner_result.plan.root()) {
        return make_planner_error_metrics(sql,
                                          std::move(planner_result.diagnostics),
                                          "Failed to plan INSERT statement.");
    }

    const auto plan_root = planner_result.plan.root();
    if (plan_root->type() != planner::PhysicalOperatorType::Insert) {
        return make_planner_error_metrics(sql,
                                          std::move(planner_result.diagnostics),
                                          "Planner produced unexpected physical operator for INSERT.");
    }
    if (plan_root->children().empty() ||
        plan_root->children().front()->type() != planner::PhysicalOperatorType::Values) {
        return make_planner_error_metrics(sql,
                                          std::move(planner_result.diagnostics),
                                          "Planner INSERT plan is missing VALUES child.");
    }

    const auto& planned_columns = plan_root->properties().output_columns;
    if (!planned_columns.empty() && planned_columns != planner_columns) {
        return make_planner_error_metrics(sql,
                                          std::move(planner_result.diagnostics),
                                          "Planner column projection does not match INSERT column list.");
    }

    std::vector<std::string> plan_detail_lines = std::move(planner_result.diagnostics);

    std::size_t inserted = 0U;
    for (const auto& row_node : parse_result.statement->rows) {
        if (row_node.values.size() != column_indexes.size()) {
            return make_error_metrics(sql, "VALUES list does not match column count.");
        }

        std::vector<ScalarValue> row_values(table->columns.size());
        for (std::size_t index = 0U; index < column_indexes.size(); ++index) {
            const auto column_index = column_indexes[index];
            const auto& column_info = table->columns[column_index];
            const auto* expression = row_node.values[index];
            if (expression == nullptr || expression->kind != parser::relational::NodeKind::LiteralExpression) {
                return make_error_metrics(sql, "INSERT values must be literals.");
            }

            const auto& literal = static_cast<const parser::relational::LiteralExpression&>(*expression);
            if (literal.tag == parser::relational::LiteralTag::Null) {
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
                return make_error_metrics(sql,
                                          "Column '" + column_info.name + "' expects a numeric literal.");
            }
            row_values[column_index] = numeric_value;
        }

        table->rows.push_back(std::move(row_values));
        ++inserted;
    }

    CommandMetrics metrics{};
    metrics.success = true;
    metrics.summary = "Inserted " + format_count("row", inserted) + ".";
    metrics.rows_touched = inserted;
    metrics.detail_lines.push_back("planner.root=Insert (children=1)");
    metrics.detail_lines.insert(metrics.detail_lines.end(),
                                plan_detail_lines.begin(),
                                plan_detail_lines.end());
    return metrics;
}

CommandMetrics ShellBackend::execute_update(const std::string& sql)
{
    auto parse_result = parser::parse_update(sql);
    if (!parse_result.success()) {
        return make_parser_error_metrics(sql,
                                         std::move(parse_result.diagnostics),
                                         "Failed to parse UPDATE statement.");
    }

    catalog::CatalogTransactionConfig tx_cfg{&txn_allocator_, &snapshot_manager_};
    catalog::CatalogTransaction transaction{tx_cfg};
    transaction.bind_transaction_context(&txn_manager_, nullptr);

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
        return make_parser_error_metrics(sql,
                                         std::move(binding.diagnostics),
                                         "Failed to bind UPDATE statement.");
    }

    const auto* target_ref = parse_result.statement->target;
    if (target_ref == nullptr || !target_ref->binding.has_value()) {
        return make_error_metrics(sql, "UPDATE target table binding is missing.");
    }

    const auto* table_binding = &*target_ref->binding;
    auto* table = table_binding->schema_name.empty() ?
                      find_table_or_default_schema(table_binding->table_name) :
                      find_table(table_binding->schema_name + "." + table_binding->table_name);
    if (table == nullptr) {
        return make_error_metrics(sql, "Target table not found.");
    }

    if (parse_result.statement->assignments.empty()) {
        return make_error_metrics(sql, "UPDATE statement is missing a SET assignment.");
    }

    const auto& assignment = parse_result.statement->assignments.front();
    if (!assignment.binding.has_value() || assignment.value == nullptr) {
        return make_error_metrics(sql, "UPDATE assignment binding is missing.");
    }

    const auto key = normalize_identifier(assignment.binding->column_name);
    auto target_it = table->column_index.find(key);
    if (target_it == table->column_index.end()) {
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
                                           "UPDATE");
    if (std::holds_alternative<CommandMetrics>(plan_result)) {
        return std::get<CommandMetrics>(std::move(plan_result));
    }
    auto plan_details = std::get<PlannerPlanDetails>(std::move(plan_result));

    auto executor_stub = simulate_executor_plan(sql, "UPDATE", std::move(plan_details.plan));
    if (std::holds_alternative<CommandMetrics>(executor_stub)) {
        return std::get<CommandMetrics>(std::move(executor_stub));
    }
    auto executor_detail_lines = std::get<std::vector<std::string>>(std::move(executor_stub));

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
            return make_error_metrics(sql,
                                      "Unsupported SET expression.",
                                      {"Supported forms: column = column +/- integer, column = integer, column = 'text'."});
        }

        if (binary.op != parser::relational::BinaryOperator::Add &&
            binary.op != parser::relational::BinaryOperator::Subtract) {
            return make_error_metrics(sql,
                                      "Unsupported SET expression.",
                                      {"Supported forms: column = column +/- integer, column = integer, column = 'text'."});
        }

        const auto& left_identifier = static_cast<const parser::relational::IdentifierExpression&>(*binary.left);
        if (!left_identifier.binding.has_value()) {
            return make_error_metrics(sql, "SET expression column binding is missing.");
        }
        const auto left_key = normalize_identifier(left_identifier.binding->column_name);
        if (left_key != key) {
            return make_error_metrics(sql,
                                      "SET operations currently require the same source and target column.");
        }

        const auto& right_literal = static_cast<const parser::relational::LiteralExpression&>(*binary.right);
        if (!parse_int64(right_literal.text, delta_value)) {
            return make_error_metrics(sql,
                                      "SET arithmetic requires an integer literal operand.");
        }

        assignment_kind = (binary.op == parser::relational::BinaryOperator::Add) ?
                              AssignmentKind::AddInteger :
                              AssignmentKind::SubtractInteger;
    } else {
        return make_error_metrics(sql,
                                  "Unsupported SET expression.",
                                  {"Supported forms: column = column +/- integer, column = integer, column = 'text'."});
    }

    const auto* where_expression = parse_result.statement->where;
    if (where_expression == nullptr ||
        where_expression->kind != parser::relational::NodeKind::BinaryExpression) {
        return make_error_metrics(sql,
                                  "Unsupported WHERE clause.",
                                  {"Only equality against integer literals is supported."});
    }

    const auto& predicate = static_cast<const parser::relational::BinaryExpression&>(*where_expression);
    if (predicate.op != parser::relational::BinaryOperator::Equal || predicate.left == nullptr ||
        predicate.right == nullptr ||
        predicate.left->kind != parser::relational::NodeKind::IdentifierExpression ||
        predicate.right->kind != parser::relational::NodeKind::LiteralExpression) {
        return make_error_metrics(sql,
                                  "Unsupported WHERE clause.",
                                  {"Only equality against integer literals is supported."});
    }

    const auto& where_identifier = static_cast<const parser::relational::IdentifierExpression&>(*predicate.left);
    if (!where_identifier.binding.has_value()) {
        return make_error_metrics(sql, "WHERE clause column binding is missing.");
    }
    const auto where_key = normalize_identifier(where_identifier.binding->column_name);
    auto where_it = table->column_index.find(where_key);
    if (where_it == table->column_index.end()) {
        return make_error_metrics(sql,
                                  "Column '" + where_identifier.binding->column_name + "' not found in target table.");
    }

    const auto where_index = where_it->second;
    const auto& where_column = table->columns[where_index];
    if (where_column.type != catalog::CatalogColumnType::Int64 &&
        where_column.type != catalog::CatalogColumnType::UInt32 &&
        where_column.type != catalog::CatalogColumnType::UInt16) {
        return make_error_metrics(sql, "WHERE clause must reference a numeric column.");
    }

    const auto& where_literal = static_cast<const parser::relational::LiteralExpression&>(*predicate.right);
    std::int64_t where_value = 0;
    if (!parse_int64(where_literal.text, where_value)) {
        return make_error_metrics(sql,
                                  "WHERE clause literal must be an integer value.");
    }

    if (assignment_kind == AssignmentKind::StringConstant &&
        target_column.type != catalog::CatalogColumnType::Utf8) {
        return make_error_metrics(sql,
                                  "Cannot assign text literal to non-text column '" + target_column.name + "'.");
    }

    if (assignment_kind != AssignmentKind::StringConstant &&
        target_column.type == catalog::CatalogColumnType::Utf8) {
        return make_error_metrics(sql,
                                  "Cannot assign numeric expression to text column '" + target_column.name + "'.");
    }

    std::size_t updated = 0U;
    for (auto& row : table->rows) {
        if (!std::holds_alternative<std::int64_t>(row[where_index])) {
            continue;
        }
        if (std::get<std::int64_t>(row[where_index]) != where_value) {
            continue;
        }

        switch (assignment_kind) {
        case AssignmentKind::StringConstant:
            row[target_index] = string_constant;
            break;
        case AssignmentKind::NumericConstant:
            row[target_index] = numeric_constant;
            break;
        case AssignmentKind::AddInteger:
        case AssignmentKind::SubtractInteger: {
            if (!std::holds_alternative<std::int64_t>(row[target_index])) {
                return make_error_metrics(sql,
                                          "Cannot apply numeric assignment to non-numeric column '" +
                                              target_column.name + "'.");
            }
            auto current_value = std::get<std::int64_t>(row[target_index]);
            current_value += (assignment_kind == AssignmentKind::AddInteger) ? delta_value : -delta_value;
            row[target_index] = current_value;
            break;
        }
        }
        ++updated;
    }

    CommandMetrics metrics{};
    metrics.success = true;
    metrics.summary = "Updated " + format_count("row", updated) + ".";
    metrics.rows_touched = updated;
    metrics.detail_lines.push_back("planner.root=" + plan_details.root_detail);
    metrics.detail_lines.insert(metrics.detail_lines.end(),
                                plan_details.detail_lines.begin(),
                                plan_details.detail_lines.end());
    metrics.detail_lines.insert(metrics.detail_lines.end(),
                                executor_detail_lines.begin(),
                                executor_detail_lines.end());
    return metrics;
}

CommandMetrics ShellBackend::execute_delete(const std::string& sql)
{
    auto parse_result = parser::parse_delete(sql);
    if (!parse_result.success()) {
        return make_parser_error_metrics(sql,
                                         std::move(parse_result.diagnostics),
                                         "Failed to parse DELETE statement.");
    }

    catalog::CatalogTransactionConfig tx_cfg{&txn_allocator_, &snapshot_manager_};
    catalog::CatalogTransaction transaction{tx_cfg};
    transaction.bind_transaction_context(&txn_manager_, nullptr);

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
        return make_parser_error_metrics(sql,
                                         std::move(binding.diagnostics),
                                         "Failed to bind DELETE statement.");
    }

    const auto* target_ref = parse_result.statement->target;
    if (target_ref == nullptr || !target_ref->binding.has_value()) {
        return make_error_metrics(sql, "DELETE target table binding is missing.");
    }

    const auto* table_binding = &*target_ref->binding;
    auto* table = table_binding->schema_name.empty() ?
                      find_table_or_default_schema(table_binding->table_name) :
                      find_table(table_binding->schema_name + "." + table_binding->table_name);
    if (table == nullptr) {
        return make_error_metrics(sql, "Target table not found.");
    }

    auto plan_result = plan_scan_operation(sql,
                                           planner::LogicalOperatorType::Delete,
                                           planner::PhysicalOperatorType::Delete,
                                           *table_binding,
                                           *table,
                                           "Delete",
                                           "DELETE");
    if (std::holds_alternative<CommandMetrics>(plan_result)) {
        return std::get<CommandMetrics>(std::move(plan_result));
    }
    auto plan_details = std::get<PlannerPlanDetails>(std::move(plan_result));

    auto executor_stub = simulate_executor_plan(sql, "DELETE", std::move(plan_details.plan));
    if (std::holds_alternative<CommandMetrics>(executor_stub)) {
        return std::get<CommandMetrics>(std::move(executor_stub));
    }
    auto executor_detail_lines = std::get<std::vector<std::string>>(std::move(executor_stub));

    const auto* where_expression = parse_result.statement->where;
    if (where_expression == nullptr ||
        where_expression->kind != parser::relational::NodeKind::BinaryExpression) {
        return make_error_metrics(sql,
                                  "Unsupported WHERE clause.",
                                  {"Only equality against integer literals is supported."});
    }

    const auto& predicate = static_cast<const parser::relational::BinaryExpression&>(*where_expression);
    if (predicate.op != parser::relational::BinaryOperator::Equal || predicate.left == nullptr ||
        predicate.right == nullptr ||
        predicate.left->kind != parser::relational::NodeKind::IdentifierExpression ||
        predicate.right->kind != parser::relational::NodeKind::LiteralExpression) {
        return make_error_metrics(sql,
                                  "Unsupported WHERE clause.",
                                  {"Only equality against integer literals is supported."});
    }

    const auto& where_identifier = static_cast<const parser::relational::IdentifierExpression&>(*predicate.left);
    if (!where_identifier.binding.has_value()) {
        return make_error_metrics(sql, "WHERE clause column binding is missing.");
    }
    const auto where_key = normalize_identifier(where_identifier.binding->column_name);
    auto where_it = table->column_index.find(where_key);
    if (where_it == table->column_index.end()) {
        return make_error_metrics(sql,
                                  "Column '" + where_identifier.binding->column_name + "' not found in target table.");
    }

    const auto where_index = where_it->second;
    const auto& where_column = table->columns[where_index];
    if (where_column.type != catalog::CatalogColumnType::Int64 &&
        where_column.type != catalog::CatalogColumnType::UInt32 &&
        where_column.type != catalog::CatalogColumnType::UInt16) {
        return make_error_metrics(sql, "WHERE clause must reference a numeric column.");
    }

    const auto& where_literal = static_cast<const parser::relational::LiteralExpression&>(*predicate.right);
    std::int64_t where_value = 0;
    if (!parse_int64(where_literal.text, where_value)) {
        return make_error_metrics(sql,
                                  "WHERE clause literal must be an integer value.");
    }

    auto new_end = std::remove_if(table->rows.begin(), table->rows.end(), [&](const auto& row) {
        if (!std::holds_alternative<std::int64_t>(row[where_index])) {
            return false;
        }
        return std::get<std::int64_t>(row[where_index]) == where_value;
    });
    std::size_t deleted = static_cast<std::size_t>(std::distance(new_end, table->rows.end()));
    table->rows.erase(new_end, table->rows.end());

    CommandMetrics metrics{};
    metrics.success = true;
    metrics.summary = "Deleted " + format_count("row", deleted) + ".";
    metrics.rows_touched = deleted;
    metrics.detail_lines.push_back("planner.root=" + plan_details.root_detail);
    metrics.detail_lines.insert(metrics.detail_lines.end(),
                                plan_details.detail_lines.begin(),
                                plan_details.detail_lines.end());
    metrics.detail_lines.insert(metrics.detail_lines.end(),
                                executor_detail_lines.begin(),
                                executor_detail_lines.end());
    return metrics;
}

CommandMetrics ShellBackend::execute_select(const std::string& sql)
{
    const auto statement = strip_trailing_semicolon(strip_leading_comments(sql));
    if (statement.empty()) {
        return make_error_metrics(sql, "SELECT statement is empty.");
    }

    auto parse_result = parser::parse_select(statement);
    if (!parse_result.success()) {
        return make_parser_error_metrics(sql, std::move(parse_result.diagnostics), "Failed to parse SELECT statement.");
    }

    catalog::CatalogTransactionConfig tx_cfg{&txn_allocator_, &snapshot_manager_};
    catalog::CatalogTransaction transaction{tx_cfg};
    transaction.bind_transaction_context(&txn_manager_, nullptr);

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
        return make_parser_error_metrics(sql,
                                         std::move(binding.diagnostics),
                                         "Failed to bind SELECT statement.");
    }

    std::string plan_error;
    std::vector<std::string> plan_hints;
    auto plan = build_simple_select_plan(*parse_result.statement, plan_error, plan_hints);
    if (!plan.has_value()) {
        return make_error_metrics(sql, std::move(plan_error), std::move(plan_hints));
    }

    const auto* query = parse_result.statement->query;
    if (query == nullptr || query->from == nullptr || !query->from->binding.has_value()) {
        return make_error_metrics(sql, "SELECT target table binding is missing.");
    }
    const auto* table_binding = &*query->from->binding;

    TableData* table = nullptr;
    if (table_binding->schema_name.empty()) {
        table = find_table_or_default_schema(table_binding->table_name);
    } else {
        table = find_table(table_binding->schema_name + "." + table_binding->table_name);
    }
    if (table == nullptr) {
        return make_error_metrics(sql, "Target table not found.");
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
                return make_error_metrics(sql, "Column '" + column_name + "' not found in target table.");
            }
            column_indexes.push_back(it->second);
            selected_columns.push_back(table->columns[it->second].name);
        }
    }

    auto plan_result = plan_select_operation(sql, *table_binding, *table, selected_columns);
    if (std::holds_alternative<CommandMetrics>(plan_result)) {
        return std::get<CommandMetrics>(std::move(plan_result));
    }
    auto plan_details = std::get<PlannerPlanDetails>(std::move(plan_result));

    auto executor_stub = simulate_executor_plan(sql, "SELECT", std::move(plan_details.plan));
    if (std::holds_alternative<CommandMetrics>(executor_stub)) {
        return std::get<CommandMetrics>(std::move(executor_stub));
    }
    auto executor_detail_lines = std::get<std::vector<std::string>>(std::move(executor_stub));

    std::vector<std::string> header_names = selected_columns;

    std::optional<std::size_t> order_index;
    if (plan->order_by_column.has_value()) {
        const auto key = normalize_identifier(*plan->order_by_column);
        auto it = table->column_index.find(key);
        if (it == table->column_index.end()) {
            return make_error_metrics(sql, "ORDER BY column '" + *plan->order_by_column + "' not found.");
        }
        order_index = it->second;
    }

    std::vector<std::size_t> order(table->rows.size());
    for (std::size_t index = 0; index < order.size(); ++index) {
        order[index] = index;
    }

    if (order_index.has_value()) {
        std::stable_sort(order.begin(), order.end(), [&](std::size_t lhs, std::size_t rhs) {
            return compare_values(table->rows[lhs][*order_index], table->rows[rhs][*order_index]) < 0;
        });
    }

    std::vector<std::size_t> widths(header_names.size(), 0U);
    for (std::size_t index = 0; index < header_names.size(); ++index) {
        widths[index] = header_names[index].size();
    }

    std::vector<std::vector<std::string>> rendered_rows;
    rendered_rows.reserve(order.size());
    for (auto row_index : order) {
        const auto& row = table->rows[row_index];
        std::vector<std::string> rendered;
        rendered.reserve(column_indexes.size());
        for (std::size_t index = 0; index < column_indexes.size(); ++index) {
            const auto column_index = column_indexes[index];
            const auto value_text = value_to_string(row[column_index]);
            widths[index] = std::max(widths[index], value_text.size());
            rendered.push_back(value_text);
        }
        rendered_rows.push_back(std::move(rendered));
    }

    CommandMetrics metrics{};
    metrics.success = true;
    metrics.summary = "Selected " + format_count("row", rendered_rows.size()) + ".";
    metrics.rows_touched = rendered_rows.size();

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

    return metrics;
}

}  // namespace bored::shell
