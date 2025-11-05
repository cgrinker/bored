#include "bored/parser/relational/logical_lowering.hpp"

#include <cctype>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace bored::parser::relational {
namespace {

std::string projection_label(const SelectItem& item, std::size_t index)
{
    if (item.alias.has_value()) {
        return item.alias->value;
    }

    if (item.expression != nullptr && item.expression->kind == NodeKind::IdentifierExpression) {
        const auto& identifier = static_cast<const IdentifierExpression&>(*item.expression);
        return format_qualified_name(identifier.name);
    }

    std::ostringstream stream;
    stream << "column_" << (index + 1U);
    return stream.str();
}

std::string normalise_identifier(std::string_view text)
{
    std::string result;
    result.reserve(text.size());
    for (char ch : text) {
        result.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
    }
    return result;
}

std::vector<LogicalColumn> propagate_schema(const LogicalOperatorPtr& input)
{
    if (input == nullptr) {
        return {};
    }
    return input->output_schema;
}

std::vector<LogicalColumn> merge_schemas(const LogicalOperatorPtr& left, const LogicalOperatorPtr& right)
{
    auto schema = propagate_schema(left);
    const auto right_schema = propagate_schema(right);
    schema.insert(schema.end(), right_schema.begin(), right_schema.end());
    return schema;
}

LogicalOperatorPtr clone_plan(const LogicalOperator& node);

LogicalOperatorPtr clone_optional(const LogicalOperatorPtr& node)
{
    if (node == nullptr) {
        return nullptr;
    }
    return clone_plan(*node);
}

std::vector<LogicalColumn> derive_cte_schema(const CommonTableExpression& cte)
{
    std::vector<LogicalColumn> schema;
    if (cte.query == nullptr) {
        return schema;
    }

    const auto& anchor_items = cte.query->select_items;
    if (anchor_items.empty()) {
        return schema;
    }

    schema.reserve(anchor_items.size());
    for (std::size_t index = 0U; index < anchor_items.size(); ++index) {
        const auto* item = anchor_items[index];
        if (item == nullptr) {
            schema.emplace_back();
            continue;
        }

        LogicalColumn column{};
        if (!cte.column_names.empty() && index < cte.column_names.size()) {
            column.name = cte.column_names[index].value;
        } else {
            column.name = projection_label(*item, index);
        }

        if (item->expression != nullptr && item->expression->inferred_type.has_value()) {
            column.type = item->expression->inferred_type->type;
            column.nullable = item->expression->inferred_type->nullable;
        }

        if (cte.recursive_query != nullptr) {
            const auto& recursive_items = cte.recursive_query->select_items;
            if (index < recursive_items.size()) {
                const auto* recursive_item = recursive_items[index];
                if (recursive_item != nullptr &&
                    recursive_item->expression != nullptr &&
                    recursive_item->expression->inferred_type.has_value()) {
                    const auto& info = *recursive_item->expression->inferred_type;
                    if (column.type == ScalarType::Unknown && info.type != ScalarType::Unknown) {
                        column.type = info.type;
                    }
                    column.nullable = column.nullable || info.nullable;
                }
            }
        }

        schema.push_back(std::move(column));
    }

    return schema;
}

LogicalOperatorPtr clone_plan(const LogicalOperator& node)
{
    switch (node.kind) {
    case LogicalOperatorKind::Scan: {
        const auto& scan = static_cast<const LogicalScan&>(node);
        auto copy = std::make_unique<LogicalScan>();
        copy->table = scan.table;
        copy->columns = scan.columns;
        copy->output_schema = scan.output_schema;
        return copy;
    }
    case LogicalOperatorKind::Project: {
        const auto& project = static_cast<const LogicalProject&>(node);
        auto copy = std::make_unique<LogicalProject>();
        copy->projections = project.projections;
        copy->output_schema = project.output_schema;
        copy->input = clone_optional(project.input);
        return copy;
    }
    case LogicalOperatorKind::Filter: {
        const auto& filter = static_cast<const LogicalFilter&>(node);
        auto copy = std::make_unique<LogicalFilter>();
        copy->predicate = filter.predicate;
        copy->output_schema = filter.output_schema;
        copy->input = clone_optional(filter.input);
        return copy;
    }
    case LogicalOperatorKind::Join: {
        const auto& join = static_cast<const LogicalJoin&>(node);
        auto copy = std::make_unique<LogicalJoin>();
        copy->join_type = join.join_type;
        copy->predicate = join.predicate;
        copy->output_schema = join.output_schema;
        copy->left = clone_optional(join.left);
        copy->right = clone_optional(join.right);
        return copy;
    }
    case LogicalOperatorKind::Aggregate: {
        const auto& aggregate = static_cast<const LogicalAggregate&>(node);
        auto copy = std::make_unique<LogicalAggregate>();
        copy->group_keys = aggregate.group_keys;
        copy->aggregates = aggregate.aggregates;
        copy->output_schema = aggregate.output_schema;
        copy->input = clone_optional(aggregate.input);
        return copy;
    }
    case LogicalOperatorKind::Sort: {
        const auto& sort = static_cast<const LogicalSort&>(node);
        auto copy = std::make_unique<LogicalSort>();
        copy->keys = sort.keys;
        copy->output_schema = sort.output_schema;
        copy->input = clone_optional(sort.input);
        return copy;
    }
    case LogicalOperatorKind::Limit: {
        const auto& limit = static_cast<const LogicalLimit&>(node);
        auto copy = std::make_unique<LogicalLimit>();
        copy->row_count = limit.row_count;
        copy->offset = limit.offset;
        copy->output_schema = limit.output_schema;
        copy->input = clone_optional(limit.input);
        return copy;
    }
    case LogicalOperatorKind::CteScan: {
        const auto& scan = static_cast<const LogicalCteScan&>(node);
        auto copy = std::make_unique<LogicalCteScan>();
        copy->cte_name = scan.cte_name;
        copy->table_alias = scan.table_alias;
        copy->output_schema = scan.output_schema;
        return copy;
    }
    case LogicalOperatorKind::RecursiveCte: {
        const auto& recursive = static_cast<const LogicalRecursiveCte&>(node);
        auto copy = std::make_unique<LogicalRecursiveCte>();
        copy->cte_name = recursive.cte_name;
        copy->output_schema = recursive.output_schema;
        copy->anchor = clone_optional(recursive.anchor);
        copy->recursive = clone_optional(recursive.recursive);
        return copy;
    }
    default:
        break;
    }

    return nullptr;
}

class LoweringContext final {
public:
    LoweringContext(const SelectStatement& statement, const LoweringConfig& config)
        : statement_(statement)
        , config_(config)
    {
        if (statement_.with != nullptr) {
            for (auto* cte : statement_.with->expressions) {
                if (cte == nullptr) {
                    continue;
                }
                const auto key = normalise_identifier(cte->name.value);
                if (!key.empty()) {
                    cte_definitions_.emplace(key, cte);
                }
            }
        }
    }

    LoweringResult run()
    {
        LoweringResult result{};
        if (statement_.query == nullptr) {
            return result;
        }

        if (statement_.query->select_items.empty()) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "SELECT lowering requires at least one projection";
            diagnostic.remediation_hints = {"Add at least one item to the SELECT list."};
            result.diagnostics.push_back(std::move(diagnostic));
            return result;
        }

        auto plan = lower_query(*statement_.query, result);
        if (plan == nullptr) {
            return result;
        }

        result.plan = std::move(plan);
        if (config_.plan_sink && result.plan != nullptr) {
            config_.plan_sink(*result.plan);
        }
        return result;
    }

private:
    struct CtePlanInfo final {
        std::vector<LogicalColumn> schema{};
        LogicalOperatorPtr blueprint{};
        bool building_anchor = false;
        bool building_recursive = false;
    };

    LogicalOperatorPtr lower_query(const QuerySpecification& query, LoweringResult& result)
    {
        auto current = lower_from(query, result);
        if (current == nullptr) {
            return nullptr;
        }

        if (query.where != nullptr) {
            auto filter = std::make_unique<LogicalFilter>();
            filter->predicate = query.where;
            filter->output_schema = propagate_schema(current);
            filter->input = std::move(current);
            current = std::move(filter);
        }

        if (!query.group_by.empty()) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "GROUP BY lowering is not implemented";
            diagnostic.remediation_hints = {"Remove the GROUP BY clause or extend the lowering pass."};
            result.diagnostics.push_back(std::move(diagnostic));
            return nullptr;
        }

        auto project = lower_projection(query, std::move(current));
        if (project == nullptr) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "SELECT lowering failed to build projection";
            result.diagnostics.push_back(std::move(diagnostic));
            return nullptr;
        }
        current = std::move(project);

        if (!query.order_by.empty()) {
            auto sort = std::make_unique<LogicalSort>();
            sort->output_schema = propagate_schema(current);
            for (const auto* item : query.order_by) {
                if (item == nullptr || item->expression == nullptr) {
                    continue;
                }
                LogicalSort::SortKey key{};
                key.expression = item->expression;
                key.direction = item->direction;
                sort->keys.push_back(key);
            }
            sort->input = std::move(current);
            current = std::move(sort);
        }

        if (query.limit != nullptr && (query.limit->row_count != nullptr || query.limit->offset != nullptr)) {
            auto limit = std::make_unique<LogicalLimit>();
            limit->output_schema = propagate_schema(current);
            if (query.limit->row_count != nullptr) {
                limit->row_count = query.limit->row_count;
            }
            if (query.limit->offset != nullptr) {
                limit->offset = query.limit->offset;
            }
            limit->input = std::move(current);
            current = std::move(limit);
        }

        return current;
    }

    LogicalOperatorPtr lower_from(const QuerySpecification& query, LoweringResult& result)
    {
        if (query.from_tables.empty()) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "SELECT lowering requires a FROM clause";
            diagnostic.remediation_hints = {"Add a table to the FROM clause."};
            result.diagnostics.push_back(std::move(diagnostic));
            return nullptr;
        }

        std::vector<LogicalOperatorPtr> table_nodes(query.from_tables.size());
        for (std::size_t index = 0; index < query.from_tables.size(); ++index) {
            auto* table = query.from_tables[index];
            if (table == nullptr) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "FROM clause contains an invalid table reference";
                diagnostic.remediation_hints = {"Ensure each table reference in the FROM clause is well-formed."};
                result.diagnostics.push_back(std::move(diagnostic));
                return nullptr;
            }
            if (!table->binding.has_value()) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "Table binding missing for SELECT lowering";
                diagnostic.remediation_hints = {"Ensure the binder has resolved table references before lowering."};
                result.diagnostics.push_back(std::move(diagnostic));
                return nullptr;
            }

            LogicalOperatorPtr node;
            const auto table_key = normalise_identifier(table->binding->table_name);
            if (cte_definitions_.find(table_key) != cte_definitions_.end()) {
                node = lower_cte_reference(*table, result);
            } else {
                auto scan = std::make_unique<LogicalScan>();
                scan->table = *table->binding;
                node = std::move(scan);
            }
            if (node == nullptr) {
                return nullptr;
            }
            table_nodes[index] = std::move(node);
        }

        const auto make_reference_error = [&](std::string message) -> LogicalOperatorPtr {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = std::move(message);
            diagnostic.remediation_hints = {"Verify JOIN clauses reference tables that appear earlier in the FROM clause."};
            result.diagnostics.push_back(std::move(diagnostic));
            return nullptr;
        };

        auto take_table = [&](std::size_t index) -> LogicalOperatorPtr {
            if (index >= table_nodes.size()) {
                std::ostringstream stream;
                stream << "JOIN references table index " << index << " which is out of range";
                return make_reference_error(stream.str());
            }
            auto& entry = table_nodes[index];
            if (entry == nullptr) {
                std::ostringstream stream;
                stream << "JOIN references table index " << index << " that is already consumed";
                return make_reference_error(stream.str());
            }
            return std::move(entry);
        };

        std::vector<LogicalOperatorPtr> join_nodes(query.joins.size());

        auto take_join = [&](std::size_t index) -> LogicalOperatorPtr {
            if (index >= join_nodes.size()) {
                std::ostringstream stream;
                stream << "JOIN references join index " << index << " which is out of range";
                return make_reference_error(stream.str());
            }
            auto& entry = join_nodes[index];
            if (entry == nullptr) {
                std::ostringstream stream;
                stream << "JOIN references join index " << index << " that is not yet available";
                return make_reference_error(stream.str());
            }
            return std::move(entry);
        };

        for (std::size_t join_index = 0; join_index < query.joins.size(); ++join_index) {
            const auto& clause = query.joins[join_index];

            LogicalOperatorPtr left_input;
            if (clause.left_kind == JoinClause::InputKind::Join) {
                left_input = take_join(clause.left_index);
            } else {
                left_input = take_table(clause.left_index);
            }
            if (left_input == nullptr) {
                return nullptr;
            }

            auto right_input = take_table(clause.right_index);
            if (right_input == nullptr) {
                return nullptr;
            }

            auto join = std::make_unique<LogicalJoin>();
            join->join_type = clause.type;
            join->predicate = clause.predicate;
            join->left = std::move(left_input);
            join->right = std::move(right_input);
            join->output_schema = merge_schemas(join->left, join->right);

            join_nodes[join_index] = std::move(join);
        }

        LogicalOperatorPtr current;
        if (!join_nodes.empty()) {
            current = std::move(join_nodes.back());
            if (current == nullptr) {
                return make_reference_error("JOIN tree missing final join result");
            }
        }

        for (auto& table_node : table_nodes) {
            if (table_node == nullptr) {
                continue;
            }
            if (current == nullptr) {
                current = std::move(table_node);
                continue;
            }

            auto join = std::make_unique<LogicalJoin>();
            join->join_type = JoinType::Cross;
            join->predicate = nullptr;
            join->left = std::move(current);
            join->right = std::move(table_node);
            join->output_schema = merge_schemas(join->left, join->right);
            current = std::move(join);
        }

        if (current == nullptr) {
            return make_reference_error("SELECT lowering requires at least one resolved table");
        }

        return current;
    }

    std::unique_ptr<LogicalProject> lower_projection(const QuerySpecification& query,
                                                     LogicalOperatorPtr input)
    {
        auto project = std::make_unique<LogicalProject>();
        project->input = std::move(input);

        std::size_t index = 0U;
        for (const auto* item : query.select_items) {
            if (item == nullptr || item->expression == nullptr) {
                ++index;
                continue;
            }

            LogicalProject::Projection projection{};
            projection.expression = item->expression;
            if (item->alias.has_value()) {
                projection.alias = item->alias->value;
            }
            project->projections.push_back(projection);

            LogicalColumn column{};
            column.name = projection_label(*item, index);
            if (item->expression->inferred_type.has_value()) {
                column.type = item->expression->inferred_type->type;
                column.nullable = item->expression->inferred_type->nullable;
            }
            project->output_schema.push_back(column);
            ++index;
        }

        return project;
    }

    const SelectStatement& statement_;
    const LoweringConfig& config_;
    std::unordered_map<std::string, const CommonTableExpression*> cte_definitions_{};
    std::unordered_set<std::string> active_ctes_{};
    std::unordered_map<std::string, CtePlanInfo> cte_plans_{};

    LogicalOperatorPtr lower_cte_reference(const TableReference& table, LoweringResult& result)
    {
        if (!table.binding.has_value()) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "CTE reference is missing binding information";
            diagnostic.remediation_hints = {
                "Ensure the binder resolves WITH clause references before lowering."};
            result.diagnostics.push_back(std::move(diagnostic));
            return nullptr;
        }

        const auto& binding = *table.binding;
        const auto key = normalise_identifier(binding.table_name);
        auto it = cte_definitions_.find(key);
        if (it == cte_definitions_.end()) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "CTE '" + binding.table_name + "' is not available during lowering";
            diagnostic.remediation_hints = {
                "Verify the WITH clause defines the referenced CTE before it is used."};
            result.diagnostics.push_back(std::move(diagnostic));
            return nullptr;
        }

        if (active_ctes_.count(key) != 0U) {
            auto it_state = cte_plans_.find(key);
            if (it_state != cte_plans_.end() && it_state->second.building_recursive) {
                auto scan = std::make_unique<LogicalCteScan>();
                scan->cte_name = binding.table_name;
                if (binding.table_alias.has_value()) {
                    scan->table_alias = binding.table_alias;
                }
                if (!it_state->second.schema.empty()) {
                    scan->output_schema = it_state->second.schema;
                }
                return scan;
            }

            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "Recursive CTE reference '" + binding.table_name + "' is not supported";
            diagnostic.remediation_hints = {
                "Rewrite the query to avoid referencing a CTE within its own definition."};
            result.diagnostics.push_back(std::move(diagnostic));
            return nullptr;
        }

        const auto* cte = it->second;
        if (cte == nullptr || cte->query == nullptr) {
            ParserDiagnostic diagnostic{};
            diagnostic.severity = ParserSeverity::Error;
            diagnostic.message = "CTE '" + binding.table_name + "' has an empty query body";
            diagnostic.remediation_hints = {
                "Provide a SELECT statement inside the CTE definition."};
            result.diagnostics.push_back(std::move(diagnostic));
            return nullptr;
        }

        auto& state = cte_plans_[key];

        const bool is_recursive = (cte->recursive_query != nullptr);

        if (is_recursive) {
            if (state.blueprint != nullptr) {
                auto clone = clone_plan(*state.blueprint);
                if (clone != nullptr) {
                    return clone;
                }
            }

            if (state.schema.empty()) {
                state.schema = derive_cte_schema(*cte);
            }

            const auto inserted = active_ctes_.insert(key).second;
            if (!inserted && !state.building_recursive) {
                ParserDiagnostic diagnostic{};
                diagnostic.severity = ParserSeverity::Error;
                diagnostic.message = "Recursive CTE '" + binding.table_name + "' has circular references";
                diagnostic.remediation_hints = {
                    "Ensure the recursive member only references the CTE from within its own definition."};
                result.diagnostics.push_back(std::move(diagnostic));
                return nullptr;
            }

            state.building_anchor = true;
            auto anchor_plan = lower_query(*cte->query, result);
            state.building_anchor = false;
            if (anchor_plan == nullptr) {
                active_ctes_.erase(key);
                return nullptr;
            }

            state.building_recursive = true;
            auto recursive_plan = lower_query(*cte->recursive_query, result);
            state.building_recursive = false;
            active_ctes_.erase(key);
            if (recursive_plan == nullptr) {
                return nullptr;
            }

            if (state.schema.empty()) {
                state.schema = propagate_schema(anchor_plan);
            }
            if (state.schema.empty()) {
                state.schema = propagate_schema(recursive_plan);
            }

            auto recursive_cte = std::make_unique<LogicalRecursiveCte>();
            recursive_cte->cte_name = binding.table_name;
            recursive_cte->output_schema = state.schema;
            recursive_cte->anchor = std::move(anchor_plan);
            recursive_cte->recursive = std::move(recursive_plan);

            state.blueprint = clone_plan(*recursive_cte);
            return recursive_cte;
        }

        if (state.blueprint != nullptr) {
            auto clone = clone_plan(*state.blueprint);
            if (clone != nullptr) {
                return clone;
            }
        }

        active_ctes_.insert(key);
        auto plan = lower_query(*cte->query, result);
        active_ctes_.erase(key);
        if (plan != nullptr) {
            state.blueprint = clone_plan(*plan);
        }
        return plan;
    }
};

}  // namespace

LoweringResult lower_select(const SelectStatement& statement, const LoweringConfig& config)
{
    LoweringContext context(statement, config);
    return context.run();
}

LoweringResult lower_select(const SelectStatement& statement)
{
    LoweringConfig config{};
    return lower_select(statement, config);
}

}  // namespace bored::parser::relational
