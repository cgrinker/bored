#include "bored/parser/grammar.hpp"
#include "bored/parser/relational/ast.hpp"

#include <catch2/catch_test_macros.hpp>

#include <string>

namespace relational = bored::parser::relational;

TEST_CASE("parse_select handles basic select", "[parser][select]")
{
    const auto result = bored::parser::parse_select("SELECT name FROM inventory;");
    if (!result.diagnostics.empty()) {
        CAPTURE(result.diagnostics.front().message);
    }
    REQUIRE(result.diagnostics.empty());
    REQUIRE(result.success());
    REQUIRE(result.statement != nullptr);

    const auto* query = result.statement->query;
    REQUIRE(query != nullptr);
    REQUIRE(query->distinct == false);
    REQUIRE(query->select_items.size() == 1U);

    const auto* item = query->select_items.front();
    REQUIRE(item != nullptr);
    REQUIRE(item->expression != nullptr);
    REQUIRE(item->expression->kind == relational::NodeKind::IdentifierExpression);

    const auto& identifier = static_cast<const relational::IdentifierExpression&>(*item->expression);
    REQUIRE(identifier.name.parts.size() == 1U);
    REQUIRE(identifier.name.parts.front().value == "name");

    REQUIRE(query->from != nullptr);
    REQUIRE(query->from->name.parts.size() == 1U);
    REQUIRE(query->from->name.parts.front().value == "inventory");
    REQUIRE_FALSE(query->from->alias.has_value());
    REQUIRE(query->where == nullptr);
    REQUIRE(query->order_by.empty());
    REQUIRE(query->limit == nullptr);
}

TEST_CASE("parse_select handles with clause", "[parser][select]")
{
    const std::string sql =
        "WITH items (item_id) AS (SELECT inventory.id FROM inventory) SELECT items.item_id FROM items;";
    const auto result = bored::parser::parse_select(sql);
    CAPTURE(sql);
    if (!result.diagnostics.empty()) {
        CAPTURE(result.diagnostics.front().message);
    }
    REQUIRE(result.success());
    REQUIRE(result.statement != nullptr);

    const auto* with_clause = result.statement->with;
    REQUIRE(with_clause != nullptr);
    CHECK_FALSE(with_clause->recursive);
    REQUIRE(with_clause->expressions.size() == 1U);

    const auto* cte = with_clause->expressions.front();
    REQUIRE(cte != nullptr);
    CHECK(cte->name.value == "items");
    REQUIRE(cte->column_names.size() == 1U);
    CHECK(cte->column_names.front().value == "item_id");
    REQUIRE(cte->query != nullptr);
    REQUIRE(cte->query->select_items.size() == 1U);
    REQUIRE(cte->query->from != nullptr);
    REQUIRE(cte->query->from->name.parts.size() == 1U);
    CHECK(cte->query->from->name.parts.front().value == "inventory");

    const auto* main_query = result.statement->query;
    REQUIRE(main_query != nullptr);
    REQUIRE(main_query->from != nullptr);
    REQUIRE(main_query->from->name.parts.size() == 1U);
    CHECK(main_query->from->name.parts.front().value == "items");
    REQUIRE(main_query->select_items.size() == 1U);
    const auto* projection = main_query->select_items.front();
    REQUIRE(projection != nullptr);
    REQUIRE(projection->expression != nullptr);
    REQUIRE(projection->expression->kind == relational::NodeKind::IdentifierExpression);
}

TEST_CASE("parse_select supports distinct order and limit", "[parser][select]")
{
    const std::string base_plain = "SELECT DISTINCT * FROM inventory AS inv;";
    const auto base_plain_result = bored::parser::parse_select(base_plain);
    CAPTURE(base_plain);
    if (!base_plain_result.diagnostics.empty()) {
        CAPTURE(base_plain_result.diagnostics.front().message);
    }
    REQUIRE(base_plain_result.success());

    const std::string base_star = "SELECT DISTINCT inv.* FROM inventory AS inv;";
    const auto base_star_result = bored::parser::parse_select(base_star);
    CAPTURE(base_star);
    if (!base_star_result.diagnostics.empty()) {
        CAPTURE(base_star_result.diagnostics.front().message);
    }
    REQUIRE(base_star_result.success());

    const std::string base_schema = "SELECT DISTINCT inv.* FROM sales.inventory AS inv;";
    const auto base_schema_result = bored::parser::parse_select(base_schema);
    CAPTURE(base_schema);
    if (!base_schema_result.diagnostics.empty()) {
        CAPTURE(base_schema_result.diagnostics.front().message);
    }
    REQUIRE(base_schema_result.success());

    const std::string with_where =
        "SELECT DISTINCT inv.* FROM sales.inventory AS inv WHERE inv.id = 42;";
    const auto where_result = bored::parser::parse_select(with_where);
    CAPTURE(with_where);
    if (!where_result.diagnostics.empty()) {
        CAPTURE(where_result.diagnostics.front().message);
    }
    REQUIRE(where_result.success());

    const std::string with_order =
        "SELECT DISTINCT inv.* FROM sales.inventory AS inv ORDER BY inv.created_at DESC, inv.id ASC;";
    const auto order_result = bored::parser::parse_select(with_order);
    CAPTURE(with_order);
    if (!order_result.diagnostics.empty()) {
        CAPTURE(order_result.diagnostics.front().message);
    }
    REQUIRE(order_result.success());

    const std::string with_limit =
        "SELECT DISTINCT inv.* FROM sales.inventory AS inv LIMIT 10 OFFSET 5;";
    const auto limit_result = bored::parser::parse_select(with_limit);
    CAPTURE(with_limit);
    if (!limit_result.diagnostics.empty()) {
        CAPTURE(limit_result.diagnostics.front().message);
    }
    REQUIRE(limit_result.success());

    const std::string sql =
        "SELECT DISTINCT inv.*, inv.id AS item_id FROM sales.inventory AS inv "
        "WHERE inv.id = 42 ORDER BY inv.created_at DESC, inv.id ASC LIMIT 10 OFFSET 5;";

    const auto result = bored::parser::parse_select(sql);
    CAPTURE(sql);
    if (!result.diagnostics.empty()) {
        CAPTURE(result.diagnostics.front().message);
    }
    REQUIRE(result.diagnostics.empty());
    REQUIRE(result.success());
    REQUIRE(result.statement != nullptr);

    const std::string description = bored::parser::relational::describe(*result.statement);
    INFO(description);
    CAPTURE(description);
    const auto* query = result.statement->query;
    REQUIRE(query != nullptr);
    REQUIRE(query->distinct);
    REQUIRE(query->select_items.size() == 2U);

    const auto* first_item = query->select_items.front();
    REQUIRE(first_item != nullptr);
    REQUIRE(first_item->expression != nullptr);
    REQUIRE(first_item->expression->kind == relational::NodeKind::StarExpression);
    const auto& first_star = static_cast<const relational::StarExpression&>(*first_item->expression);
    REQUIRE(first_star.qualifier.parts.size() == 1U);
    REQUIRE(first_star.qualifier.parts.front().value == "inv");

    const auto* second_item = query->select_items.back();
    REQUIRE(second_item != nullptr);
    REQUIRE(second_item->alias.has_value());
    REQUIRE(second_item->alias->value == "item_id");
    REQUIRE(second_item->expression != nullptr);
    REQUIRE(second_item->expression->kind == relational::NodeKind::IdentifierExpression);

    REQUIRE(query->from != nullptr);
    REQUIRE(query->from->name.parts.size() == 2U);
    REQUIRE(query->from->name.parts.front().value == "sales");
    REQUIRE(query->from->name.parts.back().value == "inventory");
    REQUIRE(query->from->alias.has_value());
    REQUIRE(query->from->alias->value == "inv");

    REQUIRE(query->where != nullptr);
    REQUIRE(query->where->kind == relational::NodeKind::BinaryExpression);
    const auto& predicate = static_cast<const relational::BinaryExpression&>(*query->where);
    REQUIRE(predicate.op == relational::BinaryOperator::Equal);
    REQUIRE(predicate.left != nullptr);
    REQUIRE(predicate.left->kind == relational::NodeKind::IdentifierExpression);
    const auto& left_id = static_cast<const relational::IdentifierExpression&>(*predicate.left);
    REQUIRE(left_id.name.parts.size() == 2U);
    REQUIRE(left_id.name.parts.front().value == "inv");
    REQUIRE(left_id.name.parts.back().value == "id");
    REQUIRE(predicate.right != nullptr);
    REQUIRE(predicate.right->kind == relational::NodeKind::LiteralExpression);
    const auto& literal = static_cast<const relational::LiteralExpression&>(*predicate.right);
    REQUIRE(literal.tag == relational::LiteralTag::Integer);
    REQUIRE(literal.text == "42");

    REQUIRE(query->order_by.size() == 2U);
    const auto* first_order = query->order_by.front();
    REQUIRE(first_order != nullptr);
    REQUIRE(first_order->expression != nullptr);
    REQUIRE(first_order->expression->kind == relational::NodeKind::IdentifierExpression);
    REQUIRE(first_order->direction == relational::OrderByItem::Direction::Descending);

    const auto* limit = query->limit;
    REQUIRE(limit != nullptr);
    REQUIRE(limit->row_count != nullptr);
    REQUIRE(limit->row_count->kind == relational::NodeKind::LiteralExpression);
    const auto& limit_literal = static_cast<const relational::LiteralExpression&>(*limit->row_count);
    REQUIRE(limit_literal.tag == relational::LiteralTag::Integer);
    REQUIRE(limit_literal.text == "10");
    REQUIRE(limit->offset != nullptr);
    const auto& offset_literal = static_cast<const relational::LiteralExpression&>(*limit->offset);
    REQUIRE(offset_literal.text == "5");
}

TEST_CASE("parse_select captures inner join metadata", "[parser][select]")
{
    const std::string sql =
        "SELECT o.id, c.name FROM orders AS o INNER JOIN customers AS c ON o.customer_id = c.id;";
    const auto result = bored::parser::parse_select(sql);
    CAPTURE(sql);
    if (!result.diagnostics.empty()) {
        CAPTURE(result.diagnostics.front().message);
    }
    REQUIRE(result.success());
    REQUIRE(result.statement != nullptr);

    const auto* query = result.statement->query;
    REQUIRE(query != nullptr);
    REQUIRE(query->from_tables.size() == 2U);
    REQUIRE(query->joins.size() == 1U);

    const auto* left_table = query->from_tables.front();
    REQUIRE(left_table != nullptr);
    REQUIRE(left_table->alias.has_value());
    REQUIRE(left_table->alias->value == "o");

    const auto* right_table = query->from_tables.back();
    REQUIRE(right_table != nullptr);
    REQUIRE(right_table->alias.has_value());
    REQUIRE(right_table->alias->value == "c");

    const auto& join = query->joins.front();
    REQUIRE(join.type == relational::JoinType::Inner);
    REQUIRE(join.left_kind == relational::JoinClause::InputKind::Table);
    REQUIRE(join.left_index == 0U);
    REQUIRE(join.right_index == 1U);
    REQUIRE(join.predicate != nullptr);
    REQUIRE(join.predicate->kind == relational::NodeKind::BinaryExpression);

    const auto& predicate = static_cast<const relational::BinaryExpression&>(*join.predicate);
    REQUIRE(predicate.left != nullptr);
    REQUIRE(predicate.left->kind == relational::NodeKind::IdentifierExpression);
    REQUIRE(predicate.right != nullptr);
    REQUIRE(predicate.right->kind == relational::NodeKind::IdentifierExpression);
}
