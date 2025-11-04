#include "bored/parser/relational/ast.hpp"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>

namespace relational = bored::parser::relational;

namespace {

struct CollectingVisitor final : relational::ExpressionVisitor {
    void visit(const relational::IdentifierExpression& expression) override
    {
    last = relational::format_qualified_name(expression.name);
    visits.emplace_back(std::string{"identifier:"} + last);
    }

    void visit(const relational::LiteralExpression& expression) override
    {
        visits.emplace_back("literal:" + expression.text);
        last = expression.text;
    }

    void visit(const relational::BinaryExpression& expression) override
    {
        visits.emplace_back("binary");
        if (expression.left) {
            expression.left->accept(*this);
        }
        if (expression.right) {
            expression.right->accept(*this);
        }
    }

    void visit(const relational::StarExpression&) override
    {
        visits.emplace_back("star");
        last = "*";
    }

    std::string last{};
    std::vector<std::string> visits{};
};

}  // namespace

TEST_CASE("AstArena reuses storage between resets", "[parser][relational_ast]")
{
    relational::AstArena arena;

    auto& first_stmt = arena.make<relational::SelectStatement>();
    first_stmt.query = &arena.make<relational::QuerySpecification>();
    first_stmt.query->select_items.push_back(&arena.make<relational::SelectItem>());

    const auto first_address = first_stmt.query;
    arena.reset();

    auto& second_stmt = arena.make<relational::SelectStatement>();
    second_stmt.query = &arena.make<relational::QuerySpecification>();

    REQUIRE(second_stmt.query != nullptr);
    REQUIRE(second_stmt.query == first_address);
}

TEST_CASE("describe renders select statement", "[parser][relational_ast]")
{
    relational::AstArena arena;

    auto& select_stmt = arena.make<relational::SelectStatement>();
    auto& query = arena.make<relational::QuerySpecification>();
    select_stmt.query = &query;
    query.distinct = true;

    auto& star_expr = arena.make<relational::StarExpression>();
    auto& projection = arena.make<relational::SelectItem>();
    projection.expression = &star_expr;
    query.select_items.push_back(&projection);

    auto& table_ref = arena.make<relational::TableReference>();
    table_ref.name.parts.push_back(bored::parser::Identifier{.value = "inventory"});
    query.from = &table_ref;
    query.from_tables.push_back(&table_ref);

    auto& predicate = arena.make<relational::BinaryExpression>();
    predicate.op = relational::BinaryOperator::Equal;
    auto& left_identifier = arena.make<relational::IdentifierExpression>();
    left_identifier.name.parts.push_back(bored::parser::Identifier{.value = "inventory"});
    left_identifier.name.parts.push_back(bored::parser::Identifier{.value = "id"});
    predicate.left = &left_identifier;

    auto& right_literal = arena.make<relational::LiteralExpression>();
    right_literal.tag = relational::LiteralTag::Integer;
    right_literal.text = "42";
    predicate.right = &right_literal;
    query.where = &predicate;

    auto& group_expr = arena.make<relational::IdentifierExpression>();
    group_expr.name.parts.push_back(bored::parser::Identifier{.value = "inventory"});
    group_expr.name.parts.push_back(bored::parser::Identifier{.value = "category"});
    query.group_by.push_back(&group_expr);

    auto& order_item = arena.make<relational::OrderByItem>();
    auto& order_expr = arena.make<relational::IdentifierExpression>();
    order_expr.name.parts.push_back(bored::parser::Identifier{.value = "inventory"});
    order_expr.name.parts.push_back(bored::parser::Identifier{.value = "created_at"});
    order_item.expression = &order_expr;
    order_item.direction = relational::OrderByItem::Direction::Descending;
    query.order_by.push_back(&order_item);

    auto& limit = arena.make<relational::LimitClause>();
    auto& limit_count = arena.make<relational::LiteralExpression>();
    limit_count.tag = relational::LiteralTag::Integer;
    limit_count.text = "10";
    limit.row_count = &limit_count;

    auto& limit_offset = arena.make<relational::LiteralExpression>();
    limit_offset.tag = relational::LiteralTag::Integer;
    limit_offset.text = "5";
    limit.offset = &limit_offset;
    query.limit = &limit;

    const auto rendered = relational::describe(select_stmt);
    REQUIRE(rendered == "SELECT DISTINCT * FROM inventory WHERE (inventory.id = 42) GROUP BY inventory.category ORDER BY inventory.created_at DESC LIMIT 10 OFFSET 5");
}

TEST_CASE("describe renders with clause", "[parser][relational_ast]")
{
    relational::AstArena arena;

    auto& statement = arena.make<relational::SelectStatement>();
    auto& with_clause = arena.make<relational::WithClause>();
    statement.with = &with_clause;

    auto& cte = arena.make<relational::CommonTableExpression>();
    cte.name = bored::parser::Identifier{.value = "items"};
    cte.column_names.push_back(bored::parser::Identifier{.value = "item_id"});

    auto& cte_query = arena.make<relational::QuerySpecification>();
    auto& cte_star = arena.make<relational::StarExpression>();
    auto& cte_item = arena.make<relational::SelectItem>();
    cte_item.expression = &cte_star;
    cte_query.select_items.push_back(&cte_item);
    auto& cte_table = arena.make<relational::TableReference>();
    cte_table.name.parts.push_back(bored::parser::Identifier{.value = "inventory"});
    cte_query.from = &cte_table;
    cte_query.from_tables.push_back(&cte_table);
    cte.query = &cte_query;
    with_clause.expressions.push_back(&cte);

    auto& main_query = arena.make<relational::QuerySpecification>();
    auto& projection = arena.make<relational::SelectItem>();
    auto& identifier = arena.make<relational::IdentifierExpression>();
    identifier.name.parts.push_back(bored::parser::Identifier{.value = "items"});
    identifier.name.parts.push_back(bored::parser::Identifier{.value = "item_id"});
    projection.expression = &identifier;
    main_query.select_items.push_back(&projection);
    auto& main_table = arena.make<relational::TableReference>();
    main_table.name.parts.push_back(bored::parser::Identifier{.value = "items"});
    main_query.from = &main_table;
    main_query.from_tables.push_back(&main_table);

    statement.query = &main_query;

    const auto rendered = relational::describe(statement);
    REQUIRE(rendered == "WITH items(item_id) AS (SELECT * FROM inventory) SELECT items.item_id FROM items");
}

TEST_CASE("expression visitor traverses tree", "[parser][relational_ast]")
{
    relational::AstArena arena;

    auto& binary = arena.make<relational::BinaryExpression>();
    binary.op = relational::BinaryOperator::Equal;
    auto& left = arena.make<relational::IdentifierExpression>();
    left.name.parts.push_back(bored::parser::Identifier{.value = "orders"});
    left.name.parts.push_back(bored::parser::Identifier{.value = "status"});
    binary.left = &left;

    auto& right = arena.make<relational::LiteralExpression>();
    right.tag = relational::LiteralTag::String;
    right.text = "shipped";
    binary.right = &right;

    CollectingVisitor visitor;
    binary.accept(visitor);

    REQUIRE(visitor.last == "shipped");
    REQUIRE(visitor.visits.size() == 3U);
    REQUIRE(visitor.visits.front() == "binary");
}
