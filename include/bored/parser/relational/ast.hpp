#pragma once

#include "bored/parser/ast.hpp"
#include "bored/catalog/catalog_ids.hpp"
#include "bored/catalog/catalog_relations.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace bored::parser::relational {

class AstArena {
public:
    AstArena() = default;
    AstArena(const AstArena&) = delete;
    AstArena& operator=(const AstArena&) = delete;
    AstArena(AstArena&&) noexcept = default;
    AstArena& operator=(AstArena&&) noexcept = default;
    ~AstArena() noexcept;

    template <typename T, typename... Args>
    T& make(Args&&... args)
    {
        void* storage = allocate(sizeof(T), alignof(T));
        auto* object = std::construct_at(static_cast<T*>(storage), std::forward<Args>(args)...);
        register_destructor(object);
        return *object;
    }

    void reset() noexcept;

private:
    struct Chunk final {
        std::unique_ptr<std::byte[]> data{};
        std::size_t capacity = 0U;
        std::size_t used = 0U;
    };

    struct Destructor final {
        void (*destroy)(void*) noexcept = nullptr;
        void* pointer = nullptr;
    };

    static constexpr std::size_t kDefaultChunkSize = 4096U;

    static std::size_t align_up(std::size_t value, std::size_t alignment) noexcept;
    void* allocate(std::size_t size, std::size_t alignment);
    void add_chunk(std::size_t minimum_capacity);

    template <typename T>
    void register_destructor(T* pointer)
    {
        Destructor entry{};
        entry.pointer = pointer;
        entry.destroy = [](void* storage) noexcept {
            std::destroy_at(static_cast<T*>(storage));
        };
        destructors_.push_back(entry);
    }

    std::vector<Chunk> chunks_{};
    std::vector<Destructor> destructors_{};
};

enum class NodeKind : std::uint8_t {
    SelectStatement = 0,
    InsertStatement,
    UpdateStatement,
    DeleteStatement,
    QuerySpecification,
    SelectItem,
    TableReference,
    IdentifierExpression,
    LiteralExpression,
    BinaryExpression,
    StarExpression,
    OrderByItem,
    LimitClause
};

enum class LiteralTag : std::uint8_t {
    Null = 0,
    Boolean,
    Integer,
    Decimal,
    String
};

enum class BinaryOperator : std::uint8_t {
    Equal = 0,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    Add,
    Subtract
};

enum class ScalarType : std::uint8_t {
    Unknown = 0,
    Boolean,
    Int64,
    UInt32,
    Decimal,
    Utf8
};

enum class JoinType : std::uint8_t {
    Inner = 0,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross
};

struct TypeAnnotation final {
    ScalarType type = ScalarType::Unknown;
    bool nullable = true;
};

struct CoercionRequirement final {
    ScalarType target_type = ScalarType::Unknown;
    bool nullable = true;
};

struct QualifiedName final {
    std::vector<Identifier> parts{};

    [[nodiscard]] bool empty() const noexcept { return parts.empty(); }
};

struct Node {
    explicit Node(NodeKind kind) noexcept : kind(kind) {}
    Node(const Node&) = delete;
    Node& operator=(const Node&) = delete;
    Node(Node&&) = delete;
    Node& operator=(Node&&) = delete;
    virtual ~Node() = default;

    NodeKind kind;
};

struct SelectStatement;
struct InsertStatement;
struct UpdateStatement;
struct DeleteStatement;
struct QuerySpecification;
struct SelectItem;
struct TableReference;
struct IdentifierExpression;
struct LiteralExpression;
struct BinaryExpression;
struct StarExpression;
struct OrderByItem;
struct LimitClause;

struct TableBinding final {
    catalog::DatabaseId database_id{};
    catalog::SchemaId schema_id{};
    catalog::RelationId relation_id{};
    std::string schema_name{};
    std::string table_name{};
    std::optional<std::string> table_alias{};
};

struct ColumnBinding final {
    catalog::DatabaseId database_id{};
    catalog::SchemaId schema_id{};
    catalog::RelationId relation_id{};
    catalog::ColumnId column_id{};
    catalog::CatalogColumnType column_type = catalog::CatalogColumnType::Unknown;
    std::string schema_name{};
    std::string table_name{};
    std::optional<std::string> table_alias{};
    std::string column_name{};
};

class ExpressionVisitor {
public:
    virtual ~ExpressionVisitor() = default;
    virtual void visit(const IdentifierExpression& expression) = 0;
    virtual void visit(const LiteralExpression& expression) = 0;
    virtual void visit(const BinaryExpression& expression) = 0;
    virtual void visit(const StarExpression& expression) = 0;
};

struct Expression : Node {
    explicit Expression(NodeKind kind) noexcept : Node(kind) {}
    ~Expression() override = default;

    void accept(ExpressionVisitor& visitor) const;

    std::optional<TypeAnnotation> inferred_type{};
    std::optional<CoercionRequirement> required_coercion{};
};

struct SelectItem : Node {
    SelectItem() noexcept : Node(NodeKind::SelectItem) {}

    Expression* expression = nullptr;
    std::optional<Identifier> alias{};
};

struct InsertColumn final {
    Identifier name{};
    std::optional<ColumnBinding> binding{};
};

struct InsertRow final {
    std::vector<Expression*> values{};
};

struct TableReference : Node {
    TableReference() noexcept : Node(NodeKind::TableReference) {}

    QualifiedName name{};
    std::optional<Identifier> alias{};
    std::optional<TableBinding> binding{};
};

struct JoinClause final {
    enum class InputKind : std::uint8_t {
        Table = 0,
        Join
    };

    JoinType type = JoinType::Inner;
    InputKind left_kind = InputKind::Table;
    std::size_t left_index = 0U;
    std::size_t right_index = 0U;
    Expression* predicate = nullptr;
};

struct OrderByItem : Node {
    enum class Direction : std::uint8_t {
        Ascending = 0,
        Descending
    };

    OrderByItem() noexcept : Node(NodeKind::OrderByItem) {}

    Expression* expression = nullptr;
    Direction direction = Direction::Ascending;
};

struct LimitClause : Node {
    LimitClause() noexcept : Node(NodeKind::LimitClause) {}

    Expression* row_count = nullptr;
    Expression* offset = nullptr;
};

struct QuerySpecification : Node {
    QuerySpecification() noexcept : Node(NodeKind::QuerySpecification) {}

    bool distinct = false;
    std::vector<SelectItem*> select_items{};
    TableReference* from = nullptr;
    std::vector<TableReference*> from_tables{};
    std::vector<JoinClause> joins{};
    Expression* where = nullptr;
    std::vector<Expression*> group_by{};
    std::vector<OrderByItem*> order_by{};
    LimitClause* limit = nullptr;
};

struct SelectStatement : Node {
    SelectStatement() noexcept : Node(NodeKind::SelectStatement) {}

    QuerySpecification* query = nullptr;
};

struct InsertStatement : Node {
    InsertStatement() noexcept : Node(NodeKind::InsertStatement) {}

    TableReference* target = nullptr;
    std::vector<InsertColumn> columns{};
    std::vector<InsertRow> rows{};
};

struct UpdateAssignment final {
    Identifier column{};
    std::optional<ColumnBinding> binding{};
    Expression* value = nullptr;
};

struct UpdateStatement : Node {
    UpdateStatement() noexcept : Node(NodeKind::UpdateStatement) {}

    TableReference* target = nullptr;
    std::vector<UpdateAssignment> assignments{};
    Expression* where = nullptr;
};

struct DeleteStatement : Node {
    DeleteStatement() noexcept : Node(NodeKind::DeleteStatement) {}

    TableReference* target = nullptr;
    Expression* where = nullptr;
};

struct IdentifierExpression : Expression {
    IdentifierExpression() noexcept : Expression(NodeKind::IdentifierExpression) {}

    QualifiedName name{};
    std::optional<ColumnBinding> binding{};
};

struct LiteralExpression : Expression {
    LiteralExpression() noexcept : Expression(NodeKind::LiteralExpression) {}

    LiteralTag tag = LiteralTag::String;
    bool boolean_value = false;
    std::string text{};
};

struct BinaryExpression : Expression {
    BinaryExpression() noexcept : Expression(NodeKind::BinaryExpression) {}

    BinaryOperator op = BinaryOperator::Equal;
    Expression* left = nullptr;
    Expression* right = nullptr;
};

struct StarExpression : Expression {
    StarExpression() noexcept : Expression(NodeKind::StarExpression) {}

    QualifiedName qualifier{};
    std::optional<TableBinding> binding{};
};

[[nodiscard]] std::string format_qualified_name(const QualifiedName& name);
[[nodiscard]] std::string describe(const SelectStatement& statement);

}  // namespace bored::parser::relational

namespace bored::parser::relational {

inline AstArena::~AstArena() noexcept
{
    reset();
}

inline std::size_t AstArena::align_up(std::size_t value, std::size_t alignment) noexcept
{
    const auto mask = alignment - 1U;
    return (value + mask) & ~mask;
}

inline void AstArena::reset() noexcept
{
    for (auto it = destructors_.rbegin(); it != destructors_.rend(); ++it) {
        if (it->destroy && it->pointer) {
            it->destroy(it->pointer);
        }
    }
    destructors_.clear();
    for (auto& chunk : chunks_) {
        chunk.used = 0U;
    }
}

inline void* AstArena::allocate(std::size_t size, std::size_t alignment)
{
    if (alignment == 0U) {
        alignment = alignof(std::max_align_t);
    }

    const auto adjusted_size = align_up(size, alignment);

    while (chunks_.empty() || chunks_.back().used + adjusted_size > chunks_.back().capacity) {
        add_chunk(std::max(kDefaultChunkSize, adjusted_size));
    }

    auto& chunk = chunks_.back();
    const auto offset = align_up(chunk.used, alignment);
    chunk.used = offset + adjusted_size;
    return chunk.data.get() + offset;
}

inline void AstArena::add_chunk(std::size_t minimum_capacity)
{
    Chunk chunk{};
    chunk.capacity = align_up(minimum_capacity, alignof(std::max_align_t));
    chunk.data = std::unique_ptr<std::byte[]>(new std::byte[chunk.capacity]);
    chunk.used = 0U;
    chunks_.push_back(std::move(chunk));
}

inline void Expression::accept(ExpressionVisitor& visitor) const
{
    switch (kind) {
    case NodeKind::IdentifierExpression:
        visitor.visit(static_cast<const IdentifierExpression&>(*this));
        break;
    case NodeKind::LiteralExpression:
        visitor.visit(static_cast<const LiteralExpression&>(*this));
        break;
    case NodeKind::BinaryExpression:
        visitor.visit(static_cast<const BinaryExpression&>(*this));
        break;
    case NodeKind::StarExpression:
        visitor.visit(static_cast<const StarExpression&>(*this));
        break;
    default:
        break;
    }
}

}  // namespace bored::parser::relational
