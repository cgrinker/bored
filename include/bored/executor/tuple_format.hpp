#pragma once

#include "bored/executor/tuple_buffer.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace bored::executor {

constexpr std::uint16_t kTupleColumnFlagNull = 0x0001U;

struct alignas(8) TupleRowHeader final {
    std::uint32_t column_count = 0U;
    std::uint32_t reserved = 0U;
};

struct alignas(8) TupleColumnEntry final {
    std::uint32_t offset = 0U;
    std::uint32_t length = 0U;
    std::uint16_t flags = 0U;
    std::uint16_t reserved = 0U;
};

static_assert(sizeof(TupleRowHeader) == 8U, "TupleRowHeader must remain 8 bytes");
static_assert(sizeof(TupleColumnEntry) == 12U || sizeof(TupleColumnEntry) == 16U,
              "TupleColumnEntry unexpected padding");

struct TupleColumnView final {
    std::span<const std::byte> data{};
    bool is_null = true;
};

class TupleWriter final {
public:
    explicit TupleWriter(TupleBuffer& buffer);

    void reset();
    void append_column(std::span<const std::byte> data, bool is_null = false);
    void finalize();

private:
    TupleBuffer& buffer_;
    TupleRowHeader* header_ = nullptr;
    std::vector<TupleColumnEntry> columns_{};
    bool metadata_compacted_ = false;
};

class TupleView final {
public:
    TupleView() = default;

    [[nodiscard]] std::size_t column_count() const noexcept;
    [[nodiscard]] bool valid() const noexcept;
    [[nodiscard]] TupleColumnView column(std::size_t index) const noexcept;

    static TupleView from_buffer(const TupleBuffer& buffer);

private:
    TupleView(const TupleRowHeader* header,
              const TupleColumnEntry* columns,
              std::span<const std::byte> storage) noexcept;

    const TupleRowHeader* header_ = nullptr;
    const TupleColumnEntry* columns_ = nullptr;
    std::span<const std::byte> storage_{};
};

}  // namespace bored::executor
