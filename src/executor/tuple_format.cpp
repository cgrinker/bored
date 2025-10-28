#include "bored/executor/tuple_format.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>

namespace bored::executor {
namespace {

constexpr std::size_t kMaxColumnLength = std::numeric_limits<std::uint32_t>::max();

[[nodiscard]] std::size_t safe_cast_length(std::size_t length)
{
    if (length > kMaxColumnLength) {
        throw std::length_error{"Tuple column length exceeds 32-bit storage"};
    }
    return length;
}

[[nodiscard]] std::size_t safe_cast_offset(std::size_t offset)
{
    if (offset > kMaxColumnLength) {
        throw std::length_error{"Tuple column offset exceeds 32-bit storage"};
    }
    return offset;
}

}  // namespace

TupleWriter::TupleWriter(TupleBuffer& buffer)
    : buffer_{buffer}
{}

void TupleWriter::reset()
{
    columns_.clear();
    metadata_compacted_ = false;
    buffer_.reset();
    auto header_span = buffer_.allocate(sizeof(TupleRowHeader), alignof(TupleRowHeader));
    header_ = reinterpret_cast<TupleRowHeader*>(header_span.data());
    header_->column_count = 0U;
    header_->reserved = 0U;
}

void TupleWriter::append_column(std::span<const std::byte> data, bool is_null)
{
    if (header_ == nullptr) {
        reset();
    }

    TupleColumnEntry entry{};
    entry.offset = 0U;
    entry.length = 0U;
    entry.flags = is_null ? kTupleColumnFlagNull : 0U;
    entry.reserved = 0U;

    if (!is_null && !data.empty()) {
        const auto payload_span = buffer_.write(data, alignof(std::max_align_t));
        const auto offset = static_cast<std::size_t>(payload_span.data() - buffer_.data());
        entry.offset = static_cast<std::uint32_t>(safe_cast_offset(offset));
        entry.length = static_cast<std::uint32_t>(safe_cast_length(payload_span.size()));
    }

    columns_.push_back(entry);
}

void TupleWriter::finalize()
{
    if (header_ == nullptr || metadata_compacted_) {
        return;
    }
    header_->column_count = static_cast<std::uint32_t>(columns_.size());

    if (columns_.empty()) {
        metadata_compacted_ = true;
        return;
    }

    const std::size_t metadata_bytes = columns_.size() * sizeof(TupleColumnEntry);
    const std::size_t data_offset = sizeof(TupleRowHeader);
    const std::size_t current_size = buffer_.size();
    const std::size_t shift = metadata_bytes;

    buffer_.resize(current_size + shift);
    auto* storage = buffer_.data();

    const std::size_t data_length = current_size - data_offset;
    std::memmove(storage + data_offset + shift, storage + data_offset, data_length);

    auto* metadata_ptr = reinterpret_cast<TupleColumnEntry*>(storage + data_offset);
    for (std::size_t index = 0; index < columns_.size(); ++index) {
        auto entry = columns_[index];
        if ((entry.flags & kTupleColumnFlagNull) == 0U && entry.length > 0U) {
            entry.offset += static_cast<std::uint32_t>(shift);
        }
        metadata_ptr[index] = entry;
        columns_[index] = entry;
    }

    metadata_compacted_ = true;
}

TupleView::TupleView(const TupleRowHeader* header,
                     const TupleColumnEntry* columns,
                     std::span<const std::byte> storage) noexcept
    : header_{header}
    , columns_{columns}
    , storage_{storage}
{}

std::size_t TupleView::column_count() const noexcept
{
    return header_ ? static_cast<std::size_t>(header_->column_count) : 0U;
}

bool TupleView::valid() const noexcept
{
    return header_ != nullptr && columns_ != nullptr;
}

TupleColumnView TupleView::column(std::size_t index) const noexcept
{
    if (!valid() || index >= column_count()) {
        return {};
    }

    const auto& entry = columns_[index];
    const bool is_null = (entry.flags & kTupleColumnFlagNull) != 0U;
    if (is_null) {
        return {{}, true};
    }

    const auto offset = static_cast<std::size_t>(entry.offset);
    const auto length = static_cast<std::size_t>(entry.length);
    if (offset + length > storage_.size()) {
        return {};
    }

    const auto* begin = storage_.data() + offset;
    return {std::span<const std::byte>(begin, length), false};
}

TupleView TupleView::from_buffer(const TupleBuffer& buffer)
{
    auto storage = buffer.span();
    if (storage.size() < sizeof(TupleRowHeader)) {
        return {};
    }

    const auto* header = reinterpret_cast<const TupleRowHeader*>(storage.data());
    const auto column_count = static_cast<std::size_t>(header->column_count);
    const auto metadata_bytes = column_count * sizeof(TupleColumnEntry);
    if (storage.size() < sizeof(TupleRowHeader) + metadata_bytes) {
        return {};
    }

    const auto* columns = reinterpret_cast<const TupleColumnEntry*>(storage.data() + sizeof(TupleRowHeader));
    return TupleView{header, columns, storage};
}

}  // namespace bored::executor
