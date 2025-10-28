#include "bored/executor/tuple_buffer.hpp"

#include <algorithm>
#include <stdexcept>

namespace bored::executor {

TupleBuffer::TupleBuffer()
    : TupleBuffer(4096U)
{
}

TupleBuffer::TupleBuffer(std::size_t initial_capacity)
{
    if (initial_capacity == 0U) {
        initial_capacity = 4096U;
    }
    storage_.resize(initial_capacity);
}

std::size_t TupleBuffer::capacity() const noexcept
{
    return storage_.size();
}

std::size_t TupleBuffer::size() const noexcept
{
    return write_offset_;
}

std::span<const std::byte> TupleBuffer::span() const noexcept
{
    return {storage_.data(), write_offset_};
}

const std::byte* TupleBuffer::data() const noexcept
{
    return storage_.data();
}

std::byte* TupleBuffer::data() noexcept
{
    return storage_.data();
}

std::span<std::byte> TupleBuffer::allocate(std::size_t size, std::size_t alignment)
{
    if (size == 0U) {
        return std::span<std::byte>{};
    }
    if (alignment == 0U) {
        alignment = 1U;
    }

    const std::size_t aligned_offset = align_offset(write_offset_, alignment);
    const std::size_t end_offset = aligned_offset + size;
    ensure_capacity(end_offset);

    auto span = std::span<std::byte>(storage_.data() + aligned_offset, size);
    write_offset_ = end_offset;
    return span;
}

std::span<std::byte> TupleBuffer::write(std::span<const std::byte> data, std::size_t alignment)
{
    auto destination = allocate(data.size(), alignment);
    std::copy(data.begin(), data.end(), destination.begin());
    return destination;
}

void TupleBuffer::resize(std::size_t new_size)
{
    ensure_capacity(new_size);
    write_offset_ = new_size;
}

void TupleBuffer::reset() noexcept
{
    write_offset_ = 0U;
}

void TupleBuffer::ensure_capacity(std::size_t required_capacity)
{
    if (required_capacity <= storage_.size()) {
        return;
    }

    std::size_t new_capacity = storage_.size();
    while (new_capacity < required_capacity) {
        new_capacity *= 2U;
    }
    storage_.resize(new_capacity);
}

std::size_t TupleBuffer::align_offset(std::size_t offset, std::size_t alignment) noexcept
{
    const std::size_t mask = alignment - 1U;
    if ((alignment & mask) == 0U) {
        return (offset + mask) & ~mask;
    }

    const std::size_t remainder = offset % alignment;
    if (remainder == 0U) {
        return offset;
    }
    return offset + (alignment - remainder);
}

}  // namespace bored::executor
