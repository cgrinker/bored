#pragma once

#include <cstddef>
#include <span>
#include <vector>

namespace bored::executor {

class TupleBuffer final {
public:
    TupleBuffer();
    explicit TupleBuffer(std::size_t initial_capacity);

    [[nodiscard]] std::size_t capacity() const noexcept;
    [[nodiscard]] std::size_t size() const noexcept;
    [[nodiscard]] std::span<const std::byte> span() const noexcept;
    [[nodiscard]] const std::byte* data() const noexcept;
    [[nodiscard]] std::byte* data() noexcept;

    std::span<std::byte> allocate(std::size_t size, std::size_t alignment = alignof(std::max_align_t));
    std::span<std::byte> write(std::span<const std::byte> data, std::size_t alignment = alignof(std::max_align_t));
    void resize(std::size_t new_size);

    void reset() noexcept;

private:
    void ensure_capacity(std::size_t required_capacity);
    static std::size_t align_offset(std::size_t offset, std::size_t alignment) noexcept;

    std::vector<std::byte> storage_{};
    std::size_t write_offset_ = 0U;
};

}  // namespace bored::executor
