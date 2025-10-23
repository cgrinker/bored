#pragma once

#include <cstdint>
#include <functional>
#include <system_error>

namespace bored::storage {

enum class PageLatchMode {
    Shared,
    Exclusive
};

struct PageLatchCallbacks final {
    std::function<std::error_code(std::uint32_t, PageLatchMode)> acquire{};
    std::function<void(std::uint32_t, PageLatchMode)> release{};
};

}  // namespace bored::storage
