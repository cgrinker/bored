#pragma once

#include "bored/storage/page_latch.hpp"

#include <cstdint>
#include <system_error>

namespace bored::storage {

class PageLatchGuard final {
public:
    PageLatchGuard(const PageLatchCallbacks* callbacks, std::uint32_t page_id, PageLatchMode mode) noexcept
        : callbacks_{callbacks}
        , page_id_{page_id}
        , mode_{mode}
    {
        if (callbacks_ != nullptr && callbacks_->acquire) {
            status_ = callbacks_->acquire(page_id_, mode_);
            locked_ = !status_;
        }
    }

    PageLatchGuard(const PageLatchGuard&) = delete;
    PageLatchGuard& operator=(const PageLatchGuard&) = delete;

    PageLatchGuard(PageLatchGuard&& other) noexcept
        : callbacks_{other.callbacks_}
        , page_id_{other.page_id_}
        , mode_{other.mode_}
        , locked_{other.locked_}
        , status_{other.status_}
    {
        other.locked_ = false;
    }

    PageLatchGuard& operator=(PageLatchGuard&& other) noexcept
    {
        if (this != &other) {
            release();
            callbacks_ = other.callbacks_;
            page_id_ = other.page_id_;
            mode_ = other.mode_;
            locked_ = other.locked_;
            status_ = other.status_;
            other.locked_ = false;
        }
        return *this;
    }

    ~PageLatchGuard()
    {
        release();
    }

    [[nodiscard]] std::error_code status() const noexcept
    {
        return status_;
    }

    void release() noexcept
    {
        if (locked_ && callbacks_ != nullptr && callbacks_->release) {
            callbacks_->release(page_id_, mode_);
        }
        locked_ = false;
    }

private:
    const PageLatchCallbacks* callbacks_ = nullptr;
    std::uint32_t page_id_ = 0U;
    PageLatchMode mode_ = PageLatchMode::Shared;
    bool locked_ = false;
    std::error_code status_{};
};

}  // namespace bored::storage
