#pragma once

#include "bored/storage/page_operations.hpp"
#include "bored/storage/wal_payloads.hpp"
#include "bored/storage/wal_writer.hpp"

#include <memory>
#include <system_error>

namespace bored::storage {

class PageManager final {
public:
    struct TupleInsertResult final {
        TupleSlot slot{};
        WalAppendResult wal{};
    };

    struct TupleDeleteResult final {
        WalAppendResult wal{};
    };

    PageManager(FreeSpaceMap* fsm, std::shared_ptr<WalWriter> wal_writer);

    PageManager(const PageManager&) = delete;
    PageManager& operator=(const PageManager&) = delete;
    PageManager(PageManager&&) = delete;
    PageManager& operator=(PageManager&&) = delete;

    [[nodiscard]] std::error_code initialize_page(std::span<std::byte> page,
                                                  PageType type,
                                                  std::uint32_t page_id,
                                                  std::uint64_t base_lsn = 0U) const;

    [[nodiscard]] std::error_code insert_tuple(std::span<std::byte> page,
                                               std::span<const std::byte> payload,
                                               std::uint64_t row_id,
                                               TupleInsertResult& out_result) const;

    [[nodiscard]] std::error_code delete_tuple(std::span<std::byte> page,
                                               std::uint16_t slot_index,
                                               std::uint64_t row_id,
                                               TupleDeleteResult& out_result) const;

    [[nodiscard]] std::error_code flush_wal() const;
    [[nodiscard]] std::error_code close_wal() const;

    [[nodiscard]] std::shared_ptr<WalWriter> wal_writer() const noexcept;

private:
    FreeSpaceMap* fsm_ = nullptr;
    std::shared_ptr<WalWriter> wal_writer_{};
};

}  // namespace bored::storage
