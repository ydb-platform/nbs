#pragma once

#include "page_store.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/buffer.h>
#include <util/generic/stack.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

class TPersistentBitmap
{
private:
    const ui64 FirstPageNo;
    const ui64 MaxBits;
    const ui64 PageSize;
    const ui64 BitsPerPage;
    IPageStorePtr PageStore;

    mutable TVector<TBuffer> BitmapPages;
    mutable TStack<ui64> BitmapPagesWithFreeBits;
    mutable ui64 UnusedBits = 0;

public:
    TPersistentBitmap(
        ui64 firstPageNo,
        ui64 maxBits,
        ui64 pageSize,
        IPageStorePtr pageStore)
        : FirstPageNo(firstPageNo)
        , MaxBits(maxBits)
        , PageSize(pageSize)
        , BitsPerPage(CalcBitsPerPage(pageSize))
        , PageStore(std::move(pageStore))
    {}

public:
    NProto::TError Get(ui64 bit, bool* result) const;
    NProto::TError Set(ui64 lsn, ui64 bit, TVector<TPageGroup>& pageGroups);
    NProto::TError Reset(ui64 lsn, ui64 bit, TVector<TPageGroup>& pageGroups);
    NProto::TError
    Allocate(ui64 lsn, ui64* bit, TVector<TPageGroup>& pageGroups);
    [[nodiscard]] NProto::TError CountBits(ui64* bits) const;

    [[nodiscard]] ui64 GetMaxBits() const
    {
        return MaxBits;
    }

    [[nodiscard]] ui64 GetPageCount() const
    {
        const ui64 bitsPerPage = CalcBitsPerPage(PageSize);
        return AlignUp(MaxBits, bitsPerPage) / bitsPerPage;
    }

private:
    [[nodiscard]] bool Validate(ui64 bit, NProto::TError* error) const;
    [[nodiscard]] NProto::TError InitIfNeeded() const;

    [[nodiscard]] static ui64 CalcBitsPerPage(ui64 pageSize)
    {
        return pageSize * 8;
    }
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
