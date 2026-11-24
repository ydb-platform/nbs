#pragma once

#include "helpers.h"
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

    mutable bool Initialized = false;
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
    NProto::TError GetForLookup(ui64 bit, bool* result) const;
    NProto::TError Set(ui64 lsn, ui64 bit, TVector<TPageGroup>& pageGroups);
    NProto::TError Reset(ui64 lsn, ui64 bit, TVector<TPageGroup>& pageGroups);
    NProto::TError
    Allocate(ui64 lsn, ui64* bit, TVector<TPageGroup>& pageGroups);
    [[nodiscard]] NProto::TError CountBits(ui64* bits) const;

    [[nodiscard]] ui64 GetMaxBits() const
    {
        return MaxBits;
    }

    [[nodiscard]] static ui64 GetPageCount(ui32 pageSize, ui64 maxBits)
    {
        const ui64 bitsPerPage = CalcBitsPerPage(pageSize);
        return RoundUp(maxBits, bitsPerPage) / bitsPerPage;
    }

    [[nodiscard]] ui64 GetPageCount() const
    {
        return GetPageCount(PageSize, MaxBits);
    }

private:
    [[nodiscard]] bool Validate(ui64 bit, NProto::TError* error) const;
    [[nodiscard]] NProto::TError InitIfNeeded() const;
    [[nodiscard]] NProto::TError ReadPage(
        ui64 lsn,
        ui64 relPageNo,
        TBuffer* page) const;
    [[nodiscard]] NProto::TError UpdateFreeStack(ui64 lsn) const;
    NProto::TError
    AllocateImpl(ui64 lsn, ui64* bit, TVector<TPageGroup>& pageGroups);

    [[nodiscard]] static ui64 CalcBitsPerPage(ui64 pageSize)
    {
        return pageSize * 8;
    }
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
