#pragma once

#include "page_store.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/stack.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

class TPersistentBitmap
{
private:
    const ui64 FirstPageNo;
    const ui64 PageCount;
    const ui64 PageSize;
    const ui64 BitsPerPage;
    IPageStorePtr PageStore;

    mutable TVector<TString> BitmapPages;
    mutable TStack<ui64> BitmapPagesWithFreeBits;

public:
    TPersistentBitmap(
            ui64 firstPageNo,
            ui64 pageCount,
            ui64 pageSize,
            IPageStorePtr pageStore)
        : FirstPageNo(firstPageNo)
        , PageCount(pageCount)
        , PageSize(pageSize)
        , BitsPerPage(CalcBitsPerPage(pageSize))
        , PageStore(std::move(pageStore))
    {
    }

public:
    NProto::TError Get(ui64 bit, bool* result) const;
    NProto::TError Set(ui64 lsn, ui64 bit, TVector<TPageGroup>& pageGroups);
    NProto::TError Reset(ui64 lsn, ui64 bit, TVector<TPageGroup>& pageGroups);
    NProto::TError Allocate(
        ui64 lsn,
        ui64* bit,
        TVector<TPageGroup>& pageGroups);

    static ui64 CalcBitsPerPage(ui64 pageSize)
    {
        return pageSize * 8;
    }

private:
    bool Validate(ui64 bit, NProto::TError* error) const;
    NProto::TError InitIfNeeded() const;
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
