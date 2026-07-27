#include "persistent_bitmap.h"

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 InvalidBitNo = Max<ui64>();
constexpr ui64 BitsPerWord = 64;

bool IsFull(const TString& bitmapPage)
{
    Y_ABORT_UNLESS(bitmapPage.size() % sizeof(ui64) == 0);

    for (ui64 i = 0; i < bitmapPage.size(); i += sizeof(ui64)) {
        const ui64* word = reinterpret_cast<const ui64*>(bitmapPage.data() + i);
        if (~*word != 0) {
            return false;
        }
    }

    return true;
}

void SetBit(TString& bitmapPage, ui64 bit, bool isReset)
{
    Y_ABORT_UNLESS(bitmapPage.size() % sizeof(ui64) == 0);

    ui64* word =
        reinterpret_cast<ui64*>(bitmapPage.begin() + bit / BitsPerWord);
    if (isReset) {
        *word &= ~(1ULL << bit);
    } else {
        *word |= 1ULL << bit;
    }
}

ui64 FindFirstFreeBit(const TString& bitmapPage)
{
    Y_ABORT_UNLESS(bitmapPage.size() % sizeof(ui64) == 0);

    for (ui64 i = 0; i < bitmapPage.size(); i += sizeof(ui64)) {
        const ui64* word = reinterpret_cast<const ui64*>(bitmapPage.data() + i);
        if (*word + 1 != 0) {
            return i * BitsPerWord + std::countr_one(*word);
        }
    }

    return InvalidBitNo;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TError TPersistentBitmap::Set(
    ui64 lsn,
    ui64 bit,
    TVector<TPageGroup>& pageGroups)
{
    auto error = InitIfNeeded();
    if (HasError(error)) {
        return error;
    }

    const ui64 bitmapPageNo = bit / BitsPerPage;
    SetBit(BitmapPages[bitmapPageNo], bit % BitsPerPage, false /* isReset */);

    const ui64 pageNo = FirstPageNo + bitmapPageNo;
    PageStore->WritePage(lsn, pageNo, BitmapPages[bitmapPageNo], pageGroups);
    return {};
}

NProto::TError TPersistentBitmap::Reset(
    ui64 lsn,
    ui64 bit,
    TVector<TPageGroup>& pageGroups)
{
    auto error = InitIfNeeded();
    if (HasError(error)) {
        return error;
    }

    const ui64 bitmapPageNo = bit / BitsPerPage;
    SetBit(BitmapPages[bitmapPageNo], bit % BitsPerPage, true /* isReset */);

    const ui64 pageNo = FirstPageNo + bitmapPageNo;
    PageStore->WritePage(lsn, pageNo, BitmapPages[bitmapPageNo], pageGroups);
    return {};
}

NProto::TError TPersistentBitmap::Allocate(
    ui64 lsn,
    ui64* bit,
    TVector<TPageGroup>& pageGroups)
{
    auto error = InitIfNeeded();
    if (HasError(error)) {
        return error;
    }

    while (!BitmapPagesWithFreeBits.empty()) {
        const ui64 bitmapPageNo = BitmapPagesWithFreeBits.top();
        auto& page = BitmapPages[bitmapPageNo];
        const ui64 pageBit = FindFirstFreeBit(page);
        if (pageBit == InvalidBitNo) {
            //
            // Can happen because of Set() calls.
            //

            BitmapPagesWithFreeBits.pop();
            continue;
        }

        SetBit(page, pageBit, false /* isReset */);
        if (IsFull(page)) {
            BitmapPagesWithFreeBits.pop();
        }

        *bit = bitmapPageNo * BitsPerPage + pageBit;

        const ui64 pageNo = FirstPageNo + bitmapPageNo;
        PageStore->WritePage(lsn, pageNo, page, pageGroups);
        return {};
    }

    return MakeError(E_FS_OUT_OF_SPACE, "bitmap full");
}

NProto::TError TPersistentBitmap::InitIfNeeded()
{
    if (!BitmapPages.empty()) {
        return {};
    }

    BitmapPages.resize(PageCount);

    for (ui64 i = 0; i < PageCount; ++i) {
        const ui64 pageNo = FirstPageNo + i;
        auto error = PageStore->ReadPage(0 /* lsn */, pageNo, &BitmapPages[i]);
        if (HasError(error)) {
            BitmapPages.clear();
            return error;
        }

        Y_ABORT_UNLESS(BitmapPages[i].size() == PageSize);

        if (!IsFull(BitmapPages[i])) {
            BitmapPagesWithFreeBits.push(i);
        }
    }

    return {};
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
