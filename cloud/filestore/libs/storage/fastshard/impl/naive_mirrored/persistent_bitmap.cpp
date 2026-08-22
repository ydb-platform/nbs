#include "persistent_bitmap.h"

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 InvalidBitNo = Max<ui64>();
constexpr ui64 BitsPerWord = 64;

bool IsFull(const TBuffer& bitmapPage)
{
    Y_ABORT_UNLESS(bitmapPage.Size() % sizeof(ui64) == 0);

    for (ui64 i = 0; i < bitmapPage.Size(); i += sizeof(ui64)) {
        const ui64* word = reinterpret_cast<const ui64*>(bitmapPage.Data() + i);
        if (~*word != 0) {
            return false;
        }
    }

    return true;
}

static ui16 PopCount(ui64 x)
{
    // 64-bit SWAR
    // https://www.playingwithpointers.com/blog/swar.html
    ui64 byteSums = x - ((x & 0xAAAAAAAAAAAAAAAAULL) >> 1);
    byteSums = (byteSums & 0x3333333333333333ULL)
        + ((byteSums >> 2) & 0x3333333333333333ULL);
    byteSums = (byteSums + (byteSums >> 4)) & 0x0F0F0F0F0F0F0F0FULL;

    return byteSums * 0x0101010101010101ULL >> 56;
}

ui64 PopCount(const TBuffer& bitmapPage)
{
    Y_ABORT_UNLESS(bitmapPage.Size() % sizeof(ui64) == 0);
    ui64 c = 0;

    for (ui64 i = 0; i < bitmapPage.Size(); i += sizeof(ui64)) {
        const ui64* word = reinterpret_cast<const ui64*>(bitmapPage.Data() + i);
        c += PopCount(*word);
    }

    return c;
}

bool GetBit(TBuffer& bitmapPage, ui64 bit)
{
    Y_ABORT_UNLESS(bitmapPage.Size() % sizeof(ui64) == 0);

    ui64* word =
        reinterpret_cast<ui64*>(bitmapPage.Data()) + bit / BitsPerWord;
    return (*word & (1ULL << (bit % BitsPerWord))) != 0;
}

void SetBit(TBuffer& bitmapPage, ui64 bit, bool isReset)
{
    Y_ABORT_UNLESS(bitmapPage.Size() % sizeof(ui64) == 0);

    ui64* word =
        reinterpret_cast<ui64*>(bitmapPage.Data()) + bit / BitsPerWord;
    if (isReset) {
        *word &= ~(1ULL << (bit % BitsPerWord));
    } else {
        *word |= 1ULL << (bit % BitsPerWord);
    }
}

ui64 FindFirstFreeBit(const TBuffer& bitmapPage)
{
    Y_ABORT_UNLESS(bitmapPage.Size() % sizeof(ui64) == 0);

    for (ui64 i = 0; i < bitmapPage.Size(); i += sizeof(ui64)) {
        const ui64* word = reinterpret_cast<const ui64*>(bitmapPage.Data() + i);
        if (~*word != 0) {
            return i * 8 + std::countr_one(*word);
        }
    }

    return InvalidBitNo;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TPersistentBitmap::Validate(ui64 bit, NProto::TError* error) const
{
    if (bit >= PageCount * BitsPerPage) {
        *error = MakeError(
            E_ARGUMENT,
            TStringBuilder() << "out of bounds"
                                " bitmap access: "
                             << bit << " >= " << (PageCount * BitsPerPage));
        return false;
    }

    return true;
}

NProto::TError TPersistentBitmap::Get(ui64 bit, bool* result) const
{
    NProto::TError error;
    if (!Validate(bit, &error)) {
        return error;
    }

    error = InitIfNeeded();
    if (HasError(error)) {
        return error;
    }

    const ui64 bitmapPageNo = bit / BitsPerPage;
    *result = GetBit(BitmapPages[bitmapPageNo], bit % BitsPerPage);
    return {};
}

NProto::TError
TPersistentBitmap::Set(ui64 lsn, ui64 bit, TVector<TPageGroup>& pageGroups)
{
    NProto::TError error;
    if (!Validate(bit, &error)) {
        return error;
    }

    error = InitIfNeeded();
    if (HasError(error)) {
        return error;
    }

    const ui64 bitmapPageNo = bit / BitsPerPage;
    SetBit(BitmapPages[bitmapPageNo], bit % BitsPerPage, false /* isReset */);

    const ui64 pageNo = FirstPageNo + bitmapPageNo;
    return PageStore
        ->WritePage(lsn, pageNo, BitmapPages[bitmapPageNo], pageGroups);
}

NProto::TError
TPersistentBitmap::Reset(ui64 lsn, ui64 bit, TVector<TPageGroup>& pageGroups)
{
    NProto::TError error;
    if (!Validate(bit, &error)) {
        return error;
    }

    error = InitIfNeeded();
    if (HasError(error)) {
        return error;
    }

    const ui64 bitmapPageNo = bit / BitsPerPage;
    auto& page = BitmapPages[bitmapPageNo];
    const bool wasFull = IsFull(page);
    SetBit(page, bit % BitsPerPage, true /* isReset */);

    if (wasFull) {
        BitmapPagesWithFreeBits.push(bitmapPageNo);
    }

    const ui64 pageNo = FirstPageNo + bitmapPageNo;
    return PageStore
        ->WritePage(lsn, pageNo, BitmapPages[bitmapPageNo], pageGroups);
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
        return PageStore->WritePage(lsn, pageNo, page, pageGroups);
    }

    return MakeError(E_FS_OUT_OF_SPACE, "bitmap full");
}

[[nodiscard]] NProto::TError TPersistentBitmap::CountBits(ui64* bits) const
{
    auto e = InitIfNeeded();
    if (HasError(e)) {
        return e;
    }

    *bits = 0;
    for (const auto& page: BitmapPages) {
        *bits += PopCount(page);
    }

    return {};
}

NProto::TError TPersistentBitmap::InitIfNeeded() const
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

        Y_ABORT_UNLESS(BitmapPages[i].Size() == PageSize);

        if (!IsFull(BitmapPages[i])) {
            BitmapPagesWithFreeBits.push(i);
        }
    }

    return {};
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
