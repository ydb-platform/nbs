#include "key_buffer_store_format.h"

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/string/builder.h>

#include <cstddef>
#include <cstring>
#include <type_traits>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 SuperblockMagic = 0x4B4253'53555042ULL;   // KBS'SUPB
constexpr ui64 PageMagic = 0x4B4253'50414745ULL;         // KBS'PAGE
constexpr ui32 StoreFormatVersion = 1;

// A page holds EntryCount entries, each behind a TEntryHeader, and carries a
// checksum over everything but the Crc field.
struct TPageHeader
{
    ui64 Magic = 0;
    ui32 Crc = 0;
    ui32 Version = 0;
    ui32 EntryCount = 0;
    ui32 Reserved = 0;
};

struct TEntryHeader
{
    ui64 Key = 0;
    ui64 Seq = 0;
    ui32 PayloadSize = 0;
    ui32 Reserved = 0;
};

// A superblock page holds the erased bound and the metadata buffer, which
// follows the header, and carries a checksum over everything but the Crc
// field.
struct TSuperblockHeader
{
    ui64 Magic = 0;
    ui64 Seq = 0;
    ui32 Crc = 0;
    ui32 Version = 0;
    ui64 ErasedBelowKey = 0;
    ui32 MetaSize = 0;
    ui32 Reserved = 0;
};

constexpr ui32 PageHeaderSize = sizeof(TPageHeader);
constexpr ui32 EntryHeaderSize = sizeof(TEntryHeader);
constexpr ui32 SuperblockHeaderSize = sizeof(TSuperblockHeader);

static_assert(PageHeaderSize == 24);
static_assert(EntryHeaderSize == 24);
static_assert(SuperblockHeaderSize == 40);

// The headers are copied to and from the pages as raw bytes, so they must
// be trivially copyable and must not contain padding.
static_assert(std::is_trivially_copyable_v<TPageHeader>);
static_assert(std::is_trivially_copyable_v<TEntryHeader>);
static_assert(std::is_trivially_copyable_v<TSuperblockHeader>);
static_assert(std::has_unique_object_representations_v<TPageHeader>);
static_assert(std::has_unique_object_representations_v<TEntryHeader>);
static_assert(std::has_unique_object_representations_v<TSuperblockHeader>);

////////////////////////////////////////////////////////////////////////////////

// The checksum of a page covers everything but the Crc field itself.
ui32 PageCrc(TStringBuf page)
{
    constexpr size_t crcOffset = offsetof(TPageHeader, Crc);
    constexpr size_t afterCrc = crcOffset + sizeof(TPageHeader::Crc);

    ui32 crc = Crc32c(page.data(), crcOffset);
    return Crc32cExtend(crc, page.data() + afterCrc, page.size() - afterCrc);
}

// The checksum of a superblock page covers everything but the Crc field
// itself.
ui32 SuperblockCrc(TStringBuf page)
{
    constexpr size_t crcOffset = offsetof(TSuperblockHeader, Crc);
    constexpr size_t afterCrc = crcOffset + sizeof(TSuperblockHeader::Crc);

    ui32 crc = Crc32c(page.data(), crcOffset);
    return Crc32cExtend(crc, page.data() + afterCrc, page.size() - afterCrc);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidatePageSize(ui32 pageSize)
{
    if (pageSize < PageHeaderSize + EntryHeaderSize ||
        pageSize < SuperblockHeaderSize)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "the page size " << pageSize
                             << " is too small for the store");
    }

    return MakeError(S_OK);
}

ui32 GetPageHeaderSize()
{
    return PageHeaderSize;
}

ui32 GetSerializedEntrySize(ui32 entryBufferSize)
{
    return EntryHeaderSize + entryBufferSize;
}

TResultOrError<TBuffer> MakePageBuffer(
    ui32 pageSize,
    const TVector<TEntry>& entries)
{
    TPageHeader header = {
        .Magic = PageMagic,
        .Version = StoreFormatVersion,
        .EntryCount = static_cast<ui32>(entries.size()),
    };

    TBuffer page(pageSize);
    page.Append(reinterpret_cast<const char*>(&header), sizeof(header));

    for (const auto& entry: entries) {
        if (page.Size() + EntryHeaderSize + entry.Data.size() > pageSize) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "the entry of " << entry.Data.size()
                    << " bytes under key " << entry.Key
                    << " does not fit the page of " << pageSize << " bytes");
        }

        TEntryHeader entryHeader = {
            .Key = entry.Key,
            .Seq = entry.Seq,
            .PayloadSize = static_cast<ui32>(entry.Data.size()),
        };

        page.Append(
            reinterpret_cast<const char*>(&entryHeader),
            sizeof(entryHeader));
        page.Append(entry.Data.data(), entry.Data.size());
    }

    page.Fill('\0', pageSize - page.Size());

    const ui32 crc = PageCrc({page.Data(), page.Size()});
    memcpy(page.Data() + offsetof(TPageHeader, Crc), &crc, sizeof(crc));

    return page;
}

TVector<TEntry> ParsePageBuffer(TStringBuf page, ui32 pageSize)
{
    if (page.size() != pageSize) {
        return {};
    }

    TPageHeader header;
    memcpy(&header, page.data(), sizeof(header));

    if (header.Magic != PageMagic || header.Version != StoreFormatVersion ||
        header.Crc != PageCrc(page) || header.EntryCount == 0)
    {
        return {};
    }

    TVector<TEntry> entries;
    entries.reserve(header.EntryCount);

    ui64 offset = PageHeaderSize;
    for (ui32 i = 0; i < header.EntryCount; ++i) {
        if (page.size() - offset < EntryHeaderSize) {
            return {};
        }

        TEntryHeader entryHeader;
        memcpy(&entryHeader, page.data() + offset, sizeof(entryHeader));
        offset += EntryHeaderSize;

        if (page.size() - offset < entryHeader.PayloadSize) {
            return {};
        }

        entries.push_back({
            .Key = entryHeader.Key,
            .Seq = entryHeader.Seq,
            .Data = page.SubStr(offset, entryHeader.PayloadSize),
        });
        offset += entryHeader.PayloadSize;
    }

    return entries;
}

ui32 GetSuperblockMetaCapacity(ui32 pageSize)
{
    if (pageSize < SuperblockHeaderSize) {
        return 0;
    }

    return pageSize - SuperblockHeaderSize;
}

// Lays out the metadata buffer behind the header, fills in the magic, the
// version and the checksum. The buffer must fit the page.
TResultOrError<TBuffer> MakeSuperblockPage(
    const TSuperblock& superblock,
    ui32 pageSize)
{
    const auto& meta = superblock.MetaBuffer;

    if (pageSize < SuperblockHeaderSize + meta.Size()) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "The metadata buffer of " << meta.Size()
                             << " bytes does not fit the superblock page of "
                             << pageSize << " bytes");
    }

    TSuperblockHeader header = {
        .Magic = SuperblockMagic,
        .Seq = superblock.Seq,
        .Version = StoreFormatVersion,
        .ErasedBelowKey = superblock.ErasedBelowKey,
        .MetaSize = static_cast<ui32>(meta.Size()),
    };

    TBuffer page(pageSize);
    page.Append(reinterpret_cast<const char*>(&header), sizeof(header));
    page.Append(meta.Data(), meta.Size());
    page.Fill('\0', pageSize - page.Size());

    const ui32 crc = SuperblockCrc({page.Data(), page.Size()});
    memcpy(page.Data() + offsetof(TSuperblockHeader, Crc), &crc, sizeof(crc));

    return page;
}

// Returns the contents of a valid superblock page, nothing for a page that
// holds anything else.
std::optional<TSuperblock> ParseSuperblockPage(TStringBuf page, ui32 pageSize)
{
    if (page.size() != pageSize) {
        return std::nullopt;
    }

    TSuperblockHeader header;
    memcpy(&header, page.data(), sizeof(header));

    if (header.Magic != SuperblockMagic ||
        header.Version != StoreFormatVersion ||
        header.MetaSize > pageSize - SuperblockHeaderSize ||
        header.Crc != SuperblockCrc(page))
    {
        return std::nullopt;
    }

    TStringBuf meta = page.SubStr(SuperblockHeaderSize, header.MetaSize);

    return TSuperblock{
        .Seq = header.Seq,
        .ErasedBelowKey = header.ErasedBelowKey,
        .MetaBuffer = TBuffer(meta.data(), meta.size()),
    };
}

}   // namespace NCloud::NJournalled
