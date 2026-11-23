#include "log_record.h"

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/system/yassert.h>

#include <cstring>
#include <limits>
#include <utility>
#include <type_traits>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TMetadataHeader
{
    ui64 Version = 0;
    ui64 LsnLowWatermark = 0;
};

struct TRecordHeader
{
    ui64 Lsn = 0;
    ui64 PrevLsn = 0;
    ui32 PageMappingCount = 0;
    ui32 PagesCrc = 0;
};

static_assert(sizeof(TMetadataHeader) == 16);
static_assert(sizeof(TRecordHeader) == 24);
static_assert(sizeof(TPageMapping) == 24);

static_assert(std::has_unique_object_representations_v<TMetadataHeader>);
static_assert(std::has_unique_object_representations_v<TRecordHeader>);
static_assert(std::has_unique_object_representations_v<TPageMapping>);

// Takes |byteCount| from the counter if it stays under |maxBytes|.
bool TryReserve(std::atomic<ui64>& cachedBytes, ui64 byteCount, ui64 maxBytes)
{
    //
    // Retry until the counter is updated or the limit is reached: several
    // threads can do this at the same time
    //

    ui64 cached = cachedBytes.load(std::memory_order_relaxed);
    do {
        if (cached > maxBytes || byteCount > maxBytes - cached) {
            return false;
        }
    } while (!cachedBytes.compare_exchange_weak(
        cached,
        cached + byteCount,
        std::memory_order_relaxed));

    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCachedPages::TCachedPages(TCachedPages&& other) noexcept
    : Pages(std::move(other.Pages))
    , CachedBytes(std::move(other.CachedBytes))
{
    other.Pages.clear();
}

TCachedPages& TCachedPages::operator=(TCachedPages&& other) noexcept
{
    //
    // Move the old pages into a temporary object: its destructor returns
    // their size to the counter
    //

    TCachedPages old(std::move(*this));

    Pages = std::move(other.Pages);
    CachedBytes = std::move(other.CachedBytes);
    other.Pages.clear();

    return *this;
}

TCachedPages::~TCachedPages()
{
    if (!CachedBytes) {
        return;
    }

    ui64 byteCount = 0;
    for (const auto& page: Pages) {
        byteCount += page.Size();
    }

    CachedBytes->fetch_sub(byteCount, std::memory_order_relaxed);
}

TCachedPages TCachedPages::Reserve(
    TVector<TBuffer> pages,
    std::shared_ptr<std::atomic<ui64>> cachedBytes,
    ui64 maxBytes)
{
    ui64 byteCount = 0;
    for (const auto& page: pages) {
        byteCount += page.Size();
    }

    if (!TryReserve(*cachedBytes, byteCount, maxBytes)) {
        return {};
    }

    TCachedPages result;
    result.Pages = std::move(pages);
    result.CachedBytes = std::move(cachedBytes);
    return result;
}

TCachedPages TCachedPages::Reserve(
    const TVector<TStringBuf>& pages,
    std::shared_ptr<std::atomic<ui64>> cachedBytes,
    ui64 maxBytes)
{
    ui64 byteCount = 0;
    for (const auto& page: pages) {
        byteCount += page.size();
    }

    if (!TryReserve(*cachedBytes, byteCount, maxBytes)) {
        return {};
    }

    TCachedPages result;
    result.CachedBytes = std::move(cachedBytes);

    result.Pages.reserve(pages.size());
    for (const auto& page: pages) {
        result.Pages.emplace_back(page.data(), page.size());
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TBuffer SerializeMetadata(const TJournalMetadata& metadata)
{
    TMetadataHeader header;
    header.Version = metadata.Version;
    header.LsnLowWatermark = metadata.LsnLowWatermark;

    TBuffer buffer(sizeof(header));
    buffer.Append(reinterpret_cast<const char*>(&header), sizeof(header));

    return buffer;
}

std::optional<TJournalMetadata> DeserializeMetadata(const TBuffer& buffer)
{
    if (buffer.Size() != sizeof(TMetadataHeader)) {
        return std::nullopt;
    }

    TMetadataHeader header;
    memcpy(&header, buffer.Data(), sizeof(header));

    if (header.Version != CurrentFormatVersion) {
        return std::nullopt;
    }

    return TJournalMetadata{
        .Version = static_cast<ui32>(header.Version),
        .LsnLowWatermark = header.LsnLowWatermark,
    };
}

TBuffer SerializeRecord(const TLogRecord& record)
{
    const ui64 pageMappingCount = record.PageMappings.size();

    TRecordHeader header;
    header.Lsn = record.Lsn;
    header.PrevLsn = record.PrevLsn;
    header.PageMappingCount = static_cast<ui32>(pageMappingCount);
    header.PagesCrc = record.PagesCrc;

    TBuffer buffer(sizeof(header) + pageMappingCount * sizeof(TPageMapping));
    buffer.Append(reinterpret_cast<const char*>(&header), sizeof(header));

    for (const auto& mapping: record.PageMappings) {
        buffer.Append(
            reinterpret_cast<const char*>(&mapping),
            sizeof(mapping));
    }

    return buffer;
}

TLogRecordPtr DeserializeRecord(const TBuffer& buffer)
{
    if (buffer.Size() < sizeof(TRecordHeader)) {
        return nullptr;
    }

    TRecordHeader header;
    memcpy(&header, buffer.Data(), sizeof(header));

    const size_t payloadSize = buffer.Size() - sizeof(header);
    if (payloadSize % sizeof(TPageMapping) != 0 ||
        header.PageMappingCount != payloadSize / sizeof(TPageMapping))
    {
        return nullptr;
    }

    auto record = std::make_shared<TLogRecord>();
    record->Lsn = header.Lsn;
    record->PrevLsn = header.PrevLsn;
    record->PagesCrc = header.PagesCrc;

    record->PageMappings.reserve(header.PageMappingCount);
    const char* ptr = buffer.Data() + sizeof(header);
    for (ui64 i = 0; i < header.PageMappingCount; ++i) {
        TPageMapping mapping;
        memcpy(&mapping, ptr, sizeof(mapping));
        ptr += sizeof(mapping);

        record->PageMappings.push_back(mapping);
    }

    record->Promise = NewPromise<NProto::TError>();
    return record;
}

ui32 CalculatePagesCrc(const TVector<TBuffer>& pages)
{
    ui32 crc = 0;
    for (const auto& page: pages) {
        crc = Crc32cExtend(crc, page.Data(), page.Size());
    }

    return crc;
}

ui32 CalculatePagesCrc(const TVector<TStringBuf>& pages)
{
    ui32 crc = 0;
    for (const auto& page: pages) {
        crc = Crc32cExtend(crc, page.data(), page.size());
    }

    return crc;
}

ui32 CalculateMinRecordBufferSize()
{
    auto logRecordBuffer = SerializeRecord(TLogRecord{
        .PageMappings = {TPageMapping()},
    });

    return static_cast<ui32>(logRecordBuffer.size());
}

}   // namespace NCloud::NJournalled
