#include "log_record.h"

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/system/yassert.h>

#include <cstring>
#include <limits>
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

}   // namespace

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
    Y_DEBUG_ABORT_UNLESS(
        pageMappingCount <= std::numeric_limits<ui32>::max(),
        "too many page mappings (%lu) in the log record with lsn %lu",
        pageMappingCount,
        record.Lsn);

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

    record->Promise = NewPromise<NCloud::NProto::TWriteLogRecordResponse>();
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

ui32 CalculateMinRecordBufferSize()
{
    auto logRecordBuffer = SerializeRecord(TLogRecord{
        .PageMappings = {TPageMapping()},
    });

    return static_cast<ui32>(logRecordBuffer.size());
}

}   // namespace NCloud::NJournalled
