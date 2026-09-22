#include "log_record.h"

#include <cstring>
#include <type_traits>

namespace NCloud::NJournalled {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TMetadataHeader
{
    ui32 Version = 0;
    ui32 Reserved = 0;
    ui64 LastAckedLsn = 0;
};

struct TRecordHeader
{
    ui64 Lsn = 0;
    ui64 PrevLsn = 0;
    ui64 PageMappingCount = 0;
};

static_assert(sizeof(TMetadataHeader) == 16);
static_assert(sizeof(TRecordHeader) == 24);
static_assert(sizeof(TPageMapping) == 24);

static_assert(std::is_trivially_copyable_v<TMetadataHeader>);
static_assert(std::is_trivially_copyable_v<TRecordHeader>);
static_assert(std::is_trivially_copyable_v<TPageMapping>);
static_assert(std::has_unique_object_representations_v<TMetadataHeader>);
static_assert(std::has_unique_object_representations_v<TRecordHeader>);
static_assert(std::has_unique_object_representations_v<TPageMapping>);

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBuffer SerializeMetadata(const TJournalMetadata& metadata)
{
    TMetadataHeader header;
    header.Version = metadata.Version;
    header.LastAckedLsn = metadata.LastAckedLsn;

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

    if (header.Version != CurrentFormatVersion || header.Reserved != 0) {
        return std::nullopt;
    }

    return TJournalMetadata{
        .Version = header.Version,
        .LastAckedLsn = header.LastAckedLsn,
    };
}

TBuffer SerializeRecord(const TLogRecord& record)
{
    TRecordHeader header;
    header.Lsn = record.Lsn;
    header.PrevLsn = record.PrevLsn;
    header.PageMappingCount = record.PageMappings.size();

    TBuffer buffer(
        sizeof(header) + record.PageMappings.size() * sizeof(TPageMapping));
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

}   // namespace NCloud::NJournalled
