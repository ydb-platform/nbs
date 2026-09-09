#include "log_record.h"

#include <util/stream/buffer.h>
#include <util/ysaveload.h>

namespace NCloud::NJournalled {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool AtEnd(IInputStream& in)
{
    char byte = 0;
    return in.Read(&byte, 1) == 0;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TBuffer SerializeMetadata(const TJournalMetadata& metadata)
{
    TBuffer buffer;
    TBufferOutput out(buffer);

    Save(&out, metadata.Version);
    Save(&out, metadata.LastAckedLsn);

    return buffer;
}

std::optional<TJournalMetadata> DeserializeMetadata(const TBuffer& buffer)
{
    try {
        TBufferInput in(buffer);

        TJournalMetadata metadata;
        Load(&in, metadata.Version);
        Load(&in, metadata.LastAckedLsn);

        if (metadata.Version != CurrentFormatVersion || !AtEnd(in)) {
            return std::nullopt;
        }

        return metadata;
    } catch (const TSerializeException&) {
        return std::nullopt;
    }
}

TBuffer SerializeRecord(const TLogRecord& record)
{
    TBuffer buffer;
    TBufferOutput out(buffer);

    Save(&out, record.Lsn);
    Save(&out, record.PrevLsn);
    Save(&out, static_cast<ui64>(record.PageMappings.size()));

    for (const auto& [pageNo, location]: record.PageMappings) {
        Save(&out, pageNo);
        Save(&out, location.FirstPageNo);
        Save(&out, location.PageCount);
    }

    return buffer;
}

TLogRecordPtr DeserializeRecord(const TBuffer& buffer)
{
    try {
        TBufferInput in(buffer);
        auto record = std::make_shared<TLogRecord>();

        ui64 pageMappingCount = 0;
        Load(&in, record->Lsn);
        Load(&in, record->PrevLsn);
        Load(&in, pageMappingCount);

        constexpr size_t headerSize = 3 * sizeof(ui64);
        constexpr size_t entrySize = 3 * sizeof(ui64);
        if (pageMappingCount > (buffer.Size() - headerSize) / entrySize) {
            return nullptr;
        }

        record->PageMappings.reserve(pageMappingCount);
        for (ui64 i = 0; i < pageMappingCount; ++i) {
            TPageMapping mapping;

            Load(&in, mapping.PageNo);
            Load(&in, mapping.Location.FirstPageNo);
            Load(&in, mapping.Location.PageCount);

            record->PageMappings.push_back(mapping);
        }

        if (!AtEnd(in)) {
            return nullptr;
        }

        record->Promise = NewPromise<NCloud::NProto::TWriteLogRecordResponse>();
        return record;
    } catch (const TSerializeException&) {
        return nullptr;
    }
}

}   // namespace NCloud::NJournalled
