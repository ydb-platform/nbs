#include "log_record.h"

#include <util/stream/buffer.h>
#include <util/ysaveload.h>

namespace NCloud::NJournalled {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

namespace {

// Metadata and records are written as a flat sequence of fixed width fields.
// Load throws TLoadEOF once the buffer runs out, so a truncated buffer is
// rejected instead of read past its end.
bool AtEnd(IInputStream& in)
{
    char byte = 0;
    return in.Read(&byte, 1) == 0;
}

}   // namespace

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
    Save(&out, static_cast<ui64>(record.PageGroupIndex.size()));

    for (const auto& [pageNo, pageGroupRef]: record.PageGroupIndex) {
        Save(&out, pageNo);
        Save(&out, pageGroupRef.FirstPageNo);
        Save(&out, pageGroupRef.PageCount);
    }

    return buffer;
}

TLogRecordPtr DeserializeRecord(const TBuffer& buffer)
{
    try {
        TBufferInput in(buffer);
        auto record = std::make_shared<TLogRecord>();

        ui64 pageGroupCount = 0;
        Load(&in, record->Lsn);
        Load(&in, record->PrevLsn);
        Load(&in, pageGroupCount);

        constexpr size_t headerSize = 3 * sizeof(ui64);
        constexpr size_t entrySize = 3 * sizeof(ui64);
        if (pageGroupCount > (buffer.Size() - headerSize) / entrySize) {
            return nullptr;
        }

        record->PageGroupIndex.reserve(pageGroupCount);
        for (ui64 i = 0; i < pageGroupCount; ++i) {
            ui64 pageNo = 0;
            TPageGroupRef pageGroupRef;

            Load(&in, pageNo);
            Load(&in, pageGroupRef.FirstPageNo);
            Load(&in, pageGroupRef.PageCount);

            record->PageGroupIndex.emplace_back(pageNo, pageGroupRef);
        }

        if (!AtEnd(in)) {
            return nullptr;
        }

        record->Promise = NewPromise<NCloud::NProto::TError>();
        record->Ready.store(true);
        return record;
    } catch (const TSerializeException&) {
        return nullptr;
    }
}

}   // namespace NCloud::NJournalled
