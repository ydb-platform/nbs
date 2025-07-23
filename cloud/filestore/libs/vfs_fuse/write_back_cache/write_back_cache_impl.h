#pragma once

#include "write_back_cache.h"

#include <util/generic/intrlist.h>

namespace NCloud::NFileStore::NFuse {

enum class TWriteBackCache::EFlushStatus
{
    NotStarted,
    Started,
    Finished
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteBackCache::TWriteDataEntry
    : public TIntrusiveListItem<TWriteDataEntry>
{
    ui64 Handle;
    ui64 Offset;
    ui64 Length;
    // serialized TWriteDataRequest
    TStringBuf SerializedRequest;
    TString FileSystemId;
    NProto::THeaders RequestHeaders;

    TWriteDataEntry(
            ui64 handle,
            ui64 offset,
            ui64 length,
            TStringBuf serializedRequest,
            TString fileSystemId,
            NProto::THeaders requestHeaders)
        : Handle(handle)
        , Offset(offset)
        , Length(length)
        , SerializedRequest(serializedRequest)
        , FileSystemId(std::move(fileSystemId))
        , RequestHeaders(std::move(requestHeaders))
    {}

    ui64 End() const
    {
        return Offset + Length;
    }

    EFlushStatus FlushStatus = EFlushStatus::NotStarted;
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteBackCache::TWriteDataEntryPart
{
    const TWriteDataEntry* Source = nullptr;
    ui64 OffsetInSource = 0;
    ui64 Offset = 0;
    ui64 Length = 0;

    ui64 End() const
    {
        return Offset + Length;
    }

    bool operator==(const TWriteDataEntryPart& p) const
    {
        return std::tie(Source, OffsetInSource, Offset, Length) ==
            std::tie(p.Source, p.OffsetInSource, p.Offset, p.Length);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TDataPartsUtil
{
public:
    static TVector<TWriteDataEntryPart> CalculateDataPartsToRead(
        const TVector<TWriteDataEntry*>& entries,
        ui64 startingFromOffset,
        ui64 length);
};

}   // namespace NCloud::NFileStore::NFuse
