#pragma once

#include "write_back_cache.h"

#include <cloud/filestore/libs/service/filestore.h>

#include <util/generic/intrlist.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

enum class TWriteBackCache::EFlushStatus
{
    NotStarted,
    Started,
    Finished
};

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache::TWriteDataEntry
    : public TIntrusiveListItem<TWriteDataEntry>
{
private:
    // Store request metadata and request buffer separately
    // The idea is to deduplicate memory and to reference request buffer
    // directly in the persistent buffer if the request is stored there.
    std::shared_ptr<NProto::TWriteDataRequest> Request;
    TString RequestBuffer;

    // Reference to either RequestBuffer or a memory region
    // in WriteDataRequestsQueue
    TStringBuf BufferRef;

public:
    explicit TWriteDataEntry(
        std::shared_ptr<NProto::TWriteDataRequest> request);

    TWriteDataEntry(ui32 checksum, TStringBuf serializedRequest);

    const NProto::TWriteDataRequest* GetRequest() const
    {
        return Request.get();
    }

    ui64 GetHandle() const
    {
        return Request->GetHandle();
    }

    TStringBuf GetBuffer() const
    {
        return BufferRef;
    }

    ui64 Offset() const
    {
        return Request->GetOffset();
    }

    ui64 End() const
    {
        return Request->GetOffset() + BufferRef.size();
    }

    size_t GetSerializedSize() const;
    void SerializeAndMoveRequestBuffer(char* allocationPtr);

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

class TWriteBackCache::TUtil
{
public:
    static TVector<TWriteDataEntryPart> CalculateDataPartsToRead(
        const TVector<TWriteDataEntry*>& entries,
        ui64 startingFromOffset,
        ui64 length);

    static TVector<TWriteDataEntryPart> InvertDataParts(
        const TVector<TWriteDataEntryPart>& sortedParts,
        ui64 startingFromOffset,
        ui64 length);
};

}   // namespace NCloud::NFileStore::NFuse
