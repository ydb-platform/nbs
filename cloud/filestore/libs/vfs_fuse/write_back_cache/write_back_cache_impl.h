#pragma once

#include "write_back_cache.h"

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

enum class TWriteBackCache::EWriteDataEntryStatus
{
    NotStarted,
    Pending,
    PendingFlushRequested,
    Cached,
    CachedFlushRequested,
    Finished
};

struct TWriteBackCache::TWriteDataEntry
    : public TIntrusiveListItem<TWriteDataEntry>
{
    std::shared_ptr<NProto::TWriteDataRequest> Request;
    TString RequestBuffer;
    NThreading::TPromise<NProto::TWriteDataResponse> Promise;
    NThreading::TPromise<void> FinishedPromise;
    EWriteDataEntryStatus Status = EWriteDataEntryStatus::NotStarted;

    // Is not valid when WriteDataRequestStatus is Finished
    TStringBuf Buffer;

    explicit TWriteDataEntry(
            std::shared_ptr<NProto::TWriteDataRequest> request)
        : Request(std::move(request))
        , RequestBuffer(std::move(*Request->MutableBuffer()))
        , Buffer(RequestBuffer)
    {
        Buffer.Skip(Request->GetBufferOffset());
        Request->ClearBuffer();
        Request->ClearBufferOffset();
    }

    ui64 Begin() const
    {
        return Request->GetOffset();
    }

    ui64 End() const
    {
        return Request->GetOffset() + Buffer.size();
    }
};

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

}   // namespace NCloud::NFileStore::NFuse
