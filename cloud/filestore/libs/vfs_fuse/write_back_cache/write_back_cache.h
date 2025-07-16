#pragma once

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/vfs_fuse/public.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/intrlist.h>
#include <util/generic/strbuf.h>

#include <memory>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache final
{
private:
    friend class TImpl;
    class TImpl;

    std::shared_ptr<TImpl> Impl;

public:
    TWriteBackCache();

    TWriteBackCache(
        IFileStorePtr session,
        ISchedulerPtr scheduler,
        ITimerPtr timer,
        const TString& filePath,
        ui32 capacityBytes,
        TDuration automaticFlushPeriod);

    ~TWriteBackCache();

    explicit operator bool() const
    {
        return !!Impl;
    }

    NThreading::TFuture<NProto::TReadDataResponse> ReadData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request);

    NThreading::TFuture<NProto::TWriteDataResponse> WriteData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request);

    NThreading::TFuture<void> FlushData(ui64 handle);

    NThreading::TFuture<void> FlushAllData();

private:
    // only for testing purposes
    friend struct TCalculateDataPartsToReadTestBootstrap;

    enum class EFlushStatus
    {
        NotStarted,
        Started,
        Finished
    };

    class TWriteDataEntry
        : public TIntrusiveListItem<TWriteDataEntry>
    {
    private:
        // Store request metadata and request buffer separately
        // The idea is to deduplicate memory and to reference request buffer
        // directly in the cache if the request is cached.
        std::shared_ptr<NProto::TWriteDataRequest> Request;
        TString RequestBuffer;

        // Reference to either RequestBuffer or a memory region
        // in WriteDataRequestsQueue
        TStringBuf Buffer;

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
            return Buffer;
        }

        ui64 Begin() const
        {
            return Request->GetOffset();
        }

        ui64 End() const
        {
            return Request->GetOffset() + Buffer.size();
        }

        size_t GetSerializedSize() const;
        void SerializeToCache(char* cachePtr);

        EFlushStatus FlushStatus = EFlushStatus::NotStarted;
    };

    struct TWriteDataEntryPart
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

    static TVector<TWriteDataEntryPart> CalculateDataPartsToRead(
        const TVector<TWriteDataEntry*>& entries,
        ui64 startingFromOffset,
        ui64 length);
};

}   // namespace NCloud::NFileStore::NFuse
