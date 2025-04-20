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
    TWriteBackCache(
        IFileStorePtr session,
        ISchedulerPtr scheduler,
        ITimerPtr timer,
        TDuration automaticFlushPeriod,
        const TString& filePath,
        ui32 capacityBytes);

    ~TWriteBackCache();

    NThreading::TFuture<NProto::TReadDataResponse> ReadData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request);

    NThreading::TFuture<NProto::TWriteDataResponse> WriteData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request);

    NThreading::TFuture<void> FlushData(ui64 handle);

    NThreading::TFuture<void> FlushAllData();

private:
    struct TWriteDataEntry
        : public TIntrusiveListItem<TWriteDataEntry>
    {
        ui64 Handle;
        ui64 Offset;
        ui64 Length;
        // serialized TWriteDataRequest
        TStringBuf SerializedRequest;

        TWriteDataEntry(
                ui64 handle,
                ui64 offset,
                ui64 length,
                TStringBuf serializedRequest)
            : Handle(handle)
            , Offset(offset)
            , Length(length)
            , SerializedRequest(serializedRequest)
        {}

        ui64 End() const
        {
            return Offset + Length;
        }

        bool Flushing = false;
        bool Flushed = false;
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
    };

    static TVector<TWriteDataEntryPart> CalculateDataPartsToRead(
        const TVector<TWriteDataEntry*>& entries,
        ui64 startingFromOffset,
        ui64 length);
};

////////////////////////////////////////////////////////////////////////////////

TWriteBackCachePtr CreateWriteBackCache(
    IFileStorePtr session,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    TDuration automaticFlushPeriod,
    const TString& filePath,
    ui32 capacityBytes);

}   // namespace NCloud::NFileStore::NFuse
