#pragma once

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/vfs_fuse/public.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/threading/future/future.h>

#include <memory>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCache final
{
private:
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
        TDuration automaticFlushPeriod,
        TDuration flushRetryPeriod);

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

    enum class EWriteDataEntryStatus;
    class TWriteDataEntry;
    struct TWriteDataEntryPart;
    struct THandleState;
    struct TFlushState;
    class TUtil;
    struct TPendingOperations;
};

}   // namespace NCloud::NFileStore::NFuse
