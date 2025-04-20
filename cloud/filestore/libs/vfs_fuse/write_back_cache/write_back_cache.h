#pragma once

#include <cloud/filestore/libs/service/context.h>
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
