#include "filesystem_client.h"

#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_method.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/aio/aio.h>
#include <library/cpp/containers/concurrent_hash/concurrent_hash.h>

#include <util/generic/size_literals.h>
#include <util/string/builder.h>
#include <util/string/vector.h>
#include <util/system/mutex.h>

#include <cstdlib>

namespace NCloud::NBlockStore::NFilesystemClient {

namespace {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

struct TClientBase: public TBlockStoreImpl<TClientBase, IBlockStore>
{
    void Start() override
    {}

    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        return MakeFuture<typename TMethod::TResponse>(TErrorResponse(
            E_NOT_IMPLEMENTED,
            TStringBuilder()
                << "Unsupported request " << TMethod::GetName().Quote()));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TAsyncIOWrapper
{
    NAsyncIO::TAsyncIOService AsyncIO;

    TAsyncIOWrapper()
    {
        AsyncIO.Start();
    }

    ~TAsyncIOWrapper()
    {
        AsyncIO.Stop();
    }

    static auto& Instance()
    {
        static TAsyncIOWrapper instance;
        return instance.AsyncIO;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TClient: TClientBase
{
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    IMonitoringServicePtr Monitoring;
    IServerStatsPtr ClientStats;

    using THandlePtr = std::shared_ptr<TFileHandle>;
    TConcurrentHashMap<TString, THandlePtr> Files;
    TVector<TString> FilePaths;
    TMutex Lock;
    TLog Log;

    static const ui32 BlockSize = 4_KB;

    TClient(
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        IServerStatsPtr clientStats)
        : Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Monitoring(std::move(monitoring))
        , ClientStats(std::move(clientStats))
    {
        Log = logging->CreateLog("FILESYSTEM_CLIENT");
    }

    void Start() override
    {}

    void Stop() override
    {
        with_lock (Lock) {
            for (auto& x: FilePaths) {
                THandlePtr handle;
                if (Files.Get(x, handle) && handle->IsOpen()) {
                    handle->Close();
                }
            }
        }
    }

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TMountVolumeRequest> request) override
    {
        Y_UNUSED(callContext);

        THandlePtr handle;
        if (!Files.Get(request->GetDiskId(), handle)) {
            with_lock (Lock) {
                handle = std::make_shared<TFileHandle>(
                    request->GetDiskId(),
                    EOpenModeFlag::DirectAligned | EOpenModeFlag::RdWr);

                if (!handle->IsOpen() && errno == EINVAL) {
                    STORAGE_WARN(
                        TStringBuilder()
                        << "Failed to open file " << request->GetDiskId()
                        << " with O_DIRECT, opening without it");

                    handle = std::make_shared<TFileHandle>(
                        request->GetDiskId(),
                        EOpenModeFlag::RdWr);
                }

                if (handle->IsOpen()) {
                    STORAGE_INFO(
                        TStringBuilder()
                        << "Mounted volume " << request->GetDiskId()
                        << ", fd = " << FHANDLE(*handle));

                    Files.Insert(request->GetDiskId(), handle);
                    FilePaths.push_back(request->GetDiskId());
                } else {
                    return MakeFuture<NProto::TMountVolumeResponse>(
                        TErrorResponse(
                            E_IO,
                            TStringBuilder() << "Failed to open file "
                                             << request->GetDiskId()
                                             << ", errno=" << errno));
                }
            }
        }

        NProto::TMountVolumeResponse response;
        response.MutableVolume()->SetBlockSize(BlockSize);
        response.MutableVolume()->SetDiskId(request->GetDiskId());
        return MakeFuture<NProto::TMountVolumeResponse>(std::move(response));
    }

    TFuture<NProto::TUnmountVolumeResponse> UnmountVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TUnmountVolumeRequest> request) override
    {
        Y_UNUSED(callContext);

        THandlePtr handle;
        if (!Files.Get(request->GetDiskId(), handle)) {
            return MakeFuture<NProto::TUnmountVolumeResponse>(
                TErrorResponse(S_ALREADY));
        }

        handle->Close();
        Files.Remove(request->GetDiskId());
        return MakeFuture<NProto::TUnmountVolumeResponse>();
    }

    TFuture<NProto::TStatVolumeResponse> StatVolume(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TStatVolumeRequest> request) override
    {
        Y_UNUSED(callContext);

        NProto::TError error;

        if (!Files.Has(request->GetDiskId())) {
            error = MakeError(
                E_NOT_FOUND,
                TStringBuilder() << "no such volume: " << request->GetDiskId());
        }

        return MakeFuture<NProto::TStatVolumeResponse>(
            TErrorResponse(std::move(error)));
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);

        THandlePtr handle;
        if (!Files.Get(request->GetDiskId(), handle)) {
            return MakeFuture<NProto::TReadBlocksLocalResponse>(
                TErrorResponse(E_BS_INVALID_SESSION));
        }

        auto sglist = request->Sglist.Acquire();
        if (!sglist) {
            return MakeFuture<NProto::TReadBlocksLocalResponse>(
                TErrorResponse(E_REJECTED));
        }

        Y_ABORT_UNLESS(request->BlockSize == BlockSize);
        ui64 dataSize = 0;
        for (const auto& b: sglist.Get()) {
            dataSize += b.Size();
        }
        Y_ABORT_UNLESS(dataSize > 0);
        auto buffer = Acalloc(dataSize);

        auto future = TAsyncIOWrapper::Instance().Read(
            *handle,
            buffer.get(),
            dataSize,
            request->GetStartIndex() * BlockSize);

        return future.Apply(
            [=](NThreading::TFuture<ui32> f)
            {
                try {
                    if (f.GetValue() < dataSize) {
                        return NProto::TReadBlocksLocalResponse(TErrorResponse(
                            E_REJECTED,
                            TStringBuilder()
                                << "read less than expected: " << f.GetValue()
                                << " < " << dataSize));
                    }
                } catch (...) {
                    return NProto::TReadBlocksLocalResponse(
                        TErrorResponse(E_IO, CurrentExceptionMessage()));
                }

                auto sglist = request->Sglist.Acquire();
                if (!sglist) {
                    return NProto::TReadBlocksLocalResponse(
                        TErrorResponse(E_REJECTED));
                }

                SgListCopy(TBlockDataRef(buffer.get(), dataSize), sglist.Get());

                return NProto::TReadBlocksLocalResponse();
            });
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);

        THandlePtr handle;
        if (!Files.Get(request->GetDiskId(), handle)) {
            return MakeFuture<NProto::TWriteBlocksLocalResponse>(
                TErrorResponse(E_BS_INVALID_SESSION));
        }

        auto sglist = request->Sglist.Acquire();
        if (!sglist) {
            return MakeFuture<NProto::TWriteBlocksLocalResponse>(
                TErrorResponse(E_REJECTED));
        }

        Y_ABORT_UNLESS(request->BlockSize == BlockSize);
        ui64 dataSize = 0;
        for (const auto& b: sglist.Get()) {
            dataSize += b.Size();
        }
        Y_ABORT_UNLESS(dataSize > 0);
        auto buffer = Acalloc(dataSize);

        SgListCopy(sglist.Get(), TBlockDataRef(buffer.get(), dataSize));

        auto future = TAsyncIOWrapper::Instance().Write(
            *handle,
            buffer.get(),
            dataSize,
            request->GetStartIndex() * BlockSize);

        return future.Apply(
            [dataSize, buffer](NThreading::TFuture<ui32> f)
            {
                try {
                    if (f.GetValue() < dataSize) {
                        return NProto::TWriteBlocksLocalResponse(TErrorResponse(
                            E_REJECTED,
                            TStringBuilder()
                                << "written less than expected: "
                                << f.GetValue() << " < " << dataSize));
                    }
                } catch (...) {
                    return NProto::TWriteBlocksLocalResponse(
                        TErrorResponse(E_IO, CurrentExceptionMessage()));
                }

                return NProto::TWriteBlocksLocalResponse();
            });
    }

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        Y_UNUSED(callContext);

        THandlePtr handle;
        if (!Files.Get(request->GetDiskId(), handle)) {
            return MakeFuture<NProto::TZeroBlocksResponse>(
                TErrorResponse(E_BS_INVALID_SESSION));
        }

        const ui64 dataSize = request->GetBlocksCount() * BlockSize;
        Y_ABORT_UNLESS(dataSize > 0);
        auto buffer = Acalloc(dataSize);

        auto future = TAsyncIOWrapper::Instance().Write(
            *handle,
            buffer.get(),
            dataSize,
            request->GetStartIndex() * BlockSize);

        return future.Apply(
            [dataSize, buffer](NThreading::TFuture<ui32> f)
            {
                try {
                    if (f.GetValue() < dataSize) {
                        return NProto::TZeroBlocksResponse(TErrorResponse(
                            E_REJECTED,
                            TStringBuilder()
                                << "written less than expected: "
                                << f.GetValue() << " < " << dataSize));
                    }
                } catch (...) {
                    return NProto::TZeroBlocksResponse(
                        TErrorResponse(E_IO, CurrentExceptionMessage()));
                }

                return NProto::TZeroBlocksResponse();
            });
    }

    std::shared_ptr<char> Acalloc(ui64 dataSize)
    {
        std::shared_ptr<char> buffer = {
            static_cast<char*>(aligned_alloc(BlockSize, dataSize)),
            [](auto* p)
            {
                free(p);
            }};
        memset(buffer.get(), 0, dataSize);

        return buffer;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateClient(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IServerStatsPtr clientStats)
{
    return std::make_shared<TClient>(
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(monitoring),
        std::move(clientStats));
}

}   // namespace NCloud::NBlockStore::NFilesystemClient
