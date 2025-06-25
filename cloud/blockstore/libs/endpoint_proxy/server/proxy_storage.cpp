#include "proxy_storage.h"

#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/public/api/protos/io.pb.h>

#include <cloud/storage/core/protos/media.pb.h>

#include <util/datetime/base.h>
#include <util/datetime/cputimer.h>

#include <atomic>

namespace NCloud::NBlockStore::NServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRequestStats: public IProxyRequestStats
{
private:
    TRequestTypeStatsArray RequestType2Stats;

    TRequestTypeStats& S(EBlockStoreRequest requestType)
    {
        return RequestType2Stats[static_cast<ui32>(requestType)];
    }

public:
    const TRequestTypeStatsArray& GetInternalStats() const override
    {
        return RequestType2Stats;
    }

    ui64 RequestStarted(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        ui64 requestBytes) override
    {
        Y_UNUSED(mediaKind);

        auto& s = S(requestType);
        s.Count.fetch_add(1, std::memory_order_relaxed);
        s.RequestBytes.fetch_add(requestBytes, std::memory_order_relaxed);
        s.Inflight.fetch_add(1, std::memory_order_relaxed);
        s.InflightBytes.fetch_add(requestBytes, std::memory_order_relaxed);

        return GetCycleCount();
    }

    TDuration RequestCompleted(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        ui64 requestStarted,
        TDuration postponedTime,
        ui64 requestBytes,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags,
        bool unaligned,
        ECalcMaxTime calcMaxTime,
        ui64 responseSent) override
    {
        Y_UNUSED(mediaKind);
        Y_UNUSED(postponedTime);
        Y_UNUSED(errorFlags);
        Y_UNUSED(unaligned);
        Y_UNUSED(calcMaxTime);

        auto& s = S(requestType);
        s.Inflight.fetch_sub(1, std::memory_order_relaxed);
        s.InflightBytes.fetch_sub(requestBytes, std::memory_order_relaxed);
        s.E(errorKind).fetch_add(1, std::memory_order_relaxed);

        return CyclesToDurationSafe(responseSent - requestStarted);
    }

    void AddIncompleteStats(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        TRequestTime requestTime,
        ECalcMaxTime calcMaxTime) override
    {
        Y_UNUSED(mediaKind);
        Y_UNUSED(requestType);
        Y_UNUSED(requestTime);
        Y_UNUSED(calcMaxTime);
    }

    void AddRetryStats(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags) override
    {
        Y_UNUSED(mediaKind);
        Y_UNUSED(requestType);
        Y_UNUSED(errorKind);
        Y_UNUSED(errorFlags);
    }

    void RequestPostponed(EBlockStoreRequest requestType) override
    {
        Y_UNUSED(requestType);
    }

    void RequestPostponedServer(EBlockStoreRequest requestType) override
    {
        Y_UNUSED(requestType);
    }

    void RequestAdvanced(EBlockStoreRequest requestType) override
    {
        Y_UNUSED(requestType);
    }

    void RequestAdvancedServer(EBlockStoreRequest requestType) override
    {
        Y_UNUSED(requestType);
    }

    void RequestFastPathHit(EBlockStoreRequest requestType) override
    {
        Y_UNUSED(requestType);
    }

    void BatchCompleted(
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        ui64 count,
        ui64 bytes,
        ui64 errors,
        std::span<TTimeBucket> timeHist,
        std::span<TSizeBucket> sizeHist) override
    {
        Y_UNUSED(mediaKind);
        Y_UNUSED(requestType);
        Y_UNUSED(count);
        Y_UNUSED(bytes);
        Y_UNUSED(errors);
        Y_UNUSED(timeHist);
        Y_UNUSED(sizeHist);
    }

    void UpdateStats(bool updateIntervalFinished) override
    {
        Y_UNUSED(updateIntervalFinished);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TProxyStorage final: public IStorage
{
private:
    const IBlockStorePtr Service;
    const IRequestStatsPtr RequestStats;
    const ui32 BlockSize;

public:
    explicit TProxyStorage(
            IBlockStorePtr service,
            IRequestStatsPtr requestStats,
            ui32 blockSize)
        : Service(std::move(service))
        , RequestStats(std::move(requestStats))
        , BlockSize(blockSize)
    {}

    NThreading::TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        const auto requestBytes = request->GetBlocksCount() * BlockSize;
        const auto requestStarted = RequestStats->RequestStarted(
            NProto::STORAGE_MEDIA_DEFAULT,
            EBlockStoreRequest::ZeroBlocks,
            requestBytes);

        auto f = Service->ZeroBlocks(
            std::move(callContext),
            std::move(request));

        f.Subscribe([=, rs = RequestStats] (const auto& f) {
            auto errorKind = EDiagnosticsErrorKind::ErrorFatal;
            try {
                const auto& result = f.GetValue();
                errorKind = GetDiagnosticsErrorKind(result.GetError());
            } catch (...) {
                Cerr << "unexpected exception in ZeroBlocks result: "
                    << CurrentExceptionMessage() << Endl;
                Y_DEBUG_ABORT_UNLESS(0);
            }

            rs->RequestCompleted(
                NProto::STORAGE_MEDIA_DEFAULT,
                EBlockStoreRequest::ZeroBlocks,
                requestStarted,
                TDuration::Zero(),
                requestBytes,
                errorKind,
                0,
                false,
                ECalcMaxTime::DISABLE,
                GetCycleCount());
        });

        return f;
    }

    NThreading::TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        ui64 requestBytes = 0;
        if (auto g = request->Sglist.Acquire()) {
            for (const auto& block: g.Get()) {
                requestBytes += block.Size();
            }
        }
        const auto requestStarted = RequestStats->RequestStarted(
            NProto::STORAGE_MEDIA_DEFAULT,
            EBlockStoreRequest::ReadBlocks,
            requestBytes);

        auto f = Service->ReadBlocksLocal(
            std::move(callContext),
            std::move(request));

        f.Subscribe([=, rs = RequestStats] (const auto& f) {
            auto errorKind = EDiagnosticsErrorKind::ErrorFatal;
            try {
                const auto& result = f.GetValue();
                errorKind = GetDiagnosticsErrorKind(result.GetError());
            } catch (...) {
                Cerr << "unexpected exception in ReadBlocksLocal result: "
                    << CurrentExceptionMessage() << Endl;
                Y_DEBUG_ABORT_UNLESS(0);
            }

            rs->RequestCompleted(
                NProto::STORAGE_MEDIA_DEFAULT,
                EBlockStoreRequest::ReadBlocks,
                requestStarted,
                TDuration::Zero(),
                requestBytes,
                errorKind,
                0,
                false,
                ECalcMaxTime::DISABLE,
                GetCycleCount());
        });

        return f;
    }

    NThreading::TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        ui64 requestBytes = 0;
        if (auto g = request->Sglist.Acquire()) {
            for (const auto& block: g.Get()) {
                requestBytes += block.Size();
            }
        }
        const auto requestStarted = RequestStats->RequestStarted(
            NProto::STORAGE_MEDIA_DEFAULT,
            EBlockStoreRequest::WriteBlocks,
            requestBytes);

        auto f = Service->WriteBlocksLocal(
            std::move(callContext),
            std::move(request));

        f.Subscribe([=, rs = RequestStats] (const auto& f) {
            auto errorKind = EDiagnosticsErrorKind::ErrorFatal;
            try {
                const auto& result = f.GetValue();
                errorKind = GetDiagnosticsErrorKind(result.GetError());
            } catch (...) {
                Cerr << "unexpected exception in WriteBlocksLocal result: "
                    << CurrentExceptionMessage() << Endl;
                Y_DEBUG_ABORT_UNLESS(0);
            }

            rs->RequestCompleted(
                NProto::STORAGE_MEDIA_DEFAULT,
                EBlockStoreRequest::WriteBlocks,
                requestStarted,
                TDuration::Zero(),
                requestBytes,
                errorKind,
                0,
                false,
                ECalcMaxTime::DISABLE,
                GetCycleCount());
        });

        return f;
    }

    NThreading::TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        Y_UNUSED(method);
        return NThreading::MakeFuture(MakeError(E_NOT_IMPLEMENTED));
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    void ReportIOError() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IProxyRequestStatsPtr CreateProxyRequestStats()
{
    return std::make_shared<TRequestStats>();
}

IStoragePtr CreateProxyStorage(
    IBlockStorePtr blockStore,
    IRequestStatsPtr requestStats,
    ui32 blockSize)
{
    return std::make_shared<TProxyStorage>(
        std::move(blockStore),
        std::move(requestStats),
        blockSize);
}

}   // namespace NCloud::NBlockStore::NServer
