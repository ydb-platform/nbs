#include "switchable_session.h"

#include <cloud/blockstore/libs/client/switchable_client.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/vector.h>
#include <util/system/mutex.h>

#include <utility>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEnsureVolumeMountedArgs
{
};

struct TMountVolumeArgs1
{
    NProto::EVolumeAccessMode AccessMode;
    NProto::EVolumeMountMode MountMode;
    ui64 MountSeqNumber;
    NProto::THeaders Headers;
};
struct TMountVolumeArgs2
{
    NProto::THeaders Headers;
};

struct TUnmountVolumeArgs
{
    NProto::THeaders Headers;
};

////////////////////////////////////////////////////////////////////////////////

auto DoExecute(
    ISession* session,
    TCallContextPtr callContext,
    TMountVolumeArgs1 request)
{
    return session->MountVolume(
        request.AccessMode,
        request.MountMode,
        request.MountSeqNumber,
        std::move(callContext),
        request.Headers);
}

auto DoExecute(
    ISession* session,
    TCallContextPtr callContext,
    TMountVolumeArgs2 request)
{
    return session->MountVolume(std::move(callContext), request.Headers);
}

auto DoExecute(
    ISession* session,
    TCallContextPtr callContext,
    TUnmountVolumeArgs request)
{
    return session->UnmountVolume(std::move(callContext), request.Headers);
}

auto DoExecute(
    ISession* session,
    TCallContextPtr callContext,
    TEnsureVolumeMountedArgs request)
{
    Y_UNUSED(callContext);
    Y_UNUSED(request);

    return session->EnsureVolumeMounted();
}

auto DoExecute(
    ISession* session,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
{
    return session->ReadBlocksLocal(std::move(callContext), std::move(request));
}

auto DoExecute(
    ISession* session,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    return session->WriteBlocksLocal(
        std::move(callContext),
        std::move(request));
}

auto DoExecute(
    ISession* session,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TZeroBlocksRequest> request)
{
    return session->ZeroBlocks(std::move(callContext), std::move(request));
}

auto DoExecute(
    ISession* session,
    TCallContextPtr callContext,
    NProto::EDeviceEraseMethod method)
{
    Y_UNUSED(callContext);

    return session->EraseDevice(method);
}

////////////////////////////////////////////////////////////////////////////////

struct TSharedCounter: std::enable_shared_from_this<TSharedCounter>
{
    std::atomic<size_t> Count{0};
};
using TSharedCounterPtr = std::shared_ptr<TSharedCounter>;

struct TSessionInfo
{
    TString DiskId;
    ISessionPtr Session;
    ISwitchableBlockStorePtr SwitchableClient;
    IBlockStorePtr DataClient;
    TPromise<void> DrainPromise = NewPromise<void>();
    TSharedCounterPtr InflightRequestCounter =
        std::make_shared<TSharedCounter>();
};

class TSwitchableSession final
    : public std::enable_shared_from_this<TSwitchableSession>
    , public ISwitchableSession
{
private:
    const ILoggingServicePtr Logging;
    const ISchedulerPtr Scheduler;
    TLog Log;

    std::array<TSessionInfo, 2> Sessions;
    std::atomic<size_t> ActiveSession{0};

public:
    TSwitchableSession(
        ILoggingServicePtr logging,
        ISchedulerPtr scheduler,
        TString diskId,
        ISessionPtr session,
        ISwitchableBlockStorePtr switchableClient,
        IBlockStorePtr dataClient)
        : Logging(std::move(logging))
        , Scheduler(std::move(scheduler))
        , Log(Logging->CreateLog("BLOCKSTORE_CLIENT"))
        , Sessions{
              TSessionInfo{
                  .DiskId = std::move(diskId),
                  .Session = std::move(session),
                  .SwitchableClient = std::move(switchableClient),
                  .DataClient = std::move(dataClient)},
              {}}
    {}

    // Implement ISwitchableSession

    TFuture<void> SwitchSession(
        const TString& newDiskId,
        ISessionPtr newSession,
        ISwitchableBlockStorePtr newSwitchableClient,
        IBlockStorePtr newDataClient,
        const TString& newSessionId) override
    {
        auto& oldSession = GetCurrent();
        size_t activeSession = ActiveSession.load();
        STORAGE_INFO(
            "Switch #" << activeSession << " session from "
                       << oldSession.DiskId.Quote() << " to "
                       << newDiskId.Quote() << ". Inflight requests:"
                       << oldSession.InflightRequestCounter->Count.load());
        oldSession.SwitchableClient->Switch(
            newDataClient,
            newDiskId,
            newSessionId);

        const size_t nextSession = (activeSession + 1) % Sessions.size();
        Sessions[nextSession] = {
            .DiskId = newDiskId,
            .Session = std::move(newSession),
            .SwitchableClient = std::move(newSwitchableClient),
            .DataClient = std::move(newDataClient)};

        ActiveSession.store(nextSession, std::memory_order_release);
        ScheduleCheckAllRequestsDrained(activeSession);
        return oldSession.DrainPromise;
    }

    template <typename TRequest>
    auto ExecuteRequestWithInflightCount(
        TCallContextPtr callContext,
        TRequest request)
    {
        auto& current = GetCurrent();
        auto counter = current.InflightRequestCounter;
        ++counter->Count;

        auto future = DoExecute(
            current.Session.get(),
            std::move(callContext),
            std::move(request));

        future.Subscribe(
            [counter = std::move(counter)](const auto& f)
            {
                Y_UNUSED(f);
                --counter->Count;
            });
        return future;
    }

    // Implement ISession

    ui32 GetMaxTransfer() const override
    {
        return GetCurrentSession()->GetMaxTransfer();
    }

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        NProto::EVolumeAccessMode accessMode,
        NProto::EVolumeMountMode mountMode,
        ui64 mountSeqNumber,
        TCallContextPtr callContext,
        const NProto::THeaders& headers) override
    {
        return ExecuteRequestWithInflightCount(
            std::move(callContext),
            TMountVolumeArgs1{
                .AccessMode = accessMode,
                .MountMode = mountMode,
                .MountSeqNumber = mountSeqNumber,
                .Headers = headers});
    }

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext,
        const NProto::THeaders& headers) override
    {
        return ExecuteRequestWithInflightCount(
            std::move(callContext),
            TMountVolumeArgs2{.Headers = headers});
    }

    TFuture<NProto::TUnmountVolumeResponse> UnmountVolume(
        TCallContextPtr callContext,
        const NProto::THeaders& headers) override
    {
        return ExecuteRequestWithInflightCount(
            std::move(callContext),
            TUnmountVolumeArgs{.Headers = headers});
    }

    TFuture<NProto::TMountVolumeResponse> EnsureVolumeMounted() override
    {
        return ExecuteRequestWithInflightCount(
            MakeIntrusive<TCallContext>(),
            TEnsureVolumeMountedArgs{});
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        return ExecuteRequestWithInflightCount(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        return ExecuteRequestWithInflightCount(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        return ExecuteRequestWithInflightCount(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        return ExecuteRequestWithInflightCount(
            MakeIntrusive<TCallContext>(),
            method);
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return GetCurrentSession()->AllocateBuffer(bytesCount);
    }

    void ReportIOError() override
    {
        GetCurrentSession()->ReportIOError();
    }

private:
    TSessionInfo& GetCurrent()
    {
        return Sessions[ActiveSession.load(std::memory_order_acquire)];
    }

    ISession* GetCurrentSession() const
    {
        return Sessions[ActiveSession.load(std::memory_order_acquire)]
            .Session.get();
    }

    void ScheduleCheckAllRequestsDrained(size_t sessionIndex)
    {
        Scheduler->Schedule(
            TInstant::Now() + TDuration::Seconds(5),
            [sessionIndex, weakSelf = weak_from_this()]
            {
                if (auto self = weakSelf.lock()) {
                    self->CheckAllRequestsDrained(sessionIndex);
                }
            });
    }

    void CheckAllRequestsDrained(size_t sessionIndex)
    {
        auto& session = Sessions[sessionIndex];
        size_t count = session.InflightRequestCounter->Count;
        STORAGE_INFO(
            "Inflight request for " << session.DiskId.Quote() << ": "
                                    << count);
        if (count == 0) {
            session.DrainPromise.SetValue();
        } else {
            ScheduleCheckAllRequestsDrained(sessionIndex);
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ISwitchableSessionPtr CreateSwitchableSession(
    ILoggingServicePtr logging,
    ISchedulerPtr scheduler,
    TString diskId,
    ISessionPtr session,
    ISwitchableBlockStorePtr switchableClient,
    IBlockStorePtr dataClient)
{
    return std::make_shared<TSwitchableSession>(
        std::move(logging),
        std::move(scheduler),
        std::move(diskId),
        std::move(session),
        std::move(switchableClient),
        std::move(dataClient));
}

}   // namespace NCloud::NBlockStore::NClient
