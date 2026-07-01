#include "switchable_session.h"

#include <cloud/blockstore/libs/client/switchable_client.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <utility>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto CheckDrainCompletedPeriod = TDuration::MilliSeconds(100);

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
    const TMountVolumeArgs1& request)
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
    const TMountVolumeArgs2& request)
{
    return session->MountVolume(std::move(callContext), request.Headers);
}

auto DoExecute(
    ISession* session,
    TCallContextPtr callContext,
    const TUnmountVolumeArgs& request)
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

struct TSessionInfo: public TAtomicRefCount<TSessionInfo>
{
    const TString DiskId;
    const ISessionPtr Session;
    const ISwitchableBlockStorePtr SwitchableClient;
    TPromise<void> DrainPromise = NewPromise<void>();
    std::atomic<i64> InflightRequestCounter = 0;

    TSessionInfo(
        TString diskId,
        ISessionPtr session,
        ISwitchableBlockStorePtr switchableClient)
        : DiskId(std::move(diskId))
        , Session(std::move(session))
        , SwitchableClient(std::move(switchableClient))
    {}
};

using TSessionInfoPtr = TIntrusivePtr<TSessionInfo>;

////////////////////////////////////////////////////////////////////////////////

class TSwitchableSession final
    : public std::enable_shared_from_this<TSwitchableSession>
    , public ISwitchableSession
{
private:
    const ISchedulerPtr Scheduler;
    TLog Log;

    THotSwap<TSessionInfo> Session;
    size_t SwitchCount = 0;

public:
    TSwitchableSession(
        ILoggingServicePtr logging,
        ISchedulerPtr scheduler,
        TString diskId,
        ISessionPtr session,
        ISwitchableBlockStorePtr switchableClient)
        : Scheduler(std::move(scheduler))
        , Log(logging->CreateLog("BLOCKSTORE_CLIENT"))
    {
        TSessionInfoPtr sessionInfo = MakeIntrusive<TSessionInfo>(
            std::move(diskId),
            std::move(session),
            std::move(switchableClient));
        Session.AtomicStore(sessionInfo);
    }

    template <typename TRequest>
    auto ExecuteRequestWithInflightCount(
        TCallContextPtr callContext,
        TRequest request)
    {
        TSessionInfoPtr currentSession = Session.AtomicLoad();
        ++currentSession->InflightRequestCounter;

        auto future = DoExecute(
            currentSession->Session.get(),
            std::move(callContext),
            std::move(request));

        future.Subscribe(
            [currentSession = std::move(currentSession)]   //
            (const auto& f)
            {
                Y_UNUSED(f);

                i64 current = --currentSession->InflightRequestCounter;
                Y_DEBUG_ABORT_UNLESS(current >= 0);
            });
        return future;
    }

    // Implements ISwitchableSession
    TFuture<void> SwitchSession(
        const TString& newDiskId,
        const TString& newSessionId,
        ISessionPtr newSession,
        ISwitchableBlockStorePtr newSwitchableClient) override
    {
        TSessionInfoPtr currentSession = Session.AtomicLoad();

        STORAGE_INFO(
            "Switch #" << SwitchCount++ << " session from "
                       << currentSession->DiskId.Quote() << " to "
                       << newDiskId.Quote() << ". Inflight requests:"
                       << currentSession->InflightRequestCounter.load());
        currentSession->SwitchableClient->Switch(
            newSwitchableClient,
            newDiskId,
            newSessionId);

        TSessionInfoPtr newSessionInfo = MakeIntrusive<TSessionInfo>(
            newDiskId,
            std::move(newSession),
            std::move(newSwitchableClient));
        Session.AtomicStore(newSessionInfo);

        ScheduleCheckAllRequestsDrained(currentSession);
        return currentSession->DrainPromise;
    }

    // Implements ISession
    ui32 GetMaxTransfer() const override
    {
        TSessionInfoPtr currentSession = Session.AtomicLoad();
        return currentSession->Session->GetMaxTransfer();
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
        TSessionInfoPtr currentSession = Session.AtomicLoad();
        return currentSession->Session->AllocateBuffer(bytesCount);
    }

    void ReportIOError() override
    {
        TSessionInfoPtr currentSession = Session.AtomicLoad();
        currentSession->Session->ReportIOError();
    }

private:
    void ScheduleCheckAllRequestsDrained(TSessionInfoPtr sessionInfo)
    {
        Scheduler->Schedule(
            TInstant::Now() + CheckDrainCompletedPeriod,
            [sessionInfo = std::move(sessionInfo),
             weakSelf = weak_from_this()]   //
            () mutable
            {
                if (auto self = weakSelf.lock()) {
                    self->CheckAllRequestsDrained(std::move(sessionInfo));
                }
            });
    }

    void CheckAllRequestsDrained(TSessionInfoPtr sessionInfo)
    {
        const size_t count = sessionInfo->InflightRequestCounter;

        STORAGE_INFO(
            "Inflight request for " << sessionInfo->DiskId.Quote() << ": "
                                    << count);
        if (count == 0) {
            sessionInfo->DrainPromise.SetValue();
        } else {
            ScheduleCheckAllRequestsDrained(std::move(sessionInfo));
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
    ISwitchableBlockStorePtr switchableClient)
{
    return std::make_shared<TSwitchableSession>(
        std::move(logging),
        std::move(scheduler),
        std::move(diskId),
        std::move(session),
        std::move(switchableClient));
}

}   // namespace NCloud::NBlockStore::NClient
