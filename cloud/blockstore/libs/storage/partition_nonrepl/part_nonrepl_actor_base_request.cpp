#include "part_nonrepl_actor_base_request.h"

#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

TDiskAgentBaseRequestActor::TDiskAgentBaseRequestActor(
        TRequestInfoPtr requestInfo,
        ui64 requestId,
        TString requestName,
        TRequestTimeoutPolicy timeoutPolicy,
        TVector<TDeviceRequest> deviceRequests,
        TNonreplicatedPartitionConfigPtr partConfig,
        const TActorId& part)
    : RequestInfo(std::move(requestInfo))
    , DeviceRequests(std::move(deviceRequests))
    , PartConfig(std::move(partConfig))
    , Part(part)
    , RequestName(std::move(requestName))
    , RequestId(requestId)
    , TimeoutPolicy(std::move(timeoutPolicy))
{}

void TDiskAgentBaseRequestActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_VolumeWorker,
        RequestInfo->CallContext->LWOrbit,
        RequestName,
        RequestInfo->CallContext->RequestId);

    StartTime = ctx.Now();
    ctx.Schedule(TimeoutPolicy.Timeout, new TEvents::TEvWakeup());

    SendRequest(ctx);
}

bool TDiskAgentBaseRequestActor::HandleError(
    const TActorContext& ctx,
    NProto::TError error,
    bool timedOut)
{
    if (FAILED(error.GetCode())) {
        ProcessError(*TActorContext::ActorSystem(), *PartConfig, error);

        Done(
            ctx,
            MakeResponse(std::move(error)),
            timedOut ? EStatus::Timeout : EStatus::Fail);
        return true;
    }

    return false;
}

void TDiskAgentBaseRequestActor::Done(
    const TActorContext& ctx,
    IEventBasePtr response,
    EStatus status)
{
    LWTRACK(
        ResponseSent_VolumeWorker,
        RequestInfo->CallContext->LWOrbit,
        RequestName,
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    ui32 blockCount = 0;
    for (const auto& dr: DeviceRequests) {
        blockCount += dr.BlockRange.Size();
    }

    auto completion = MakeCompletionResponse(blockCount);

    completion.Body->Status = status;
    completion.Body->TotalCycles = RequestInfo->GetTotalCycles();
    completion.Body->ExecCycles = RequestInfo->GetExecCycles();
    completion.Body->ExecutionTime = status == EStatus::Timeout
                                         ? TimeoutPolicy.Timeout
                                         : ctx.Now() - StartTime;

    for (const auto& dr: DeviceRequests) {
        completion.Body->DeviceIndices.push_back(dr.DeviceIdx);
    }

    NCloud::Send(ctx, Part, std::move(completion.Event));

    Die(ctx);
}

void TDiskAgentBaseRequestActor::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& device = DeviceRequests[ev->Cookie].Device;
    LOG_WARN_S(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        RequestName << " request #" << RequestId
                    << " timed out. Disk id: " << PartConfig->GetName().Quote()
                    << " Device: " << LogDevice(device));

    TString message = TStringBuilder() << RequestName << " request timed out";
    HandleError(
        ctx,
        MakeError(
            TimeoutPolicy.ErrorCode,
            TimeoutPolicy.OverrideMessage ? TimeoutPolicy.OverrideMessage
                                          : message),
        true);
}

void TDiskAgentBaseRequestActor::StateWork(TAutoPtr<NActors::IEventHandle>& ev)
{
    TRequestScope timer(*RequestInfo);

    if (OnMessage(ev)) {
        return;
    }

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleTimeout);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
