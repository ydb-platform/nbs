#include "part_nonrepl_actor_base_request.h"

#include "part_nonrepl_common.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <util/string/join.h>

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace NCloud::NBlockStore::NStorage {

using EReason = TEvNonreplPartitionPrivate::TCancelRequest::EReason;

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
    ctx.Schedule(
        TimeoutPolicy.Timeout,
        new TEvNonreplPartitionPrivate::TEvCancelRequest(EReason::Timeouted));

    SendRequest(ctx);
}

void TDiskAgentBaseRequestActor::HandleError(
    const TActorContext& ctx,
    NProto::TError error,
    EStatus status)
{
    ProcessError(*TActorContext::ActorSystem(), *PartConfig, error);
    Done(ctx, MakeResponse(std::move(error)), status);
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

void TDiskAgentBaseRequestActor::HandleCancelRequest(
    const TEvNonreplPartitionPrivate::TEvCancelRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    TVector<TString> devices;
    for (const auto& request: DeviceRequests) {
        devices.push_back(request.Device.GetDeviceUUID());
    }

    switch (msg->Reason) {
        case EReason::Timeouted:
            LOG_WARN(
                ctx,
                TBlockStoreComponents::PARTITION_WORKER,
                "[%s] %s request #%lu timed out. Devices: [%s]",
                PartConfig->GetName().c_str(),
                RequestName.c_str(),
                RequestId,
                JoinSeq(", ", devices).c_str());
            HandleError(
                ctx,
                PartConfig->MakeError(
                    TimeoutPolicy.ErrorCode,
                    TimeoutPolicy.OverrideMessage
                        ? TimeoutPolicy.OverrideMessage
                        : (TStringBuilder()
                           << RequestName << " request timed out")),
                EStatus::Timeout);
            return;
        case EReason::Canceled:
            LOG_WARN(
                ctx,
                TBlockStoreComponents::PARTITION_WORKER,
                "[%s] %s request #%lu is canceled from outside. Devices: [%s]",
                PartConfig->GetName().c_str(),
                RequestName.c_str(),
                RequestId,
                JoinSeq(", ", devices).c_str());
            HandleError(
                ctx,
                PartConfig->MakeError(
                    E_REJECTED,
                    TStringBuilder() << RequestName << " request is canceled"),
                EStatus::Fail);
            return;
    }

    Y_DEBUG_ABORT_UNLESS(false);
    HandleError(
        ctx,
        PartConfig->MakeError(
            E_REJECTED,
            TStringBuilder()
                << RequestName << " request got an unknown cancel reason: "
                << static_cast<int>(msg->Reason)),
        EStatus::Fail);
}

void TDiskAgentBaseRequestActor::StateWork(TAutoPtr<NActors::IEventHandle>& ev)
{
    TRequestScope timer(*RequestInfo);

    if (OnMessage(ev)) {
        return;
    }

    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvNonreplPartitionPrivate::TEvCancelRequest,
            HandleCancelRequest);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
