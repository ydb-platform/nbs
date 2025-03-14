#include "part_mirror_actor.h"

#include "mirror_request_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::HandleWriteOrZeroCompleted(
    const TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto requestIdentityKey = msg->RequestId;
    auto completeRequest =
        RequestsInProgress.ExtractRequest(requestIdentityKey);
    if (!completeRequest) {
        return;
    }
    DrainActorCompanion.ProcessDrainRequests(ctx);
    auto [range, volumeRequestId] = completeRequest.value();
    for (const auto& [id, request]: RequestsInProgress.AllRequests()) {
        if (range.Overlaps(request.Value.BlockRange)) {
            DirtyReadRequestIds.insert(id);
        }
    }

    if (ResyncActorId) {
        auto completion = std::make_unique<
            TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted>(
            volumeRequestId,
            msg->TotalCycles,
            msg->FollowerGotNonRetriableError);

        auto undeliveredRequestActor = MakeUndeliveredHandlerServiceId();
        auto newEv = std::make_unique<IEventHandle>(
            ResyncActorId,
            ev->Sender,
            completion.release(),
            ev->Flags | IEventHandle::FlagForwardOnNondelivery,
            ev->Cookie,
            &undeliveredRequestActor);

        ctx.Send(std::move(newEv));
    }
}

void TMirrorPartitionActor::HandleMirroredReadCompleted(
    const TEvNonreplPartitionPrivate::TEvMirroredReadCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    const auto requestIdentityKey = ev->Get()->RequestCounter;
    auto requestCtx = RequestsInProgress.ExtractRequest(requestIdentityKey);
    auto it = DirtyReadRequestIds.find(requestIdentityKey);
    if (it == DirtyReadRequestIds.end()) {
        if (ev->Get()->ChecksumMismatchObserved) {
            ReportMirroredDiskChecksumMismatchUponRead(
                TStringBuilder()
                << " disk: " << DiskId << ", range: "
                << (requestCtx ? requestCtx->BlockRange.Print() : ""));
        }
    } else {
        DirtyReadRequestIds.erase(it);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TMirrorPartitionActor::MirrorRequest(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    if (HasError(Status)) {
        Reply(
            ctx,
            *requestInfo,
            std::make_unique<typename TMethod::TResponse>(Status)
        );

        return;
    }

    const auto range = BuildRequestBlockRange(
        *ev->Get(),
        State.GetBlockSize());
    const auto requestIdentityKey = TakeNextRequestIdentifier();
    if (GetScrubbingRange().Overlaps(range)) {
        if (ResyncRangeStarted) {
            auto response = std::make_unique<typename TMethod::TResponse>(
                MakeError(
                    E_REJECTED,
                    TStringBuilder()
                        << "Request " << TMethod::Name
                        << " intersects with currently resyncing range"));
            NCloud::Reply(ctx, *ev, std::move(response));
            return;
        }
        WriteIntersectsWithScrubbing = true;
    }
    for (const auto& [id, request]: RequestsInProgress.AllRequests()) {
        if (range.Overlaps(request.Value.BlockRange)) {
            DirtyReadRequestIds.insert(id);
        }
    }
    RequestsInProgress.AddWriteRequest(
        requestIdentityKey,
        {range, ev->Get()->Record.GetHeaders().GetVolumeRequestId()});

    NCloud::Register<TMirrorRequestActor<TMethod>>(
        ctx,
        std::move(requestInfo),
        State.GetReplicaActorsVector(),
        std::move(msg->Record),
        State.GetReplicaInfos()[0].Config->GetName(),   // diskId
        SelfId(),                                       // parentActorId
        requestIdentityKey);
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::HandleWriteBlocks(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    MirrorRequest<TEvService::TWriteBlocksMethod>(ev, ctx);
}

void TMirrorPartitionActor::HandleWriteBlocksLocal(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    MirrorRequest<TEvService::TWriteBlocksLocalMethod>(ev, ctx);
}

void TMirrorPartitionActor::HandleZeroBlocks(
    const TEvService::TEvZeroBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    MirrorRequest<TEvService::TZeroBlocksMethod>(ev, ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
