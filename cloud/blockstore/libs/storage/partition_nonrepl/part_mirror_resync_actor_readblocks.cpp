#include "part_mirror_resync_actor.h"
#include "part_mirror_resync_fastpath_actor.h"
#include "part_mirror_resync_util.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <cloud/storage/core/libs/common/verify.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TMirrorPartitionResyncActor::ProcessReadRequestSyncPath(
    const typename TMethod::TRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto range = BuildRequestBlockRange(
        *ev->Get(),
        PartConfig->GetBlockSize());

    if (ResyncFinished || State.IsResynced(range)) {
        ForwardRequestWithNondeliveryTracking(ctx, MirrorActorId, *ev);
        return;
    }

    // Let's wait for resync, if we have request in certain replica or request
    // for all replicas
    if (ev->Get()->Record.GetHeaders().GetReplicaIndex() ||
        ev->Get()->Record.GetHeaders().GetReplicaCount())
    {
        ProcessReadRequestSlowPath(
            NActors::IEventHandlePtr(ev.Release()),
            range,
            ctx);
        return;
    }

    TVector<TReplicaDescriptor> replicas;
    // filtering out replicas with fresh devices
    for (ui32 i = 0; i < Replicas.size(); ++i) {
        if (State.DevicesReadyForReading(i, range)) {
            replicas.push_back(Replicas[i]);
        }
    }

    ProcessReadRequestFastPath(ev, std::move(replicas), range, ctx);
}

void TMirrorPartitionResyncActor::ProcessReadRequestFastPath(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    TVector<TReplicaDescriptor>&& replicas,
    TBlockRange64 range,
    const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Resync read fast path %s",
        PartConfig->GetName().c_str(),
        DescribeRange(range).c_str());

    TFastPathRecord fastPathRecord{
        .Ev{NActors::IEventHandlePtr(ev.Release())},
        .BlockRange{range}};

    auto blockSize = PartConfig->GetBlockSize();
    fastPathRecord.Buffer =
        TGuardedBuffer(TString::Uninitialized(range.Size() * blockSize));

    fastPathRecord.SgList = fastPathRecord.Buffer.GetGuardedSgList();
    auto sgListOrError =
        SgListNormalize(fastPathRecord.SgList.Acquire().Get(), blockSize);
    STORAGE_VERIFY(
        !HasError(sgListOrError),
        TWellKnownEntityTypes::DISK,
        PartConfig->GetName());
    fastPathRecord.SgList.SetSgList(sgListOrError.ExtractResult());

    auto requestInfo = CreateRequestInfo(
        SelfId(),
        FastPathReadCount,   // cookie
        MakeIntrusive<TCallContext>());

    NCloud::Register<TMirrorPartitionResyncFastPathActor>(
        ctx,
        std::move(requestInfo),
        blockSize,
        range,
        fastPathRecord.SgList,
        std::move(replicas),
        State.GetRWClientId());

    FastPathRecords[FastPathReadCount++] = std::move(fastPathRecord);
}

void TMirrorPartitionResyncActor::ProcessReadRequestFastPath(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    TVector<TReplicaDescriptor>&& replicas,
    TBlockRange64 range,
    const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Resync read local fast path %s",
        PartConfig->GetName().c_str(),
        DescribeRange(range).c_str());

    auto blockSize = PartConfig->GetBlockSize();
    auto msg = ev->Get();
    TFastPathRecord fastPathRecord{
        .Ev{NActors::IEventHandlePtr(ev.Release())},
        .BlockRange{range}};

    fastPathRecord.SgList = msg->Record.Sglist;

    auto requestInfo = CreateRequestInfo(
        SelfId(),
        FastPathReadCount,   // cookie
        MakeIntrusive<TCallContext>());

    NCloud::Register<TMirrorPartitionResyncFastPathActor>(
        ctx,
        std::move(requestInfo),
        blockSize,
        range,
        fastPathRecord.SgList,
        std::move(replicas),
        State.GetRWClientId());

    FastPathRecords[FastPathReadCount++] = std::move(fastPathRecord);
}

void TMirrorPartitionResyncActor::ProcessReadRequestSlowPath(
    NActors::IEventHandlePtr&& ev,
    TBlockRange64 range,
    const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Resync read slow path %s",
        PartConfig->GetName().c_str(),
        DescribeRange(range).c_str());

    const auto rangeId = BlockRange2RangeId(range, PartConfig->GetBlockSize());
    for (ui32 id = rangeId.first; id <= rangeId.second; ++id) {
        const auto blockRange =
            RangeId2BlockRange(id, PartConfig->GetBlockSize());
        if (!State.IsResynced(blockRange) && State.AddPendingResyncRange(id)) {
            ResyncNextRange(ctx);
        }
    }
    PostponedReads.push_back({NActors::IEventHandlePtr(ev.release()), range});
}

void TMirrorPartitionResyncActor::ProcessReadResponseFastPathLocal(
    const TFastPathRecord& record,
    const NActors::TActorContext& ctx)
{
    // Send empty response, received blocks are already stored in the SgList
    SendReadBlocksResponse(
        NProto::TError(),
        record,
        ctx);
}

void TMirrorPartitionResyncActor::ProcessReadResponseFastPath(
    const TFastPathRecord& record,
    const NActors::TActorContext& ctx)
{
    auto reqMsg = record.Ev->Get<TEvService::TEvReadBlocksRequest>();
    auto requestInfo = CreateRequestInfo(
        record.Ev->Sender,
        record.Ev->Cookie,   // cookie
        reqMsg->CallContext);

    auto blockCount = record.BlockRange.Size();
    auto response = std::make_unique<TEvService::TEvReadBlocksResponse>();

    LWTRACK(
        ResponseSent_PartitionWorker,
        requestInfo->CallContext->LWOrbit,
        "HandleReadBlocksResponse",
        requestInfo->CallContext->RequestId);

    auto& respBuffers = *response->Record.MutableBlocks()->MutableBuffers();
    auto guard = record.SgList.Acquire();
    if (!guard) {
        SendReadBlocksResponse(
            MakeError(E_CANCELLED, "Failed to acquire SgList"),
            record,
            ctx);
        return;
    }

    const auto& sglist = guard.Get();

    if (blockCount != sglist.size()) {
        ReportReadBlockCountMismatch(Sprintf(
            "TMirrorPartitionResyncActor: "
            "Number of read blocks doesn't match requested: %lu != %lu. "
            "Range %s",
            blockCount,
            sglist.size(),
            DescribeRange(record.BlockRange).c_str()));
        SendReadBlocksResponse(
            MakeError(E_FAIL, "Number of read blocks doesn't match request"),
            record,
            ctx);
        return;
    }

    respBuffers.Reserve(blockCount);
    for (ui32 i = 0; i < blockCount; ++i) {
        respBuffers.Add()->assign(sglist[i].Data(), sglist[i].Size());
    }

    NCloud::Reply(ctx, *requestInfo, std::move(response));
}

void TMirrorPartitionResyncActor::SendReadBlocksResponse(
        const NProto::TError& error,
        const TFastPathRecord& record,
        const NActors::TActorContext& ctx)
{
    auto requestInfo = CreateRequestInfo(
        record.Ev->Sender,
        record.Ev->Cookie,
        record.Ev->Get<TEvService::TEvReadBlocksLocalRequest>()->CallContext);


    auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>(
        error
    );

    LWTRACK(
        ResponseSent_PartitionWorker,
        requestInfo->CallContext->LWOrbit,
        "HandleReadBlocksLocalResponse",
        requestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *requestInfo, std::move(response));
}


////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::HandleReadBlocks(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ProcessReadRequestSyncPath<TEvService::TReadBlocksMethod>(ev, ctx);
}

void TMirrorPartitionResyncActor::HandleReadBlocksLocal(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ProcessReadRequestSyncPath<TEvService::TReadBlocksLocalMethod>(ev, ctx);
}

void TMirrorPartitionResyncActor::HandleReadResyncFastPathResponse(
    const TEvNonreplPartitionPrivate::TEvReadResyncFastPathResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto respMsg = ev->Get();
    auto* record = FastPathRecords.FindPtr(ev->Cookie);
    STORAGE_VERIFY(
        record,
        TWellKnownEntityTypes::DISK,
        PartConfig->GetName());

    if (HasError(respMsg->GetError())) {
        ProcessReadRequestSlowPath(
            NActors::IEventHandlePtr(record->Ev.release()),
            record->BlockRange,
            ctx);
        FastPathRecords.erase(ev->Cookie);
        return;
    }

    switch (record->Ev->GetTypeRewrite()) {
        case TEvService::EvReadBlocksRequest: {
            ProcessReadResponseFastPath(*record, ctx);
            break;
        }
        case TEvService::EvReadBlocksLocalRequest: {
            ProcessReadResponseFastPathLocal(*record, ctx);
            break;
        }

        default:
            STORAGE_VERIFY_C(
                false,
                TWellKnownEntityTypes::DISK,
                PartConfig->GetName(),
                TStringBuilder() << "unexpected ev type: "
                    << static_cast<int>(record->Ev->GetTypeRewrite()));
            break;
    }

    FastPathRecords.erase(ev->Cookie);
}

void TMirrorPartitionResyncActor::HandleGetDeviceForRange(
    const TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    bool canUseDirectCopy = ResyncFinished || State.IsResynced(msg->BlockRange);
    if (canUseDirectCopy) {
        GetDeviceForRangeCompanion.HandleGetDeviceForRange(ev, ctx);
    } else {
        GetDeviceForRangeCompanion.ReplyCanNotUseDirectCopy(ev, ctx);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
