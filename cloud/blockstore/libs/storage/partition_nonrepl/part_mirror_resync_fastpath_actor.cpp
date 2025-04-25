#include "part_mirror_resync_fastpath_actor.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/disk_agent/public.h>

#include <cloud/storage/core/libs/common/guarded_sglist.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

TMirrorPartitionResyncFastPathActor::TMirrorPartitionResyncFastPathActor(
        TRequestInfoPtr requestInfo,
        ui32 blockSize,
        TBlockRange64 range,
        TGuardedSgList sgList,
        TVector<TReplicaDescriptor> replicas,
        TString clientId)
    : RequestInfo(std::move(requestInfo))
    , BlockSize(blockSize)
    , Range(range)
    , Replicas(std::move(replicas))
    , ClientId(std::move(clientId))
    , SgList(std::move(sgList))
{}

void TMirrorPartitionResyncFastPathActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "MirrorPartitionResyncReadFastPath",
        RequestInfo->CallContext->RequestId);

    ChecksumBlocks(ctx);
}

void TMirrorPartitionResyncFastPathActor::ChecksumBlocks(
    const TActorContext& ctx)
{
    ReadBlocks(ctx);
    for (size_t i = 1; i < Replicas.size(); ++i) {
        ChecksumReplicaBlocks(ctx, i);
    }
}

void TMirrorPartitionResyncFastPathActor::ChecksumReplicaBlocks(
    const TActorContext& ctx,
    int idx)
{
    auto request = std::make_unique<
        TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest>();
    request->Record.SetStartIndex(Range.Start);
    request->Record.SetBlocksCount(Range.Size());

    auto* headers = request->Record.MutableHeaders();
    headers->SetIsBackgroundRequest(true);
    headers->SetClientId(TString(BackgroundOpsClientId));

    auto event = std::make_unique<IEventHandle>(
        Replicas[idx].ActorId,
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        idx,          // cookie
        &ctx.SelfID   // forwardOnNondelivery
    );

    ctx.Send(std::move(event));
}

void TMirrorPartitionResyncFastPathActor::CompareChecksums(
    const TActorContext& ctx)
{
    ui64 firstChecksum = Checksums[0];
    for (const auto& [idx, checksum]: Checksums) {
        if (firstChecksum != checksum) {
            LOG_INFO(
                ctx,
                TBlockStoreComponents::PARTITION,
                "[%s] Resync range %s: checksum mismatch, %lu (0) != %lu (%d)"
                ", fast path reading is not available",
                Replicas[0].ReplicaId.c_str(),
                DescribeRange(Range).c_str(),
                firstChecksum,
                checksum,
                idx);
            Error = MakeError(E_REJECTED, "Checksum mismatch detected");
            Done(ctx);
            return;
        }
    }

    Done(ctx);
}

void TMirrorPartitionResyncFastPathActor::ReadBlocks(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvReadBlocksLocalRequest>();
    request->Record.SetStartIndex(Range.Start);
    request->Record.SetBlocksCount(Range.Size());
    request->Record.BlockSize = BlockSize;
    request->Record.Sglist = SgList;

    auto* headers = request->Record.MutableHeaders();
    headers->SetIsBackgroundRequest(true);
    auto clientId = ClientId;
    if (!clientId) {
        clientId = BackgroundOpsClientId;
    }
    headers->SetClientId(std::move(clientId));

    auto event = std::make_unique<IEventHandle>(
        Replicas[0].ActorId,
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        0,            // cookie
        &ctx.SelfID   // forwardOnNondelivery
    );

    ctx.Send(std::move(event));
}

void TMirrorPartitionResyncFastPathActor::CalculateChecksum()
{
    if (auto guard = SgList.Acquire()) {
        TBlockChecksum checksum;
        const TSgList& sgList = guard.Get();
        for (auto blockData: sgList) {
            checksum.Extend(blockData.Data(), blockData.Size());
        }
        Checksums.insert({0, checksum.GetValue()});
    }
}

void TMirrorPartitionResyncFastPathActor::Done(const TActorContext& ctx)
{
    auto response = std::make_unique<
        TEvNonreplPartitionPrivate::TEvReadResyncFastPathResponse>(
        std::move(Error));
    LWTRACK(
        ResponseSent_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "ReadBlocksFastPath",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncFastPathActor::HandleChecksumUndelivery(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "ChecksumBlocks request undelivered");

    Done(ctx);
}

void TMirrorPartitionResyncFastPathActor::HandleChecksumResponse(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    Error = msg->Record.GetError();

    if (HasError(Error)) {
        Done(ctx);
        return;
    }

    Checksums.insert({ev->Cookie, msg->Record.GetChecksum()});
    if (Checksums.size() == Replicas.size()) {
        CompareChecksums(ctx);
    }
}

void TMirrorPartitionResyncFastPathActor::HandleReadResponse(
    const TEvService::TEvReadBlocksLocalResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Error = ev->Get()->GetError();
    if (HasError(Error)) {
        Done(ctx);
        return;
    }

    CalculateChecksum();
    if (Checksums.size() == Replicas.size()) {
        CompareChecksums(ctx);
    }
}

void TMirrorPartitionResyncFastPathActor::HandleReadUndelivery(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "ReadBlocksLocal request undelivered");
    Done(ctx);
}

void TMirrorPartitionResyncFastPathActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "Dead");
    Done(ctx);
}

STFUNC(TMirrorPartitionResyncFastPathActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest,
            HandleChecksumUndelivery);
        HFunc(
            TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse,
            HandleChecksumResponse);
        HFunc(TEvService::TEvReadBlocksLocalRequest, HandleReadUndelivery);
        HFunc(TEvService::TEvReadBlocksLocalResponse, HandleReadResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
