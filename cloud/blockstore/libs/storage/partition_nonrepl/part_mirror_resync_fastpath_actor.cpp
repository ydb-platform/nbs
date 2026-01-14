#include "part_mirror_resync_fastpath_actor.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/common/request_checksum_helpers.h>
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
        TString diskId,
        ui32 blockSize,
        TBlockRange64 range,
        TGuardedSgList sgList,
        TVector<TReplicaDescriptor> replicas,
        TString clientId,
        bool optimizeFastPathReadsOnResync)
    : RequestInfo(std::move(requestInfo))
    , DiskId(std::move(diskId))
    , BlockSize(blockSize)
    , Range(range)
    , Replicas(std::move(replicas))
    , ClientId(std::move(clientId))
    , OptimizeFastPathReadsOnResync(optimizeFastPathReadsOnResync)
    , SgList(std::move(sgList))
{
    Y_ABORT_UNLESS(!Replicas.empty());
}

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

void TMirrorPartitionResyncFastPathActor::ReadBlocks(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvReadBlocksLocalRequest>();
    request->Record.SetStartIndex(Range.Start);
    request->Record.SetBlocksCount(Range.Size());
    request->Record.BlockSize = BlockSize;
    request->Record.Sglist = SgList;

    auto* headers = request->Record.MutableHeaders();
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

void TMirrorPartitionResyncFastPathActor::ChecksumReplicaBlocks(
    const TActorContext& ctx,
    int idx)
{
    auto request = std::make_unique<
        TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest>();
    request->Record.SetStartIndex(Range.Start);
    request->Record.SetBlocksCount(Range.Size());

    auto* headers = request->Record.MutableHeaders();
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
    const NActors::TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(!Checksums.empty());
    Y_DEBUG_ABORT_UNLESS(DataChecksum);

    if (!OptimizeFastPathReadsOnResync && !AllChecksumsReceived()) {
        return;
    }

    const auto mismatchedReplica = FindChecksumMismatch();
    if (mismatchedReplica) {
        Error = MakeError(E_REJECTED, "Checksum mismatch detected");

        const ui32 checksum = Checksums[*mismatchedReplica];
        LOG_WARN(
            ctx,
            TBlockStoreComponents::PARTITION_WORKER,
            "[%s] Resync range %s: checksum mismatch, %u (data) != %u "
            "(%u), %s",
            DiskId.c_str(),
            DescribeRange(Range).c_str(),
            *DataChecksum,
            checksum,
            *mismatchedReplica,
            (ReplySent ? "fast path reading is not available"
                       : "fast resync failed"));
    } else {
        Error = NProto::TError();
    }
}

void TMirrorPartitionResyncFastPathActor::MaybeCompareChecksums(
    const TActorContext& ctx)
{
    if (!DataChecksum) {
        return;
    }

    if (Replicas.size() == 1) {
        if (!Error) {
            Error = NProto::TError();
        }
        ReplyAndDie(ctx);
        return;
    }

    if (Checksums.empty()) {
        return;
    }

    CompareChecksums(ctx);
    MaybeReplyAndDie(ctx);
}

bool TMirrorPartitionResyncFastPathActor::MaybeReplyAndDie(
    const NActors::TActorContext& ctx)
{
    if (!Error) {
        return false;
    }

    if (!ReplySent) {
        Reply(ctx);
        if (!OptimizeFastPathReadsOnResync || HasError(*Error)) {
            Die(ctx);
            return true;
        }
    }

    if (AllChecksumsReceived() || HasError(*Error)) {
        FinishResync(ctx);
        Die(ctx);
        return true;
    }

    return false;
}

void TMirrorPartitionResyncFastPathActor::CalculateChecksum(
    const TActorContext& ctx)
{
    auto guard = SgList.Acquire();
    if (!guard) {
        Error = MakeError(E_CANCELLED, "Failed to acquire sglist");
        ReplyAndDie(ctx);
        return;
    }

    const auto checksum = NCloud::NBlockStore::CalculateChecksum(guard.Get());
    Y_DEBUG_ABORT_UNLESS(checksum.GetByteCount() == Range.Size() * BlockSize);
    DataChecksum = checksum.GetChecksum();
}

std::optional<ui32>
TMirrorPartitionResyncFastPathActor::FindChecksumMismatch() const
{
    for (auto [idx, checksum]: Checksums) {
        if (*DataChecksum != checksum) {
            return idx;
        }
    }
    return std::nullopt;
}

bool TMirrorPartitionResyncFastPathActor::AllChecksumsReceived() const {
    return Checksums.size() + 1 >= Replicas.size();
}

void TMirrorPartitionResyncFastPathActor::Reply(
    const TActorContext& ctx)
{
    Y_ABORT_UNLESS(Error);
    Y_DEBUG_ABORT_UNLESS(!ReplySent);
    ReplySent = true;

    auto response = std::make_unique<
        TEvNonreplPartitionPrivate::TEvResyncFastPathReadResponse>(*Error);
    LWTRACK(
        ResponseSent_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "ReadBlocksFastPath",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
}

void TMirrorPartitionResyncFastPathActor::FinishResync(const TActorContext& ctx)
{
    Y_ABORT_UNLESS(Error);
    auto response = std::make_unique<
        TEvNonreplPartitionPrivate::TEvResyncFastPathChecksumCompareResponse>(
        *Error,
        Range);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
}

void TMirrorPartitionResyncFastPathActor::ReplyAndDie(const TActorContext& ctx)
{
    Reply(ctx);
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncFastPathActor::HandleChecksumUndelivery(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "ChecksumBlocks request undelivered");
    const bool dead = MaybeReplyAndDie(ctx);
    Y_DEBUG_ABORT_UNLESS(dead);
}

void TMirrorPartitionResyncFastPathActor::HandleChecksumResponse(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->Record.GetError())) {
        Error = std::move(*msg->Record.MutableError());
        const bool dead = MaybeReplyAndDie(ctx);
        Y_DEBUG_ABORT_UNLESS(dead);
        return;
    }

    Checksums.emplace(
        ev->Cookie,
        SafeIntegerCast<ui32>(msg->Record.GetChecksum()));
    MaybeCompareChecksums(ctx);
}

void TMirrorPartitionResyncFastPathActor::HandleReadResponse(
    const TEvService::TEvReadBlocksLocalResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->Record.GetError())) {
        Error = std::move(*msg->Record.MutableError());
        ReplyAndDie(ctx);
        return;
    }

    CalculateChecksum(ctx);
    MaybeCompareChecksums(ctx);
}

void TMirrorPartitionResyncFastPathActor::HandleReadUndelivery(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "ReadBlocksLocal request undelivered");
    ReplyAndDie(ctx);
}

void TMirrorPartitionResyncFastPathActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "Dead");
    MaybeReplyAndDie(ctx);
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
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
