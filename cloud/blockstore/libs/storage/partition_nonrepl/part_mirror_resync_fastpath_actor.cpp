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

    NCloud::SendWithUndeliveryTracking(
        ctx,
        Replicas[0].ActorId,
        std::move(request),
        0   // cookie
    );
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

    NCloud::SendWithUndeliveryTracking(
        ctx,
        Replicas[idx].ActorId,
        std::move(request),
        idx   // cookie
    );
}

NProto::TError TMirrorPartitionResyncFastPathActor::CompareChecksums(
    const TActorContext& ctx) const
{
    Y_DEBUG_ABORT_UNLESS(!Checksums.empty());
    Y_DEBUG_ABORT_UNLESS(DataChecksum);

    auto it = FindIf(Checksums, [&] (const auto& kv) {
        return *DataChecksum != kv.second;
    });

    if (it != Checksums.end()) {
        const auto [mismatchedReplica, checksum] = *it;

        LOG_WARN(
            ctx,
            TBlockStoreComponents::PARTITION_WORKER,
            "[%s] Resync range %s: checksum mismatch, %u (data) != %u "
            "(%u), %s",
            DiskId.c_str(),
            DescribeRange(Range).c_str(),
            *DataChecksum,
            checksum,
            mismatchedReplica,
            (ReplySent ? "fast path reading is not available"
                       : "fast resync failed"));

        return MakeError(E_REJECTED, "Checksum mismatch detected");
    }

    return {};
}

void TMirrorPartitionResyncFastPathActor::MaybeCompareChecksums(
    const TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(Replicas.size() > 1);

    if (!DataChecksum || Checksums.empty()) {
        return;
    }

    if (OptimizeFastPathReadsOnResync || AllChecksumsReceived()) {
        auto compareChecksumsError = CompareChecksums(ctx);
        MaybeReplyAndDie(ctx, compareChecksumsError);
    }
}

bool TMirrorPartitionResyncFastPathActor::MaybeReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (!ReplySent) {
        Reply(ctx, error);
        if (!OptimizeFastPathReadsOnResync || HasError(error)) {
            Die(ctx);
            return true;
        }
    }

    if (AllChecksumsReceived() || HasError(error)) {
        FinishResync(ctx, error);
        Die(ctx);
        return true;
    }

    return false;
}

TResultOrError<ui32>
TMirrorPartitionResyncFastPathActor::CalculateDataChecksum() const
{
    auto guard = SgList.Acquire();
    if (!guard) {
        return MakeError(E_CANCELLED, "Failed to acquire sglist");
    }

    const auto checksum = CalculateChecksum(guard.Get());
    Y_DEBUG_ABORT_UNLESS(checksum.GetByteCount() == Range.Size() * BlockSize);
    return checksum.GetChecksum();
}

bool TMirrorPartitionResyncFastPathActor::AllChecksumsReceived() const
{
    return Checksums.size() + 1 >= Replicas.size();
}

void TMirrorPartitionResyncFastPathActor::Reply(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    Y_DEBUG_ABORT_UNLESS(!ReplySent);
    ReplySent = true;

    auto response = std::make_unique<
        TEvNonreplPartitionPrivate::TEvResyncFastPathReadResponse>(error);
    LWTRACK(
        ResponseSent_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "ReadBlocksFastPath",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
}

void TMirrorPartitionResyncFastPathActor::FinishResync(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<
        TEvNonreplPartitionPrivate::TEvResyncFastPathChecksumCompareResponse>(
        error,
        Range);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
}

void TMirrorPartitionResyncFastPathActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    Reply(ctx, error);
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncFastPathActor::HandleChecksumUndelivery(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    const bool dead = MaybeReplyAndDie(
        ctx,
        MakeError(E_REJECTED, "ChecksumBlocks request undelivered"));
    Y_DEBUG_ABORT_UNLESS(dead);
}

void TMirrorPartitionResyncFastPathActor::HandleChecksumResponse(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->Record.GetError())) {
        const bool dead = MaybeReplyAndDie(ctx, msg->Record.GetError());
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
        ReplyAndDie(ctx, msg->Record.GetError());
        return;
    }

    if (Replicas.size() == 1) {
        ReplyAndDie(ctx, {});
        return;
    }

    auto [checksum, error] = CalculateDataChecksum();
    if (HasError(error)) {
        ReplyAndDie(ctx, error);
        return;
    }

    DataChecksum = checksum;
    MaybeCompareChecksums(ctx);
}

void TMirrorPartitionResyncFastPathActor::HandleReadUndelivery(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(
        ctx,
        MakeError(E_REJECTED, "ReadBlocksLocal request undelivered"));
}

void TMirrorPartitionResyncFastPathActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    const bool dead = MaybeReplyAndDie(ctx, MakeError(E_REJECTED, "Dead"));
    Y_DEBUG_ABORT_UNLESS(dead);
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
