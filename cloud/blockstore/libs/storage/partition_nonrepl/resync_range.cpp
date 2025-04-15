#include "resync_range.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/disk_agent/public.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <cloud/storage/core/libs/common/sglist.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

TResyncRangeActor::TResyncRangeActor(
        TRequestInfoPtr requestInfo,
        ui32 blockSize,
        TBlockRange64 range,
        TVector<TReplicaDescriptor> replicas,
        TString writerClientId,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        NProto::EResyncPolicy resyncPolicy,
        NActors::TActorId volumeActorId,
        bool assignVolumeRequestId)
    : RequestInfo(std::move(requestInfo))
    , BlockSize(blockSize)
    , Range(range)
    , Replicas(std::move(replicas))
    , WriterClientId(std::move(writerClientId))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , ResyncPolicy(resyncPolicy)
    , VolumeActorId(volumeActorId)
    , AssignVolumeRequestId(assignVolumeRequestId)
{
    using EResyncPolicy = NProto::EResyncPolicy;
    Y_DEBUG_ABORT_UNLESS(
        ResyncPolicy == EResyncPolicy::RESYNC_POLICY_MINOR_4MB ||
        ResyncPolicy == EResyncPolicy::RESYNC_POLICY_MINOR_AND_MAJOR_4MB);
}

void TResyncRangeActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "ResyncRange",
        RequestInfo->CallContext->RequestId);

    ChecksumRangeActorCompanion.CalculateChecksums(ctx, Range);
}

void TResyncRangeActor::GetVolumeRequestId(const NActors::TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        VolumeActorId,
        std::make_unique<TEvVolumePrivate::TEvTakeVolumeRequestIdRequest>());
}

void TResyncRangeActor::CompareChecksums(const TActorContext& ctx)
{
    const auto& checksums = ChecksumRangeActorCompanion.GetChecksums();
    THashMap<ui64, ui32> checksumCount;
    ui32 majorCount = 0;
    ui64 majorChecksum = 0;
    int majorIdx = 0;

    for (size_t i = 0; i < checksums.size(); i++) {
        ui64 checksum = checksums[i];
        if (++checksumCount[checksum] > majorCount) {
            majorCount = checksumCount[checksum];
            majorChecksum = checksum;
            majorIdx = i;
        }
    }

    if (majorCount == Replicas.size()) {
        // all checksums match
        Done(ctx);
        return;
    }

    if (ResyncPolicy == NProto::EResyncPolicy::RESYNC_POLICY_MINOR_4MB &&
        majorCount == 1)
    {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::PARTITION,
            "[%s] Can't resync range %s with major error due to policy "
            "restrictions",
            Replicas[0].ReplicaId.c_str(),
            DescribeRange(Range).c_str());
        Done(ctx);
        return;
    }

    LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Resync range %s: majority replica %lu, checksum %lu, count %u of %u",
        Replicas[0].ReplicaId.c_str(),
        DescribeRange(Range).c_str(),
        majorIdx,
        majorChecksum,
        majorCount,
        Replicas.size());

    for (size_t i = 0; i < checksums.size(); i++) {
        ui64 checksum = checksums[i];
        if (checksum != majorChecksum) {
            LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
                "[%s] Replica %lu block range %s checksum %lu differs from majority checksum %lu",
                Replicas[0].ReplicaId.c_str(),
                Replicas[i].ReplicaIndex,
                DescribeRange(Range).c_str(),
                checksum,
                majorChecksum);

            ActorsToResync.push_back(i);
        }
    }

    ReplicaIndexToReadFrom = majorIdx;
    if (AssignVolumeRequestId) {
        GetVolumeRequestId(ctx);
        return;
    }
    ReadBlocks(ctx);
}

void TResyncRangeActor::ReadBlocks(const TActorContext& ctx)
{
    Buffer = TGuardedBuffer(TString::Uninitialized(Range.Size() * BlockSize));

    auto sgList = Buffer.GetGuardedSgList();
    auto sgListOrError = SgListNormalize(sgList.Acquire().Get(), BlockSize);
    Y_ABORT_UNLESS(!HasError(sgListOrError));
    SgList.SetSgList(sgListOrError.ExtractResult());

    auto request = std::make_unique<TEvService::TEvReadBlocksLocalRequest>();
    request->Record.SetStartIndex(Range.Start);
    request->Record.SetBlocksCount(Range.Size());
    request->Record.BlockSize = BlockSize;
    request->Record.Sglist = SgList;

    auto* headers = request->Record.MutableHeaders();
    headers->SetIsBackgroundRequest(true);
    headers->SetClientId(TString(BackgroundOpsClientId));

    auto event = std::make_unique<NActors::IEventHandle>(
        Replicas[ReplicaIndexToReadFrom].ActorId,
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        ReplicaIndexToReadFrom,   // cookie
        &ctx.SelfID               // forwardOnNondelivery
    );

    ctx.Send(event.release());

    ReadStartTs = ctx.Now();
}

void TResyncRangeActor::WriteBlocks(const TActorContext& ctx) {
    for (int idx: ActorsToResync) {
        WriteReplicaBlocks(ctx, idx);
    }

    WriteStartTs = ctx.Now();
}

void TResyncRangeActor::WriteReplicaBlocks(const TActorContext& ctx, int idx)
{
    auto request = std::make_unique<TEvService::TEvWriteBlocksLocalRequest>();
    request->Record.MutableHeaders()->SetVolumeRequestId(VolumeRequestId);
    request->Record.SetStartIndex(Range.Start);
    auto clientId =
        WriterClientId ? WriterClientId : TString(BackgroundOpsClientId);
    request->Record.BlocksCount = Range.Size();
    request->Record.BlockSize = BlockSize;
    request->Record.Sglist = SgList;

    auto* headers = request->Record.MutableHeaders();
    headers->SetIsBackgroundRequest(true);
    headers->SetClientId(std::move(clientId));

    for (const auto blockIndex: xrange(Range)) {
        auto* data = Buffer.Get().data() + (blockIndex - Range.Start) * BlockSize;

        const auto digest = BlockDigestGenerator->ComputeDigest(
            blockIndex,
            TBlockDataRef(data, BlockSize)
        );

        if (digest.Defined()) {
            AffectedBlockInfos.push_back({blockIndex, *digest});
        }
    }

    auto event = std::make_unique<NActors::IEventHandle>(
        Replicas[idx].ActorId,
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        idx,          // cookie
        &ctx.SelfID   // forwardOnNondelivery
    );

    LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Replica %lu Overwrite block range %s during resync",
        Replicas[idx].ReplicaId.c_str(),
        Replicas[idx].ReplicaIndex,
        DescribeRange(Range).c_str());

    ctx.Send(event.release());
}

void TResyncRangeActor::Done(const TActorContext& ctx)
{
    auto response =
        std::make_unique<TEvNonreplPartitionPrivate::TEvRangeResynced>(
            std::move(Error),
            Range,
            ChecksumRangeActorCompanion.GetChecksumStartTs(),
            ChecksumRangeActorCompanion.GetChecksumDuration(),
            ReadStartTs,
            ReadDuration,
            WriteStartTs,
            WriteDuration,
            std::move(AffectedBlockInfos));

    LWTRACK(
        ResponseSent_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "ResyncRange",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TResyncRangeActor::HandleChecksumUndelivery(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ChecksumRangeActorCompanion.HandleChecksumUndelivery(ctx);
    Error = ChecksumRangeActorCompanion.GetError();

    Done(ctx);
}

void TResyncRangeActor::HandleChecksumResponse(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ChecksumRangeActorCompanion.HandleChecksumResponse(ev, ctx);

    Error = ChecksumRangeActorCompanion.GetError();
    if (HasError(Error)) {
        Done(ctx);
        return;
    }

    if (ChecksumRangeActorCompanion.IsFinished()) {
        CompareChecksums(ctx);
    }
}

void TResyncRangeActor::HandleVolumeRequestId(
    const TEvVolumePrivate::TEvTakeVolumeRequestIdResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    if (HasError(msg->Error)) {
        Error = msg->Error;
        Done(ctx);
        return;
    }

    VolumeRequestId = msg->VolumeRequestId;
    ReadBlocks(ctx);
}

void TResyncRangeActor::HandleReadUndelivery(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ReadDuration = ctx.Now() - ReadStartTs;

    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "ReadBlocks request undelivered");

    Done(ctx);
}

void TResyncRangeActor::HandleReadResponse(
    const TEvService::TEvReadBlocksLocalResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ReadDuration = ctx.Now() - ReadStartTs;

    auto* msg = ev->Get();

    Error = msg->Record.GetError();

    if (HasError(Error)) {
        Done(ctx);
        return;
    }

    WriteBlocks(ctx);
}

void TResyncRangeActor::HandleWriteUndelivery(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    WriteDuration = ctx.Now() - WriteStartTs;

    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "WriteBlocks request undelivered");

    Done(ctx);
}

void TResyncRangeActor::HandleWriteResponse(
    const TEvService::TEvWriteBlocksLocalResponse::TPtr& ev,
    const TActorContext& ctx)
{
    WriteDuration = ctx.Now() - WriteStartTs;

    auto* msg = ev->Get();

    Error = msg->Record.GetError();

    if (HasError(Error)) {
        Done(ctx);
        return;
    }

    if (++ResyncedCount == ActorsToResync.size()) {
        Done(ctx);
    }
}

void TResyncRangeActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "Dead");
    Done(ctx);
}

STFUNC(TResyncRangeActor::StateWork)
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
        HFunc(
            TEvVolumePrivate::TEvTakeVolumeRequestIdResponse,
            HandleVolumeRequestId);
        HFunc(TEvService::TEvReadBlocksLocalRequest, HandleReadUndelivery);
        HFunc(TEvService::TEvReadBlocksLocalResponse, HandleReadResponse);
        HFunc(TEvService::TEvWriteBlocksLocalRequest, HandleWriteUndelivery);
        HFunc(TEvService::TEvWriteBlocksLocalResponse, HandleWriteResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
