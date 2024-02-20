#include "checksum_range.h"

#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/disk_agent/public.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

TChecksumRangeActor::TChecksumRangeActor(
        TRequestInfoPtr requestInfo,
        TBlockRange64 range,
        TVector<TReplicaId> replicas)
    : RequestInfo(std::move(requestInfo))
    , Range(range)
    , Replicas(std::move(replicas))
{
    ActivityType = TBlockStoreActivities::PARTITION_WORKER;
}

void TChecksumRangeActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "ChecksumRange",
        RequestInfo->CallContext->RequestId);

    ChecksumBlocks(ctx);
}

void TChecksumRangeActor::ChecksumBlocks(const TActorContext& ctx) {
    for (size_t i = 0; i < Replicas.size(); ++i) {
        ChecksumReplicaBlocks(ctx, i);
    }

    ChecksumStartTs = ctx.Now();
}

void TChecksumRangeActor::ChecksumReplicaBlocks(const TActorContext& ctx, int idx)
{
    auto request = std::make_unique<TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest>();
    request->Record.SetStartIndex(Range.Start);
    request->Record.SetBlocksCount(Range.Size());

    auto* headers = request->Record.MutableHeaders();
    headers->SetIsBackgroundRequest(true);
    headers->SetClientId(TString(BackgroundOpsClientId));

    auto event = std::make_unique<NActors::IEventHandle>(
        Replicas[idx].ActorId,
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        idx,          // cookie
        &ctx.SelfID   // forwardOnNondelivery
    );

    ctx.Send(event.release());
}

void TChecksumRangeActor::Done(const TActorContext& ctx)
{
    auto response = std::make_unique<TEvNonreplPartitionPrivate::TEvChecksumRangeCompleted>(
        std::move(Error),
        Range,
        ChecksumStartTs,
        ChecksumDuration,
        std::move(Checksums)
    );

    LWTRACK(
        ResponseSent_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "ChecksumRange",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TChecksumRangeActor::HandleChecksumUndelivery(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ChecksumDuration = ctx.Now() - ChecksumStartTs;

    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "ChecksumBlocks request undelivered");

    Done(ctx);
}

void TChecksumRangeActor::HandleChecksumResponse(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    ChecksumDuration = ctx.Now() - ChecksumStartTs;

    auto* msg = ev->Get();

    Error = msg->Record.GetError();

    if (HasError(Error)) {
        LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
            "[%s] Checksum error %s",
            Replicas[0].Name.c_str(),
            FormatError(Error).c_str());

        Done(ctx);
        return;
    }

    Checksums.insert({ev->Cookie, msg->Record.GetChecksum()});
    if (Checksums.size() == Replicas.size()) {
        Done(ctx);
    }
}

void TChecksumRangeActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Error = MakeError(E_REJECTED, "Dead");
    Done(ctx);
}

STFUNC(TChecksumRangeActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest, HandleChecksumUndelivery);
        HFunc(TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse, HandleChecksumResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
