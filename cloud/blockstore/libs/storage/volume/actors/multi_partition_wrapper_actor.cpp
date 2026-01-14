
#include "multi_partition_wrapper_actor.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/storage/api/service.h>

#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/library/actors/core/log.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TBriefPartitionInfoList MakePartitions(
    const TString& diskId,
    ui32 blockSize,
    const TVector<NActors::TActorId>& partitionActors)
{
    TBriefPartitionInfoList partitions;
    for (const auto& partitionActor: partitionActors) {
        partitions.push_back(
            TBriefPartitionInfo{
                .ActorId = partitionActor,
                .BlockSize = blockSize,
                .DiskId = diskId});
    }
    return partitions;
}

}   // namespace

TMultiPartitionWrapperActor::TMultiPartitionWrapperActor(
    TChildLogTitle logTitle,
    ITraceSerializerPtr traceSerializer,
    const TString& diskId,
    ui32 blockSize,
    ui32 blocksPerStripe,
    const TVector<NActors::TActorId>& partitionActors)
    : LogTitle(std::move(logTitle))
    , TraceSerializer(std::move(traceSerializer))
    , BlockSize(blockSize)
    , BlocksPerStripe(blocksPerStripe)
    , Partitions(MakePartitions(diskId, blockSize, partitionActors))
{}

void TMultiPartitionWrapperActor::Bootstrap(const NActors::TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s MultiPartitionWrapper actor created",
        LogTitle.GetWithTime().c_str());
}

void TMultiPartitionWrapperActor::StateWork(
    TAutoPtr<::NActors::IEventHandle>& ev)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvService::TEvReadBlocksRequest,
            HandleRequest<TEvService::TReadBlocksMethod>);
        HFunc(
            TEvService::TEvReadBlocksLocalRequest,
            HandleRequest<TEvService::TReadBlocksLocalMethod>);
        HFunc(
            TEvService::TEvWriteBlocksRequest,
            HandleRequest<TEvService::TWriteBlocksMethod>);
        HFunc(
            TEvService::TEvWriteBlocksLocalRequest,
            HandleRequest<TEvService::TWriteBlocksLocalMethod>);
        HFunc(
            TEvService::TEvZeroBlocksRequest,
            HandleRequest<TEvService::TZeroBlocksMethod>);
        HFunc(
            TEvVolume::TEvDescribeBlocksRequest,
            HandleRequest<TEvVolume::TDescribeBlocksMethod>);
        HFunc(
            TEvService::TEvGetChangedBlocksRequest,
            HandleRequest<TEvService::TGetChangedBlocksMethod>);
        HFunc(
            TEvVolume::TEvGetPartitionInfoRequest,
            HandleRequest<TEvVolume::TGetPartitionInfoMethod>);
        HFunc(
            TEvVolume::TEvCompactRangeRequest,
            HandleRequest<TEvVolume::TCompactRangeMethod>);
        HFunc(
            TEvVolume::TEvGetCompactionStatusRequest,
            HandleRequest<TEvVolume::TGetCompactionStatusMethod>);
        HFunc(
            TEvVolume::TEvRebuildMetadataRequest,
            HandleRequest<TEvVolume::TRebuildMetadataMethod>);
        HFunc(
            TEvVolume::TEvGetRebuildMetadataStatusRequest,
            HandleRequest<TEvVolume::TGetRebuildMetadataStatusMethod>);
        HFunc(
            TEvVolume::TEvScanDiskRequest,
            HandleRequest<TEvVolume::TScanDiskMethod>);
        HFunc(
            TEvVolume::TEvGetScanDiskStatusRequest,
            HandleRequest<TEvVolume::TGetScanDiskStatusMethod>);
        HFunc(
            TEvVolume::TEvGetUsedBlocksRequest,
            HandleRequest<TEvVolume::TGetUsedBlocksMethod>);
        HFunc(
            TEvVolume::TEvCheckRangeRequest,
            HandleRequest<TEvVolume::TCheckRangeMethod>);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME,
                __PRETTY_FUNCTION__);
            break;
    }
}

template <typename TMethod>
void TMultiPartitionWrapperActor::HandleRequest(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();

    TVector<TPartitionRequest<TMethod>> partitionRequests;
    TBlockRange64 blockRange;

    const bool ok = ToPartitionRequests<TMethod>(
        Partitions,
        BlockSize,
        BlocksPerStripe,
        ev,
        &partitionRequests,
        &blockRange);

    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s HandleMultiPartitionRequest %s %s",
        LogTitle.GetWithTime().c_str(),
        TMethod::Name,
        blockRange.Print().c_str());

    if (!ok) {
        auto response = std::make_unique<typename TMethod::TResponse>(
            MakeError(E_REJECTED, "Sglist destroyed"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    const bool isCrossPartitionRequest =
        partitionRequests.size() > 1 || IsDescribeBlocksMethod<TMethod>;

    if (!isCrossPartitionRequest) {
        auto newEvent = typename TMethod::TRequest::TPtr(
            static_cast<typename TMethod::TRequest::THandle*>(new IEventHandle(
                partitionRequests.front().ActorId,
                ev->Sender,   // The response is sent directly to the volume
                              // actor, bypassing the wrapper.
                partitionRequests.front().Event.release(),
                IEventHandle::FlagForwardOnNondelivery,   // flags
                ev->Cookie,                               // cookie
                &ev->Sender   // The non-delivery error of the request is also
                              // handled by the volume actor
                )));

        ctx.Send(newEvent.Release());
        return;
    }

    for (const auto& partitionRequest: partitionRequests) {
        LOG_TRACE(
            ctx,
            TBlockStoreComponents::VOLUME,
            "%s Forward %s request to partition: %u (%s)",
            LogTitle.GetWithTime().c_str(),
            TMethod::Name,
            partitionRequest.PartitionId,
            ToString(partitionRequest.ActorId).data());
    }

    const bool isTraced = TraceSerializer->IsTraced(msg->CallContext->LWOrbit);
    const ui64 traceTs =
        isTraced ? msg->Record.GetHeaders().GetInternal().GetTraceTs() : 0;

    NCloud::Register<TMultiPartitionRequestActor<TMethod>>(
        ctx,
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext),
        blockRange,
        BlocksPerStripe,
        BlockSize,
        Partitions.size(),
        std::move(partitionRequests),
        TRequestTraceInfo(isTraced, traceTs, TraceSerializer));
}

void TMultiPartitionWrapperActor::HandlePoisonPill(
    const NActors::TEvents::TEvPoisonPill::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    NCloud::Reply(ctx, *ev, std::make_unique<TEvents::TEvPoisonTaken>());
    Die(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
