#include "fresh_blocks_writer_actor.h"

#include <cloud/blockstore/libs/storage/partition_common/actor_writefreshblocks.h>

#include <cloud/storage/core/libs/common/helpers.h>

namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter {

using namespace NActors;

using namespace NKikimr;

using namespace NPartition;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TFreshBlocksWriterActor::WriteFreshBlocks(
    const TActorContext& ctx,
    TRequestInBuffer<TWriteBufferRequestData> requestInBuffer)
{
    WriteFreshBlocks(ctx, MakeArrayRef(&requestInBuffer, 1));
}

void TFreshBlocksWriterActor::WriteFreshBlocks(
    const TActorContext& ctx,
    TArrayRef<TRequestInBuffer<TWriteBufferRequestData>> requestsInBuffer)
{
    if (requestsInBuffer.empty()) {
        return;
    }

    if (SharedState->UnflushedFreshBlobByteCount.load() >=
        Config->GetFreshByteCountHardLimit())
    {
        for (auto& r: requestsInBuffer) {
            ui32 flags = 0;
            SetProtoFlag(flags, NProto::EF_SILENT);
            auto response = CreateWriteBlocksResponse(
                r.Data.ReplyLocal,
                MakeError(
                    E_REJECTED,
                    TStringBuilder() << "FreshByteCountHardLimit exceeded: "
                                     << FlushState->GetUnflushedFreshBlobByteCount(),
                    flags));

            LWTRACK(
                ResponseSent_Partition,
                r.Data.RequestInfo->CallContext->LWOrbit,
                "WriteBlocks",
                r.Data.RequestInfo->CallContext->RequestId);

            NCloud::Reply(ctx, *r.Data.RequestInfo, std::move(response));
        }

        return;
    }

    const auto commitId = CommitIdsState->GenerateCommitId();

    if (commitId == InvalidCommitId) {
        for (auto& r: requestsInBuffer) {
            r.Data.RequestInfo->CancelRequest(ctx);
        }
        RebootOnCommitIdOverflow(ctx, "WriteFreshBlocks");

        return;
    }

    const bool freshChannelWriteRequestsEnabled =
        Config->GetFreshChannelWriteRequestsEnabled() ||
        Config->IsFreshChannelWriteRequestsFeatureEnabled(
            PartitionConfig.GetCloudId(),
            PartitionConfig.GetFolderId(),
            PartitionConfig.GetDiskId());

    STORAGE_VERIFY_C(
        freshChannelWriteRequestsEnabled &&
            ChannelsState->GetFreshChannelCount() > 0,
        TWellKnownEntityTypes::TABLET,
        PartitionTabletID,
        "Fresh channels write requests are not enabled");

    TVector<TWriteFreshBlocksActor::TRequest> requests;
    requests.reserve(requestsInBuffer.size());

    TVector<TBlockRange32> blockRanges;
    blockRanges.reserve(requestsInBuffer.size());

    TVector<IWriteBlocksHandlerPtr> writeHandlers;
    writeHandlers.reserve(requestsInBuffer.size());

    ui32 blockCount = 0;

    for (const auto& r: requestsInBuffer) {
        requests.emplace_back(
            r.Data.RequestInfo,
            r.Data.ReplyLocal ? EFreshRequestType::WriteBlocksLocal
                              : EFreshRequestType::WriteBlocks);

        if (!r.Weight) {
            continue;
        }

        blockCount += r.Weight;

        FlushState->IncrementFreshBlocksInFlight(r.Data.Range.Size());

        blockRanges.push_back(r.Data.Range);
        writeHandlers.push_back(r.Data.Handler);
    }

    const ui32 channel = ChannelsState->PickNextChannel(
        EChannelDataKind::Fresh,
        EChannelPermission::UserWritesAllowed);

    auto actor = NCloud::Register<TWriteFreshBlocksActor>(
        ctx,
        SelfId(),
        PartitionActorId,
        commitId,
        channel,
        blockCount,
        std::move(requests),
        std::move(blockRanges),
        std::move(writeHandlers),
        BlockDigestGenerator,
        PartitionTabletID,
        false,   // waitForAddFreshBlocksResponseBeforeResponse
        SharedState);

    Actors.Insert(actor);
}

void TFreshBlocksWriterActor::ZeroFreshBlocks(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    TBlockRange32 writeRange,
    ui64 commitId)
{
    if (FlushState->GetUnflushedFreshBlobByteCount() >=
        Config->GetFreshByteCountHardLimit())
    {
        ui32 flags = 0;
        SetProtoFlag(flags, NProto::EF_SILENT);
        auto response =
            std::make_unique<TEvService::TEvZeroBlocksResponse>(MakeError(
                E_REJECTED,
                TStringBuilder() << "FreshByteCountHardLimit exceeded: "
                                 << FlushState->GetUnflushedFreshBlobByteCount(),
                flags));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo->CallContext->LWOrbit,
            "ZeroBlocks",
            requestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    ++WriteAndZeroRequestsInProgress;

    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Start zero blocks @%lu (range: %s)",
        LogTitle.GetWithTime().c_str(),
        commitId,
        DescribeRange(writeRange).c_str());

    const bool freshChannelZeroRequestsEnabled =
        Config->GetFreshChannelZeroRequestsEnabled();

    const ui32 blockCount = writeRange.Size();
    FlushState->IncrementFreshBlocksInFlight(blockCount);

    STORAGE_VERIFY_C(
        freshChannelZeroRequestsEnabled && ChannelsState->GetFreshChannelCount() > 0,
        TWellKnownEntityTypes::TABLET,
        PartitionTabletID,
        "Fresh channels write requests are not enabled");

    TVector<TWriteFreshBlocksActor::TRequest> requests;
    TVector<TBlockRange32> blockRanges;

    requests.emplace_back(requestInfo, EFreshRequestType::ZeroBlocks);
    blockRanges.emplace_back(writeRange);

    const ui32 channel = ChannelsState->PickNextChannel(
        EChannelDataKind::Fresh,
        EChannelPermission::UserWritesAllowed);

    auto actor = NCloud::Register<TWriteFreshBlocksActor>(
        ctx,
        SelfId(),
        PartitionActorId,
        commitId,
        channel,
        blockCount,
        std::move(requests),
        std::move(blockRanges),
        TVector<IWriteBlocksHandlerPtr>{},
        BlockDigestGenerator,
        PartitionTabletID,
        false,   // waitForAddFreshBlocksResponseBeforeResponse
        SharedState);

    Actors.Insert(actor);
}

}   // namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter
