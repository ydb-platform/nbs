#include "fresh_blocks_writer_actor.h"

#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TFreshBlocksWriterActor::HandleZeroBlocks(
    const TEvService::TEvZeroBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "ZeroBlocks",
        requestInfo->CallContext->RequestId);

    TBlockRange64 writeRange;

    auto ok = InitReadWriteBlockRange(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount(),
        &writeRange
    );

    if (!ok) {
        auto response =
            std::make_unique<TEvService::TEvZeroBlocksResponse>(MakeError(
                E_ARGUMENT,
                TStringBuilder() << "invalid block range "
                                 << TBlockRange64::WithLength(
                                        msg->Record.GetStartIndex(),
                                        msg->Record.GetBlocksCount())));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo->CallContext->LWOrbit,
            "ZeroBlocks",
            requestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    ui64 commitId = CommitIdsState->GenerateCommitId();
    if (commitId == InvalidCommitId) {
        requestInfo->CancelRequest(ctx);
        RebootOnCommitIdOverflow(ctx, "ZeroBlocks");
        return;
    }

    const auto requestSize = writeRange.Size() * PartitionConfig.GetBlockSize();
    const bool isFreshRequest = IsFreshRequest(
        *Config,
        PartitionConfig.GetStorageMediaKind(),
        requestSize);

    if (!isFreshRequest) {
        ForwardMessageToActor(ev, ctx, PartitionActorId);
        return;
    }

    ZeroFreshBlocks(ctx, requestInfo, ConvertRangeSafe(writeRange), commitId);
}

void TFreshBlocksWriterActor::HandleZeroBlocksCompleted(
    const TEvPartitionCommonPrivate::TEvZeroFreshBlocksCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    ui64 commitId = msg->CommitId;
    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Complete zero blocks @%lu",
        LogTitle.GetWithTime().c_str(),
        commitId);

    UpdateStats(msg->Stats);

    ui64 blocksCount = msg->Stats.GetUserWriteCounters().GetBlocksCount();
    ui64 requestBytes = blocksCount * PartitionConfig.GetBlockSize();

    SharedState->ResourceMetricsQueue.Push(
        NPartition::TUpdateCPUUsageStat{ctx.Now(), msg->ExecCycles});

    auto time = CyclesToDurationSafe(msg->TotalCycles).MicroSeconds();
    SharedState->PartCounters.Access(
        [&](auto& partCounters)
        {
            partCounters->RequestCounters.ZeroBlocks.AddRequest(
                time,
                requestBytes);
        });

    Actors.Erase(ev->Sender);

    Y_DEBUG_ABORT_UNLESS(WriteAndZeroRequestsInProgress > 0);
    --WriteAndZeroRequestsInProgress;
}

}  // namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter
