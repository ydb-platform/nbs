#include "part_actor.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/model/rebuild_metadata.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/guid.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/ymath.h>
#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

using namespace NMonitoringUtils;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleHttpInfo_RebuildMetadata(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    using namespace NMonitoringUtils;

    const auto& batchSizeParam = params.Get("BatchSize");
    ui32 batchSize = 0;
    NProto::TError result;
    if (batchSizeParam) {
        if (!TryFromString(batchSizeParam, batchSize)) {
            result = MakeError(E_ARGUMENT, "BatchSize value is not an integer");
        }
    }

    if (!HasError(result)) {
        result = DoHandleMetadataRebuildBatch(
            ctx,
            NProto::USED_BLOCKS,
            batchSize,
            0);
    }

    auto alertType = EAlertLevel::SUCCESS;
    if (HasError(result)) {
        alertType = EAlertLevel::DANGER;
    }

    SendHttpResponse(ctx, *requestInfo, result.GetMessage(), alertType);
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleRebuildMetadata(
    const TEvVolume::TEvRebuildMetadataRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    auto result = ValidateRebuildMetadataCpuLimit(msg->Record);
    if (!HasError(result)) {
        result = DoHandleMetadataRebuildBatch(
            ctx,
            msg->Record.GetMetadataType(),
            msg->Record.GetBatchSize(),
            msg->Record.GetAllowedCpuTimePerSecond());
    }

    auto response = std::make_unique<TEvVolume::TEvRebuildMetadataResponse>(result);
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TPartitionActor::HandleGetRebuildMetadataStatus(
    const TEvVolume::TEvGetRebuildMetadataStatusRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response = std::make_unique<TEvVolume::TEvGetRebuildMetadataStatusResponse>();
    NProto::TError result;

    if (State) {
        if (State->GetMetadataRebuildType() == EMetadataRebuildType::NoOperation)
        {
            result = MakeError(E_NOT_FOUND, "No operation found");
        } else {
            auto& progress = *response->Record.MutableProgress();
            const auto p = State->GetMetadataRebuildProgress();
            progress.SetProcessed(p.Processed);
            progress.SetTotal(p.Total);
            progress.SetIsCompleted(p.IsCompleted);
        }
    } else {
        result = MakeError(E_REJECTED, "tablet is shutting down");
    };

    *response->Record.MutableError() = std::move(result);
    NCloud::Reply(ctx, *ev, std::move(response));
}

NProto::TError TPartitionActor::DoHandleMetadataRebuildBatch(
    const TActorContext& ctx,
    NProto::ERebuildMetadataType type,
    ui32 batchSize,
    ui64 allowedCpuTimePerSecondMs)
{
    if (State->IsMetadataRebuildStarted()) {
        return MakeError(S_ALREADY, "Metadata rebuild is already running");
    }

    if (!batchSize) {
        return MakeError(E_ARGUMENT, "Batch size is 0");
    }

    switch (type) {
        case NProto::USED_BLOCKS: {
            if (State->GetMetadataRebuildType() == EMetadataRebuildType::UsedBlocks)
            {
                return MakeError(S_ALREADY, "Used blocks are already calculated");
            }
            State->StartRebuildUsedBlocks();

            auto actorId = NCloud::Register(
                ctx,
                CreateMetadataRebuildUsedBlocksActor(
                    SelfId(),
                    static_cast<ui64>(batchSize) * State->GetUsedBlocks().CHUNK_SIZE,
                    State->GetBlocksCount(),
                    Config->GetCompactionRetryTimeout()));

            Actors.Insert(actorId);

            return MakeError(S_OK, "Metadata rebuild(used blocks) has been started");
        }
        case NProto::BLOCK_COUNT: {
            if (State->GetMetadataRebuildType() == EMetadataRebuildType::BlockCount)
            {
                return MakeError(S_ALREADY, "Block count is already calculated");
            }

            if (State->GetCleanupState().Status == EOperationStatus::Started) {
                return MakeError(E_REJECTED, "Cleanup is running");
            }

            return StartMetadataRebuildBlockCount(ctx, batchSize);
        }
        case NProto::COMPACTION_MAP: {
            if (State->GetMetadataRebuildType() ==
                EMetadataRebuildType::CompactionMap)
            {
                return MakeError(
                    S_ALREADY,
                    "Compaction map is already rebuilt");
            }

            if (CompactionMapLoadState) {
                return MakeError(E_REJECTED, "Compaction map is still loading");
            }

            const ui32 totalRangeCount = SafeIntegerCast<ui32>(CeilDiv(
                State->GetBlocksCount(),
                static_cast<ui64>(State->GetCompactionMap().GetRangeSize())));
            State->StartRebuildCompactionMap(totalRangeCount);

            auto actorId = NCloud::Register(
                ctx,
                CreateMetadataRebuildCompactionMapActor(
                    SelfId(),
                    batchSize,
                    totalRangeCount,
                    allowedCpuTimePerSecondMs
                        ? allowedCpuTimePerSecondMs / 1000.0 / SiblingCount
                        : 1.0,
                    Config->GetCompactionRetryTimeout()));

            Actors.Insert(actorId);

            return MakeError(S_OK, "Compaction map rebuild has been started");
        }
        default: {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Unknown metadata type: " << static_cast<ui32>(type));
        }
    }
}

NProto::TError TPartitionActor::StartMetadataRebuildBlockCount(
    const TActorContext& ctx,
    ui32 batchSize)
{
    const ui64 commitId = State->GenerateCommitId();
    if (commitId == InvalidCommitId) {
        RebootPartitionOnCommitIdOverflow(ctx, "MetadataRebuild");
        return MakeError(E_REJECTED, "tablet is shutting down");
    }

    // Block cleanup while waiting for commits, before capturing the counters.
    State->StartRebuildBlockCount();
    State->StartMetadataRebuildCounterTracking(commitId);
    State->AccessCommitQueue()->AcquireBarrier(commitId);
    SharedState->WaitCommit(
        ctx,
        CreateTx<TStartMetadataRebuild>(commitId, batchSize),
        commitId);

    return MakeError(S_OK, "Metadata rebuild has been started");
}

bool TPartitionActor::PrepareStartMetadataRebuild(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TStartMetadataRebuild& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
    return true;
}

void TPartitionActor::ExecuteStartMetadataRebuild(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TStartMetadataRebuild& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteStartMetadataRebuild(
    const TActorContext& ctx,
    TTxPartition::TStartMetadataRebuild& args)
{
    State->AccessCommitQueue()->ReleaseBarrier(args.CommitId);
    ProcessCommitQueue(ctx);

    // Newer commits may already have changed the counters while we waited.
    // Exclude their delta from the baseline, since their blobs are outside
    // the scan and must be preserved by the final counter adjustment.
    const auto delta = State->FinishMetadataRebuildCounterTracking();
    const auto& blobsOutsideScan = delta.Index;
    State->SetRebuildBlockCountTotal(
        blobsOutsideScan.MixedBlobs + blobsOutsideScan.MergedBlobs);

    auto actorId = NCloud::Register(
        ctx,
        CreateMetadataRebuildBlockCountActor(
            SelfId(),
            args.BatchSize,
            args.CommitId - 1,
            State->GetBlobAndBlockCounts() - delta,
            Config->GetCompactionRetryTimeout()));
    Actors.Insert(actorId);
}

void TPartitionActor::HandleMetadataRebuildCompleted(
    const TEvPartitionPrivate::TEvMetadataRebuildCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    if (State && State->IsMetadataRebuildStarted()) {
        const auto type = State->GetMetadataRebuildType();
        State->CompleteMetadataRebuild();
        if (type == EMetadataRebuildType::BlockCount) {
            EnqueueCleanupIfNeeded(ctx);
        } else if (
            type == EMetadataRebuildType::CompactionMap &&
            !HasError(ev->Get()->Error))
        {
            EnqueueCompactionIfNeeded(ctx);
        }
    }
    Actors.Erase(ev->Sender);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
