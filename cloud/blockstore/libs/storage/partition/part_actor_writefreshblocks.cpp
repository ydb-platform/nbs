#include "part_actor.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/storage/partition/model/fresh_blob.h>

#include <cloud/storage/core/libs/common/helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename ...T>
IEventBasePtr CreateWriteBlocksResponse(bool replyLocal, T&& ...args)
{
    if (replyLocal) {
        return std::make_unique<TEvService::TEvWriteBlocksLocalResponse>(
            std::forward<T>(args)...);
    }
    return std::make_unique<TEvService::TEvWriteBlocksResponse>(
        std::forward<T>(args)...);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::WriteFreshBlocks(
    const TActorContext& ctx,
    TRequestInBuffer<TWriteBufferRequestData> requestInBuffer)
{
    WriteFreshBlocks(ctx, MakeArrayRef(&requestInBuffer, 1));
}

void TPartitionActor::WriteFreshBlocks(
    const TActorContext& ctx,
    TArrayRef<TRequestInBuffer<TWriteBufferRequestData>> requestsInBuffer)
{
    if (requestsInBuffer.empty()) {
        return;
    }

    if (State->GetUnflushedFreshBlobByteCount()
            >= Config->GetFreshByteCountHardLimit())
    {
        for (auto& r: requestsInBuffer) {
            ui32 flags = 0;
            SetProtoFlag(flags, NProto::EF_SILENT);
            auto response = CreateWriteBlocksResponse(
                r.Data.ReplyLocal,
                MakeError(
                    E_REJECTED,
                    TStringBuilder() << "FreshByteCountHardLimit exceeded: "
                                     << State->GetUnflushedFreshBlobByteCount(),
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

    const auto commitId = State->GenerateCommitId();

    if (commitId == InvalidCommitId) {
        for (auto& r: requestsInBuffer) {
            r.Data.RequestInfo->CancelRequest(ctx);
        }
        RebootPartitionOnCommitIdOverflow(ctx, "WriteFreshBlocks");

        return;
    }

    State->AccessCommitQueue().AcquireBarrier(commitId);

    const bool freshChannelWriteRequestsEnabled =
        Config->GetFreshChannelWriteRequestsEnabled() ||
        Config->IsFreshChannelWriteRequestsFeatureEnabled(
            PartitionConfig.GetCloudId(),
            PartitionConfig.GetFolderId(),
            PartitionConfig.GetDiskId());

    if (freshChannelWriteRequestsEnabled && State->GetFreshChannelCount() > 0) {
        FreshBlocksCompanion->WriteFreshBlocks(ctx, requestsInBuffer, commitId);
    } else {
        // write fresh blocks to FreshBlocks table
        TVector<TTxPartition::TWriteBlocks::TSubRequestInfo> subRequests(
            Reserve(requestsInBuffer.size()));

        for (auto& r: requestsInBuffer) {
            LOG_TRACE(
                ctx,
                TBlockStoreComponents::PARTITION,
                "%s Writing fresh blocks @%lu (range: %s)",
                LogTitle.GetWithTime().c_str(),
                commitId,
                DescribeRange(r.Data.Range).c_str());

            AddTransaction(
                *r.Data.RequestInfo,
                r.Data.RequestInfo->CancelRoutine);

            subRequests.emplace_back(
                std::move(r.Data.RequestInfo),
                r.Data.Range,
                std::move(r.Data.Handler),
                !r.Weight,
                r.Data.ReplyLocal
            );

            if (r.Weight) {
                State->IncrementFreshBlocksInFlight(r.Data.Range.Size());
            }
        }

        ExecuteTx(
            ctx,
            CreateTx<TWriteBlocks>(commitId, std::move(subRequests)));
    }
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleAddFreshBlocks(
    const TEvPartitionCommonPrivate::TEvAddFreshBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    FreshBlocksCompanion->HandleAddFreshBlocks(ev, ctx);
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareWriteBlocks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TWriteBlocks& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    // we really want to keep the writes blind
    return true;
}

void TPartitionActor::ExecuteWriteBlocks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TWriteBlocks& args)
{
    Y_UNUSED(ctx);

    TDeque<TRequestScope> timers;

    for (const auto& sr: args.Requests) {
        timers.emplace_back(*sr.RequestInfo);
    }

    TVector<std::unique_ptr<TGuardHolder>> guardHolders(
        Reserve(args.Requests.size()));

    for (const auto& sr: args.Requests) {
        auto guardedSgList = sr.WriteHandler->GetBlocks(ConvertRangeSafe(sr.Range));
        auto holder = std::make_unique<TGuardHolder>(std::move(guardedSgList));
        if (holder->Acquired()) {
            guardHolders.push_back(std::move(holder));
        } else {
            args.Interrupted = true;
            return;
        }
    }

    TPartitionDatabase db(tx.DB);

    for (ui32 i = 0; i < args.Requests.size(); ++i) {
        const auto& sr = args.Requests[i];
        const auto& sgList = guardHolders[i]->GetSgList();

        if (sr.Empty) {
            continue;
        }

        ui64 commitId = args.CommitId;

        for (size_t index = 0; index < sgList.size(); ++index) {
            const auto& blockContent = sgList[index];
            Y_ABORT_UNLESS(blockContent.Size() == State->GetBlockSize());

            ui32 blockIndex = sr.Range.Start + index;

            const auto digest = BlockDigestGenerator->ComputeDigest(
                blockIndex,
                blockContent);

            if (digest.Defined()) {
                args.AffectedBlockInfos.push_back({blockIndex, *digest});
            }
        }

        State->WriteFreshBlocksToDb(db, sr.Range, commitId, sgList);

        // update counters
        State->DecrementFreshBlocksInFlight(sr.Range.Size());

        State->SetUsedBlocks(db, sr.Range, 0);
    }

    db.WriteMeta(State->GetMeta());
}

void TPartitionActor::CompleteWriteBlocks(
    const TActorContext& ctx,
    TTxPartition::TWriteBlocks& args)
{
    ui32 blockCount = 0;
    ui64 totalBytes = 0;
    ui64 execCycles = 0;
    ui64 waitCycles = 0;

    ui64 commitId = args.CommitId;
    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Complete WriteBlocks transaction @%lu",
        LogTitle.GetWithTime().c_str(),
        commitId);

    if (args.Requests.size()) {
        ui64 startCycles = GetCycleCount();

        for (const auto& sr: args.Requests) {
            RemoveTransaction(*sr.RequestInfo);

            NProto::TError error;
            if (args.Interrupted) {
                error = MakeError(E_REJECTED, "WriteBlocks transaction was interrupted");
            }

            auto response = CreateWriteBlocksResponse(sr.ReplyLocal, error);

            LWTRACK(
                ResponseSent_Partition,
                sr.RequestInfo->CallContext->LWOrbit,
                "WriteBlocks",
                sr.RequestInfo->CallContext->RequestId);

            NCloud::Reply(ctx, *sr.RequestInfo, std::move(response));

            ui64 requestBytes = 0;
            if (!sr.Empty) {
                requestBytes = static_cast<ui64>(sr.Range.Size()) * State->GetBlockSize();
                totalBytes += requestBytes;
                blockCount += sr.Range.Size();
            }
        }

        // all subrequests have the same exec and wait time since we
        // handle them together. So we only need to report times
        // for first subrequest.
        auto cycles = GetCycleCount() - startCycles;
        auto time =
            CyclesToDurationSafe(args.Requests[0].RequestInfo->GetTotalCycles()).MicroSeconds();
        PartCounters->RequestCounters.WriteBlocks.AddRequest(time, totalBytes, args.Requests.size());

        execCycles += args.Requests[0].RequestInfo->GetExecCycles() + cycles;
        waitCycles += args.Requests[0].RequestInfo->GetWaitCycles();

        UpdateCPUUsageStat(ctx.Now(), execCycles);
        UpdateNetworkStat(ctx.Now(), totalBytes);
    }

    NProto::TPartitionStats stats;
    {
        auto execTime = CyclesToDurationSafe(execCycles);
        auto waitTime = CyclesToDurationSafe(waitCycles);

        auto& counters = *stats.MutableUserWriteCounters();
        counters.SetRequestsCount(args.Requests.size());
        counters.SetBatchCount(1);
        counters.SetBlocksCount(blockCount);
        counters.SetExecTime(execTime.MicroSeconds());
        counters.SetWaitTime(waitTime.MicroSeconds());
    }
    UpdateStats(stats);

    if (args.AffectedBlockInfos) {
        IProfileLog::TReadWriteRequestBlockInfos request;
        request.RequestType = EBlockStoreRequest::WriteBlocks;
        request.BlockInfos = std::move(args.AffectedBlockInfos);
        request.CommitId = commitId;

        IProfileLog::TRecord record;
        record.DiskId = State->GetConfig().GetDiskId();
        record.Ts = ctx.Now();
        record.Request = std::move(request);

        ProfileLog->Write(std::move(record));
    }

    State->AccessCommitQueue().ReleaseBarrier(args.CommitId);
    Y_DEBUG_ABORT_UNLESS(WriteAndZeroRequestsInProgress >= args.Requests.size());
    WriteAndZeroRequestsInProgress -= args.Requests.size();

    EnqueueFlushIfNeeded(ctx);
    DrainActorCompanion.ProcessDrainRequests(ctx);
    ProcessCommitQueue(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::ZeroFreshBlocks(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    TBlockRange32 writeRange,
    ui64 commitId)
{
    const bool freshChannelZeroRequestsEnabled =
        Config->GetFreshChannelZeroRequestsEnabled();

    const ui32 blockCount = writeRange.Size();
    State->IncrementFreshBlocksInFlight(blockCount);

    if (freshChannelZeroRequestsEnabled && State->GetFreshChannelCount() > 0) {
        FreshBlocksCompanion
            ->ZeroFreshBlocks(ctx, requestInfo, writeRange, commitId);
    } else {
        AddTransaction<TEvService::TZeroBlocksMethod>(*requestInfo);

        auto tx = CreateTx<TZeroBlocks>(requestInfo, commitId, writeRange);

        ExecuteTx(ctx, std::move(tx));
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
