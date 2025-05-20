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
    } else {
        return std::make_unique<TEvService::TEvWriteBlocksResponse>(
            std::forward<T>(args)...);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TWriteFreshBlocksActor final
    : public TActorBootstrapped<TWriteFreshBlocksActor>
{
public:
    struct TRequest
    {
        TRequestInfoPtr RequestInfo;
        bool ReplyLocal;

        TRequest(TRequestInfoPtr requestInfo, bool replyLocal)
            : RequestInfo(std::move(requestInfo))
            , ReplyLocal(replyLocal)
        {}
    };

private:
    const TActorId PartitionActorId;
    const ui64 CommitId;
    const ui32 Channel;
    const ui32 BlockCount;
    const TVector<TRequest> Requests;
    const TVector<TBlockRange32> BlockRanges;
    const TVector<IWriteBlocksHandlerPtr> WriteHandlers;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;

    TString BlobContent;
    ui64 BlobSize = 0;

    TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;

    TCallContextPtr CombinedContext = MakeIntrusive<TCallContext>();

public:
    TWriteFreshBlocksActor(
        const TActorId& partitionActorId,
        ui64 commitId,
        ui32 channel,
        ui32 blockCount,
        TVector<TRequest> requests,
        TVector<TBlockRange32> blockRanges,
        TVector<IWriteBlocksHandlerPtr> writeHandlers,
        IBlockDigestGeneratorPtr blockDigestGenerator);

    void Bootstrap(const TActorContext& ctx);

private:
    NProto::TError BuildBlobContentAndComputeDigest();

    void WriteBlob(const TActorContext& ctx);
    void AddBlocks(const TActorContext& ctx);

    void NotifyCompleted(const TActorContext& ctx, const NProto::TError& error);
    bool HandleError(const TActorContext& ctx, const NProto::TError& error);
    void ReplyAllAndDie(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleWriteBlobResponse(
        const TEvPartitionPrivate::TEvWriteBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAddFreshBlocksResponse(
        const TEvPartitionPrivate::TEvAddFreshBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

TWriteFreshBlocksActor::TWriteFreshBlocksActor(
        const TActorId& partitionActorId,
        ui64 commitId,
        ui32 channel,
        ui32 blockCount,
        TVector<TRequest> requests,
        TVector<TBlockRange32> blockRanges,
        TVector<IWriteBlocksHandlerPtr> writeHandlers,
        IBlockDigestGeneratorPtr blockDigestGenerator)
    : PartitionActorId(partitionActorId)
    , CommitId(commitId)
    , Channel(channel)
    , BlockCount(blockCount)
    , Requests(std::move(requests))
    , BlockRanges(std::move(blockRanges))
    , WriteHandlers(std::move(writeHandlers))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
{
    Y_ABORT_UNLESS(BlockRanges.size() == WriteHandlers.size());
}

void TWriteFreshBlocksActor::Bootstrap(const TActorContext& ctx)
{
    TDeque<TRequestScope> timers;

    for (const auto& r: Requests) {
        LWTRACK(
            RequestReceived_PartitionWorker,
            r.RequestInfo->CallContext->LWOrbit,
            "WriteFreshBlocks",
            r.RequestInfo->CallContext->RequestId);

        timers.emplace_back(*r.RequestInfo);

        if (!r.RequestInfo->CallContext->LWOrbit.Fork(CombinedContext->LWOrbit)) {
            LWTRACK(
                ForkFailed,
                r.RequestInfo->CallContext->LWOrbit,
                "TEvPartitionPrivate::TEvWriteBlobRequest",
                r.RequestInfo->CallContext->RequestId);
        }
    }

    Become(&TThis::StateWork);

    WriteBlob(ctx);
}

NProto::TError TWriteFreshBlocksActor::BuildBlobContentAndComputeDigest()
{
    TVector<TGuardHolder> holders(Reserve(BlockRanges.size()));

    auto blockRange = BlockRanges.begin();
    auto writeHandler = WriteHandlers.begin();

    while (blockRange != BlockRanges.end()) {
        const auto& holder = holders.emplace_back(
            (**writeHandler).GetBlocks(ConvertRangeSafe(*blockRange)));

        if (!holder.Acquired()) {
            return MakeError(
                E_CANCELLED,
                "failed to acquire sglist in WriteFreshBlocksActor");
        }

        const auto& sgList = holder.GetSgList();

        for (size_t index = 0; index < sgList.size(); ++index) {
            const ui32 blockIndex = blockRange->Start + index;

            const auto digest = BlockDigestGenerator->ComputeDigest(
                blockIndex,
                sgList[index]);

            if (digest.Defined()) {
                AffectedBlockInfos.push_back({blockIndex, *digest});
            }
        }

        ++blockRange;
        ++writeHandler;
    }

    BlobContent = BuildWriteFreshBlocksBlobContent(BlockRanges, holders);
    BlobSize = BlobContent.size();

    return {};
}

void TWriteFreshBlocksActor::WriteBlob(const TActorContext& ctx)
{
    auto error = BuildBlobContentAndComputeDigest();
    if (HandleError(ctx, error)) {
        return;
    }

    Y_ABORT_UNLESS(!BlobContent.empty());

    const auto [generation, step] = ParseCommitId(CommitId);

    TPartialBlobId blobId(
        generation,
        step,
        Channel,
        static_cast<ui32>(BlobContent.size()),
        0,  // cookie
        0   // partId
    );

    auto request = std::make_unique<TEvPartitionPrivate::TEvWriteBlobRequest>(
        CombinedContext,
        blobId,
        std::move(BlobContent),
        0,      // blockSizeForChecksums
        false); // async

    NCloud::Send(
        ctx,
        PartitionActorId,
        std::move(request));
}

void TWriteFreshBlocksActor::AddBlocks(const TActorContext& ctx)
{
    Y_ABORT_UNLESS(BlobSize > 0);

    using TEvent = TEvPartitionPrivate::TEvAddFreshBlocksRequest;
    auto request = std::make_unique<TEvent>(
        CombinedContext,
        CommitId,
        BlobSize,
        std::move(BlockRanges),
        std::move(WriteHandlers));

    NCloud::Send(
        ctx,
        PartitionActorId,
        std::move(request));
}

void TWriteFreshBlocksActor::NotifyCompleted(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    using TEvent = TEvPartitionPrivate::TEvWriteBlocksCompleted;
    auto ev = std::make_unique<TEvent>(
        error,
        false,                      // collectGarbageBarrierAcquired
        false,                      // unconfirmedBlobsAdded
        TVector<TBlobToConfirm>{}   // blobsToConfirm
    );

    ev->ExecCycles = Requests.front().RequestInfo->GetExecCycles();
    ev->TotalCycles = Requests.front().RequestInfo->GetTotalCycles();
    ev->CommitId = CommitId;
    ev->AffectedBlockInfos = std::move(AffectedBlockInfos);

    auto execTime = CyclesToDurationSafe(ev->ExecCycles);
    auto waitTime = CyclesToDurationSafe(Requests.front().RequestInfo->GetWaitCycles());

    auto& counters = *ev->Stats.MutableUserWriteCounters();
    counters.SetRequestsCount(Requests.size());
    counters.SetBatchCount(1);
    counters.SetBlocksCount(BlockCount);
    counters.SetExecTime(execTime.MicroSeconds());
    counters.SetWaitTime(waitTime.MicroSeconds());

    NCloud::Send(ctx, PartitionActorId, std::move(ev));
}

bool TWriteFreshBlocksActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (FAILED(error.GetCode())) {
        ReplyAllAndDie(ctx, error);
        return true;
    }
    return false;
}

void TWriteFreshBlocksActor::ReplyAllAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    NotifyCompleted(ctx, error);

    for (const auto& r: Requests) {
        auto response = CreateWriteBlocksResponse(r.ReplyLocal, error);

        LWTRACK(
            ResponseSent_Partition,
            r.RequestInfo->CallContext->LWOrbit,
            "WriteFreshBlocks",
            r.RequestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *r.RequestInfo, std::move(response));
    }

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TWriteFreshBlocksActor::HandleWriteBlobResponse(
    const TEvPartitionPrivate::TEvWriteBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    for (const auto& r: Requests) {
        r.RequestInfo->AddExecCycles(msg->ExecCycles);
    }

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    AddBlocks(ctx);
}

void TWriteFreshBlocksActor::HandleAddFreshBlocksResponse(
    const TEvPartitionPrivate::TEvAddFreshBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    for (const auto& r: Requests) {
        r.RequestInfo->CallContext->LWOrbit.Join(CombinedContext->LWOrbit);
    }

    ReplyAllAndDie(ctx, {});
}

void TWriteFreshBlocksActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(E_REJECTED, "tablet is shutting down");

    ReplyAllAndDie(ctx, error);
}

STFUNC(TWriteFreshBlocksActor::StateWork)
{
    TDeque<TRequestScope> timers;

    for (const auto& r: Requests) {
        timers.emplace_back(*r.RequestInfo);
    }

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvPartitionPrivate::TEvWriteBlobResponse, HandleWriteBlobResponse);
        HFunc(TEvPartitionPrivate::TEvAddFreshBlocksResponse, HandleAddFreshBlocksResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
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
                MakeError(E_REJECTED,
                          TStringBuilder()
                              << "FreshByteCountHardLimit exceeded: "
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

    State->GetCommitQueue().AcquireBarrier(commitId);

    const bool freshChannelWriteRequestsEnabled =
        Config->GetFreshChannelWriteRequestsEnabled() ||
        Config->IsFreshChannelWriteRequestsFeatureEnabled(
            PartitionConfig.GetCloudId(),
            PartitionConfig.GetFolderId(),
            PartitionConfig.GetDiskId());

    if (freshChannelWriteRequestsEnabled && State->GetFreshChannelCount() > 0) {
        TVector<TWriteFreshBlocksActor::TRequest> requests;
        requests.reserve(requestsInBuffer.size());

        TVector<TBlockRange32> blockRanges;
        blockRanges.reserve(requestsInBuffer.size());

        TVector<IWriteBlocksHandlerPtr> writeHandlers;
        writeHandlers.reserve(requestsInBuffer.size());

        ui32 blockCount = 0;

        for (const auto& r: requestsInBuffer) {
            requests.emplace_back(r.Data.RequestInfo, r.Data.ReplyLocal);

            if (!r.Weight) {
                continue;
            }

            blockCount += r.Weight;

            State->IncrementFreshBlocksInFlight(r.Data.Range.Size());

            blockRanges.push_back(r.Data.Range);
            writeHandlers.push_back(r.Data.Handler);
        }

        State->GetTrimFreshLogBarriers().AcquireBarrierN(commitId, blockCount);

        const ui32 channel = State->PickNextChannel(
            EChannelDataKind::Fresh,
            EChannelPermission::UserWritesAllowed);

        auto actor = NCloud::Register<TWriteFreshBlocksActor>(
            ctx,
            SelfId(),
            commitId,
            channel,
            blockCount,
            std::move(requests),
            std::move(blockRanges),
            std::move(writeHandlers),
            BlockDigestGenerator);

        Actors.Insert(actor);
    } else {
        // write fresh blocks to FreshBlocks table
        TVector<TTxPartition::TWriteBlocks::TSubRequestInfo> subRequests(
            Reserve(requestsInBuffer.size()));

        for (auto& r: requestsInBuffer) {
            LOG_TRACE(ctx, TBlockStoreComponents::PARTITION,
                "[%lu][d:%s] Writing fresh blocks @%lu (range: %s)",
                TabletID(),
                PartitionConfig.GetDiskId().c_str(),
                commitId,
                DescribeRange(r.Data.Range).data()
            );

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
            CreateTx<TWriteBlocks>(commitId, std::move(subRequests)),
            &TransactionTimeTracker);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleAddFreshBlocks(
    const TEvPartitionPrivate::TEvAddFreshBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto blockRange = msg->BlockRanges.begin();
    auto writeHandler = msg->WriteHandlers.begin();

    while (blockRange != msg->BlockRanges.end()) {
        auto guardedSgList = (**writeHandler).GetBlocks(
            ConvertRangeSafe(*blockRange));

        if (auto guard = guardedSgList.Acquire()) {
            const auto& sgList = guard.Get();
            State->WriteFreshBlocks(
                *blockRange,
                msg->CommitId,
                sgList
            );
            State->DecrementFreshBlocksInFlight(blockRange->Size());
        } else {
            LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
                "[" << TabletID() << "]"
                << "Failed to lock a guardedSgList on AddFreshBlocks");
            Suicide(ctx);
            return;
        }

        ++blockRange;
        ++writeHandler;
    }

    State->AddFreshBlob({msg->CommitId, msg->BlobSize});
    State->IncrementUnflushedFreshBlobCount(1);
    State->IncrementUnflushedFreshBlobByteCount(msg->BlobSize);

    // TODO(NBS-1976): update used blocks map

    using TResponse = TEvPartitionPrivate::TEvAddFreshBlocksResponse;
    auto response = std::make_unique<TResponse>();

    NCloud::Reply(ctx, *ev, std::move(response));
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

        State->WriteFreshBlocks(db, sr.Range, commitId, sgList);

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
    LOG_TRACE(ctx, TBlockStoreComponents::PARTITION,
        "[%lu][d:%s] Complete write blocks @%lu",
        TabletID(),
        PartitionConfig.GetDiskId().c_str(),
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

    State->GetCommitQueue().ReleaseBarrier(args.CommitId);
    Y_DEBUG_ABORT_UNLESS(WriteAndZeroRequestsInProgress >= args.Requests.size());
    WriteAndZeroRequestsInProgress -= args.Requests.size();

    EnqueueFlushIfNeeded(ctx);
    DrainActorCompanion.ProcessDrainRequests(ctx);
    ProcessCommitQueue(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
