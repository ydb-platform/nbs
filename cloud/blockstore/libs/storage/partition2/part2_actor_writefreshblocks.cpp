#include "part2_actor.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/storage/partition2/model/fresh_blob.h>

#include <cloud/storage/core/libs/common/helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

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
    TVector<TBlockRange32> BlockRanges;
    TVector<TBlockRange32> UninitializedBlockRanges;
    TVector<IWriteBlocksHandlerPtr> WriteHandlers;
    const ui64 FirstRequestDeletionId;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;

    TString BlobContent;

    TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;

    bool IsIndexInitialized = false;
    bool IsBlobWritten = false;

    TCallContextPtr CombinedContext = MakeIntrusive<TCallContext>();

public:
    TWriteFreshBlocksActor(
        const TActorId& partitionActorId,
        ui64 commitId,
        ui32 channel,
        ui32 blockCount,
        TVector<TRequest> requests,
        TVector<TBlockRange32> blockRanges,
        TVector<TBlockRange32> uninitializedBlockRanges,
        TVector<IWriteBlocksHandlerPtr> writeHandlers,
        ui64 firstRequestDeletionId,
        IBlockDigestGeneratorPtr blockDigestGenerator);

    void Bootstrap(const TActorContext& ctx);

private:
    NProto::TError BuildBlobContentAndComputeDigest();

    void WriteBlob(const TActorContext& ctx);
    void InitIndex(const TActorContext& ctx);
    void AddBlocks(const TActorContext& ctx);

    void NotifyCompleted(const TActorContext& ctx, const NProto::TError& error);
    bool HandleError(const TActorContext& ctx, const NProto::TError& error);
    void ReplyAllAndDie(const TActorContext& ctx, const NProto::TError& error);

    void Reply(
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        IEventBasePtr response);

private:
    STFUNC(StateWork);

    void HandleWriteBlobResponse(
        const TEvPartitionPrivate::TEvWriteBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleInitIndexResponse(
        const TEvPartitionPrivate::TEvInitIndexResponse::TPtr& ev,
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
        TVector<TBlockRange32> uninitializedBlockRanges,
        TVector<IWriteBlocksHandlerPtr> writeHandlers,
        ui64 firstRequestDeletionId,
        IBlockDigestGeneratorPtr blockDigestGenerator)
    : PartitionActorId(partitionActorId)
    , CommitId(commitId)
    , Channel(channel)
    , BlockCount(blockCount)
    , Requests(std::move(requests))
    , BlockRanges(std::move(blockRanges))
    , UninitializedBlockRanges(std::move(uninitializedBlockRanges))
    , WriteHandlers(std::move(writeHandlers))
    , FirstRequestDeletionId(firstRequestDeletionId)
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
    InitIndex(ctx);
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

    BlobContent = BuildFreshBlobContent(
        BlockRanges,
        holders,
        FirstRequestDeletionId);

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
        false);  // async

    NCloud::Send(
        ctx,
        PartitionActorId,
        std::move(request));
}

void TWriteFreshBlocksActor::InitIndex(const TActorContext& ctx)
{
    if (UninitializedBlockRanges.empty()) {
        IsIndexInitialized = true;
        return;
    }

    auto request = std::make_unique<TEvPartitionPrivate::TEvInitIndexRequest>(
        std::move(UninitializedBlockRanges));

    NCloud::Send(ctx, PartitionActorId, std::move(request));
}

void TWriteFreshBlocksActor::AddBlocks(const TActorContext& ctx)
{
    using TEvent = TEvPartitionPrivate::TEvAddFreshBlocksRequest;
    auto request = std::make_unique<TEvent>(
        CommitId,
        std::move(BlockRanges),
        std::move(WriteHandlers),
        FirstRequestDeletionId);

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
        false);  // collectBarrierAcquired

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
        Reply(ctx, *r.RequestInfo, std::move(response));
    }

    Die(ctx);
}

void TWriteFreshBlocksActor::Reply(
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    IEventBasePtr response)
{
    LWTRACK(
        ResponseSent_Partition,
        requestInfo.CallContext->LWOrbit,
        "WriteFreshBlocks",
        requestInfo.CallContext->RequestId);

    NCloud::Reply(ctx, requestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TWriteFreshBlocksActor::HandleWriteBlobResponse(
    const TEvPartitionPrivate::TEvWriteBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    for (const auto& r: Requests) {
        r.RequestInfo->AddExecCycles(msg->ExecCycles);
    }

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    IsBlobWritten = true;

    if (IsIndexInitialized) {
        AddBlocks(ctx);
    }
}

void TWriteFreshBlocksActor::HandleInitIndexResponse(
    const TEvPartitionPrivate::TEvInitIndexResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Y_ABORT_UNLESS(!IsIndexInitialized);
    IsIndexInitialized = true;

    if (IsBlobWritten) {
        AddBlocks(ctx);
    }
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
        HFunc(TEvPartitionPrivate::TEvInitIndexResponse, HandleInitIndexResponse);

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

    const auto freshByteCount =
        State->GetFreshBlockCount() * State->GetBlockSize();
    if (freshByteCount >= Config->GetFreshByteCountHardLimit()) {
        for (auto& r: requestsInBuffer) {
            ui32 flags = 0;
            SetProtoFlag(flags, NProto::EF_SILENT);
            auto response = CreateWriteBlocksResponse(
                r.Data.ReplyLocal,
                MakeError(
                    E_REJECTED,
                    TStringBuilder() << "FreshByteCountHardLimit exceeded: "
                                     << freshByteCount,
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

    const ui64 commitId = State->GenerateCommitId();
    if (commitId == InvalidCommitId) {
        for (auto& r: requestsInBuffer) {
            r.Data.RequestInfo->CancelRequest(ctx);
        }
        RebootPartitionOnCommitIdOverflow(ctx, "WriteFreshBlocks");
        return;
    }

    TVector<TWriteFreshBlocksActor::TRequest> requests;
    requests.reserve(requestsInBuffer.size());

    TVector<TBlockRange32> blockRanges;
    blockRanges.reserve(requestsInBuffer.size());

    TVector<TBlockRange32> uninitializedBlockRanges;

    TVector<IWriteBlocksHandlerPtr> writeHandlers;
    writeHandlers.reserve(requestsInBuffer.size());

    ui32 blockCount = 0;
    ui64 firstRequestDeletionId = State->NextDeletionId();

    for (auto& r: requestsInBuffer) {
        requests.emplace_back(r.Data.RequestInfo, r.Data.ReplyLocal);

        if (!r.Weight) {
            continue;
        }

        blockCount += r.Weight;

        Y_DEBUG_ABORT_UNLESS(r.Weight == r.Data.Range.Size());
        State->AddFreshBlocksInFlight(r.Data.Range, commitId);

        blockRanges.push_back(r.Data.Range);
        writeHandlers.push_back(r.Data.Handler);

        if (!State->IsIndexInitialized(r.Data.Range)) {
            uninitializedBlockRanges.push_back(r.Data.Range);
        }

        State->NextDeletionId();
    }

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
        std::move(uninitializedBlockRanges),
        std::move(writeHandlers),
        firstRequestDeletionId,
        BlockDigestGenerator);

    Actors.insert(actor);
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleAddFreshBlocks(
    const TEvPartitionPrivate::TEvAddFreshBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto blockRange = msg->BlockRanges.begin();
    auto writeHandler = msg->WriteHandlers.begin();
    auto deletionId = msg->FirstRequestDeletionId;

    while (blockRange != msg->BlockRanges.end()) {
        auto guardedSgList = (**writeHandler).GetBlocks(
            ConvertRangeSafe(*blockRange));

        if (auto guard = guardedSgList.Acquire()) {
            const auto& sgList = guard.Get();
            for (const auto idx: xrange(*blockRange)) {
                TBlock block(idx, msg->CommitId, InvalidCommitId, false);
                State->WriteFreshBlock(block, sgList[idx - blockRange->Start]);
            }
        } else {
            LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION,
                "[" << TabletID() << "]"
                << "Failed to lock a guardedSgList on AddFreshBlocks");
            Suicide(ctx);
            return;
        }

        State->RemoveFreshBlocksInFlight(*blockRange, msg->CommitId);

        State->AddBlobUpdateByFresh({
            *blockRange,
            msg->CommitId,
            deletionId});

        ++blockRange;
        ++writeHandler;
        ++deletionId;
    }

    using TResponse = TEvPartitionPrivate::TEvAddFreshBlocksResponse;
    auto response = std::make_unique<TResponse>();

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
