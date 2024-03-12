#include "part_actor.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

#include <util/generic/vector.h>

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

class TWriteMixedBlocksActor final
    : public TActorBootstrapped<TWriteMixedBlocksActor>
{
public:
    struct TRequest
    {
        struct TSubRequest
        {
            const TBlockRange32 WriteRange;
            // an ugly way to mark this range as empty - TBlockRange32 does not
            // support empty ranges
            const bool Empty;
            const TRequestInfoPtr RequestInfo;
            const bool ReplyLocal;
            const TVector<ui32> Checksums;

            TSubRequest(
                    const TBlockRange32 writeRange,
                    const bool empty,
                    TRequestInfoPtr requestInfo,
                    bool replyLocal,
                    TVector<ui32> checksums)
                : WriteRange(writeRange)
                , Empty(empty)
                , RequestInfo(std::move(requestInfo))
                , ReplyLocal(replyLocal)
                , Checksums(std::move(checksums))
            {
            }
        };

        TPartialBlobId BlobId;
        TVector<TSubRequest> SubRequests;
    };

private:
    const TActorId Tablet;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const ui64 CommitId;
    const TVector<TRequest> Requests;
    const IWriteBlocksHandlerPtr WriteHandler;

    TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;
    size_t RequestsCompleted = 0;

    TVector<TCallContextPtr> ForkedCallContexts;
    TCallContextPtr CombinedContext = MakeIntrusive<TCallContext>();

public:
    TWriteMixedBlocksActor(
        const TActorId& tablet,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ui64 commitId,
        TVector<TRequest> requests,
        IWriteBlocksHandlerPtr writeHandler);

    void Bootstrap(const TActorContext& ctx);

private:
    TGuardedSgList BuildBlobContent(const TRequest& request);
    ui32 CalculateSubRequestCount() const;
    TDeque<TRequestScope> BuildTimers();
    void TrackSubRequests();

    void WriteBlobs(const TActorContext& ctx);
    void AddBlobs(const TActorContext& ctx);

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

    void HandleAddBlobsResponse(
        const TEvPartitionPrivate::TEvAddBlobsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

TWriteMixedBlocksActor::TWriteMixedBlocksActor(
        const TActorId& tablet,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ui64 commitId,
        TVector<TRequest> requests,
        IWriteBlocksHandlerPtr writeHandler)
    : Tablet(tablet)
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , CommitId(commitId)
    , Requests(std::move(requests))
    , WriteHandler(std::move(writeHandler))
{}

void TWriteMixedBlocksActor::Bootstrap(const TActorContext& ctx)
{
    auto timers = BuildTimers();
    TrackSubRequests();

    Become(&TThis::StateWork);

    WriteBlobs(ctx);
}

TGuardedSgList TWriteMixedBlocksActor::BuildBlobContent(const TRequest& request)
{
    TVector<TGuardedSgList> result(Reserve(request.SubRequests.size()));

    for (const auto& sr: request.SubRequests) {
        if (!sr.Empty) {
            auto guardedSgList =
                WriteHandler->GetBlocks(ConvertRangeSafe(sr.WriteRange));

            if (auto guard = guardedSgList.Acquire()) {
                const auto& sgList = guard.Get();

                for (size_t index = 0; index < sgList.size(); ++index) {
                    const auto& block = sgList[index];

                    auto blockIndex = sr.WriteRange.Start + index;
                    const auto digest = BlockDigestGenerator->ComputeDigest(
                        blockIndex,
                        block);

                    if (digest.Defined()) {
                        AffectedBlockInfos.push_back({blockIndex, *digest});
                    }
                }
            }

            result.push_back(std::move(guardedSgList));
        }
    }
    return TGuardedSgList::CreateUnion(std::move(result));
}

ui32 TWriteMixedBlocksActor::CalculateSubRequestCount() const
{
    ui32 c = 0;

    for (const auto& r: Requests) {
        c += r.SubRequests.size();
    }

    return c;
}

TDeque<TRequestScope> TWriteMixedBlocksActor::BuildTimers()
{
    TDeque<TRequestScope> timers;

    for (const auto& r: Requests) {
        for (const auto& sr: r.SubRequests) {
            timers.emplace_back(*sr.RequestInfo);
        }
    }

    return timers;
}

void TWriteMixedBlocksActor::TrackSubRequests()
{
    for (const auto& r: Requests) {
        for (const auto& sr: r.SubRequests) {
            LWTRACK(
                RequestReceived_PartitionWorker,
                sr.RequestInfo->CallContext->LWOrbit,
                "WriteMixedBlocks",
                sr.RequestInfo->CallContext->RequestId);
        }
    }
}

void TWriteMixedBlocksActor::WriteBlobs(const TActorContext& ctx)
{
    for (ui32 i = 0; i < Requests.size(); ++i) {
        const auto& req = Requests[i];
        auto guardedSglist = BuildBlobContent(req);

        auto request = std::make_unique<TEvPartitionPrivate::TEvWriteBlobRequest>(
            req.BlobId,
            std::move(guardedSglist));

        for (const auto& sr: req.SubRequests) {
            if (!sr.RequestInfo->CallContext->LWOrbit.Fork(request->CallContext->LWOrbit)) {
                LWTRACK(
                    ForkFailed,
                    sr.RequestInfo->CallContext->LWOrbit,
                    "TEvPartitionPrivate::TEvWriteBlobRequest",
                    sr.RequestInfo->CallContext->RequestId);
            }
        }

        ForkedCallContexts.emplace_back(request->CallContext);

        NCloud::Send(
            ctx,
            Tablet,
            std::move(request),
            i);
    }
}

void TWriteMixedBlocksActor::AddBlobs(const TActorContext& ctx)
{
    TVector<TAddMixedBlob> blobs(Reserve(Requests.size()));

    for (const auto& req: Requests) {
        ui32 blockCount = 0;
        TVector<ui32> blocks(Reserve(blockCount));
        TVector<ui32> checksums(Reserve(blockCount));

        for (const auto& sr: req.SubRequests) {
            if (!sr.Empty) {
                blockCount += sr.WriteRange.Size();

                for (ui32 idx: xrange(sr.WriteRange)) {
                    blocks.push_back(idx);
                }

                for (const auto& checksum: sr.Checksums) {
                    checksums.push_back(checksum);
                }
            }
            if (!sr.RequestInfo->CallContext->LWOrbit.Fork(CombinedContext->LWOrbit)) {
                LWTRACK(
                    ForkFailed,
                    sr.RequestInfo->CallContext->LWOrbit,
                    "TEvPartitionPrivate::TEvWriteBlobRequest",
                    sr.RequestInfo->CallContext->RequestId);
            }
        }

        blobs.emplace_back(req.BlobId, std::move(blocks), std::move(checksums));
    }

    auto request = std::make_unique<TEvPartitionPrivate::TEvAddBlobsRequest>(
        CombinedContext,
        CommitId,
        std::move(blobs),
        TVector<TAddMergedBlob>(),
        TVector<TAddFreshBlob>(),
        ADD_WRITE_RESULT
    );

    NCloud::Send(
        ctx,
        Tablet,
        std::move(request));
}

void TWriteMixedBlocksActor::NotifyCompleted(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    using TEvent = TEvPartitionPrivate::TEvWriteBlocksCompleted;
    auto ev = std::make_unique<TEvent>(
        error,
        true,   // collectGarbageBarrierAcquired
        false); // unconfirmedBlobsAdded

    ui32 blocksCount = 0;
    ui64 waitCycles = 0;

    for (const auto& r: Requests) {
        ev->ExecCycles = Max(
            ev->ExecCycles,
            r.SubRequests.front().RequestInfo->GetExecCycles());
        ev->TotalCycles = Max(
            ev->TotalCycles,
            r.SubRequests.front().RequestInfo->GetTotalCycles());

        for (const auto& sr: r.SubRequests) {
            if (!sr.Empty) {
                blocksCount += sr.WriteRange.Size();
            }
        }

        waitCycles = Max(
            waitCycles,
            r.SubRequests.front().RequestInfo->GetWaitCycles());
    }

    ev->CommitId = CommitId;
    ev->AffectedBlockInfos = std::move(AffectedBlockInfos);

    auto execTime = CyclesToDurationSafe(ev->ExecCycles);
    auto waitTime = CyclesToDurationSafe(waitCycles);

    auto& counters = *ev->Stats.MutableUserWriteCounters();
    counters.SetRequestsCount(CalculateSubRequestCount());
    counters.SetBatchCount(1);
    counters.SetBlocksCount(blocksCount);
    counters.SetExecTime(execTime.MicroSeconds());
    counters.SetWaitTime(waitTime.MicroSeconds());

    NCloud::Send(ctx, Tablet, std::move(ev));
}

bool TWriteMixedBlocksActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (FAILED(error.GetCode())) {
        ReplyAllAndDie(ctx, error);
        return true;
    }
    return false;
}

void TWriteMixedBlocksActor::ReplyAllAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    NotifyCompleted(ctx, error);

    for (const auto& r: Requests) {
        for (const auto& sr: r.SubRequests) {
            auto response = CreateWriteBlocksResponse(sr.ReplyLocal, error);
            Reply(ctx, *sr.RequestInfo, std::move(response));
        }
    }

    Die(ctx);
}

void TWriteMixedBlocksActor::Reply(
    const TActorContext& ctx,
    TRequestInfo& requestInfo,
    IEventBasePtr response)
{
    LWTRACK(
        ResponseSent_Partition,
        requestInfo.CallContext->LWOrbit,
        "WriteMixedBlocks",
        requestInfo.CallContext->RequestId);

    NCloud::Reply(ctx, requestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TWriteMixedBlocksActor::HandleWriteBlobResponse(
    const TEvPartitionPrivate::TEvWriteBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    for (const auto& sr: Requests[ev->Cookie].SubRequests) {
        sr.RequestInfo->AddExecCycles(msg->ExecCycles);
    }

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    Y_ABORT_UNLESS(RequestsCompleted < Requests.size());
    if (++RequestsCompleted < Requests.size()) {
        return;
    }

    for (ui32 i = 0; i < ForkedCallContexts.size(); ++i){
        auto& context = ForkedCallContexts[i];
        for (const auto& sr: Requests[i].SubRequests) {
            sr.RequestInfo->CallContext->LWOrbit.Join(context->LWOrbit);
        }
    }

    AddBlobs(ctx);
}

void TWriteMixedBlocksActor::HandleAddBlobsResponse(
    const TEvPartitionPrivate::TEvAddBlobsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    for (const auto& r: Requests) {
        for (const auto& sr: r.SubRequests) {
            sr.RequestInfo->AddExecCycles(msg->ExecCycles);
            sr.RequestInfo->CallContext->LWOrbit.Join(CombinedContext->LWOrbit);
        }
    }

    const auto& error = msg->GetError();
    if (HandleError(ctx, error)) {
        return;
    }

    ReplyAllAndDie(ctx, error);
}

void TWriteMixedBlocksActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(E_REJECTED, "Tablet is dead");

    ReplyAllAndDie(ctx, error);
}

STFUNC(TWriteMixedBlocksActor::StateWork)
{
    auto timers = BuildTimers();

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvPartitionPrivate::TEvWriteBlobResponse, HandleWriteBlobResponse);
        HFunc(TEvPartitionPrivate::TEvAddBlobsResponse, HandleAddBlobsResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::WriteMixedBlocks(
    const TActorContext& ctx,
    const TVector<TRequestGroup>& groups)
{
    if (groups.empty()) {
        return true;
    }

    TVector<std::pair<IWriteBlocksHandlerPtr, TBlockRange64>> parts;
    TVector<TWriteMixedBlocksActor::TRequest> requests(Reserve(groups.size()));

    const auto commitId = State->GenerateCommitId();
    if (commitId == InvalidCommitId) {
        return false;
    }

    State->GetCommitQueue().AcquireBarrier(commitId);
    State->GetGarbageQueue().AcquireBarrier(commitId);

    for (const auto& group: groups) {
        requests.emplace_back();

        for (const auto* request: group.Requests) {
            if (request->Weight) {
                parts.emplace_back(
                    request->Data.Handler,
                    ConvertRangeSafe(request->Data.Range)
                );
            }

            LOG_TRACE(ctx, TBlockStoreComponents::PARTITION,
                "[%lu] Writing mixed blocks @%lu (range: %s)",
                TabletID(),
                commitId,
                DescribeRange(request->Data.Range).data()
            );

            TVector<ui32> checksums;

            const ui32 checksumBoundary =
                Config->GetDiskPrefixLengthWithBlockChecksumsInBlobs()
                / State->GetBlockSize();
            const bool checksumsEnabled =
                request->Data.Range.Start < checksumBoundary;

            if (checksumsEnabled) {
                auto sgList = request->Data.Handler->GetBlocks(
                    ConvertRangeSafe(request->Data.Range));
                if (auto g = sgList.Acquire()) {
                    for (const auto& blockContent: g.Get()) {
                        checksums.push_back(ComputeDefaultDigest(blockContent));
                    }
                }
            }

            requests.back().SubRequests.emplace_back(
                request->Data.Range,
                !request->Weight,
                request->Data.RequestInfo,
                request->Data.ReplyLocal,
                std::move(checksums)
            );
        }

        requests.back().BlobId = State->GenerateBlobId(
            EChannelDataKind::Mixed,
            EChannelPermission::UserWritesAllowed,
            commitId,
            group.Weight * State->GetBlockSize(),
            requests.size() - 1
        );
    }

    auto writeHandler = CreateMixedWriteBlocksHandler(std::move(parts));

    auto actor = NCloud::Register<TWriteMixedBlocksActor>(
        ctx,
        SelfId(),
        BlockDigestGenerator,
        commitId,
        std::move(requests),
        std::move(writeHandler)
    );
    Actors.Insert(actor);

    return true;
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
