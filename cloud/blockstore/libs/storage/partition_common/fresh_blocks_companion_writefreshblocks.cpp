#include "fresh_blocks_companion.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/partition/model/fresh_blob.h>

#include <cloud/storage/core/libs/common/helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

enum class ERequestType
{
    WriteBlocks,
    WriteBlocksLocal,
    ZeroBlocks
};

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
        ERequestType RequestType;

        TRequest(TRequestInfoPtr requestInfo, ERequestType requestType)
            : RequestInfo(std::move(requestInfo))
            , RequestType(requestType)
        {}
    };

private:
    const TActorId PartitionActorId;
    const ui64 CommitId;
    const ui32 Channel;
    const ui32 BlockCount;
    const TVector<TRequest> Requests;
    TVector<TBlockRange32> BlockRanges;
    TVector<IWriteBlocksHandlerPtr> WriteHandlers;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const bool IsZeroRequest;
    const TString DiskId;

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
        IBlockDigestGeneratorPtr blockDigestGenerator,
        TString diskId);

    void Bootstrap(const TActorContext& ctx);

private:
    NProto::TError BuildBlobContentAndComputeDigest();

    void WriteBlob(const TActorContext& ctx);
    void AddBlocks(const TActorContext& ctx);

    template <typename TEvent>
    void NotifyCompleted(const TActorContext& ctx, std::unique_ptr<TEvent> ev);
    bool HandleError(const TActorContext& ctx, const NProto::TError& error);

    void ReplyWrite(const TActorContext& ctx, const NProto::TError& error);
    void ReplyZero(const TActorContext& ctx, const NProto::TError& error);

    void ReplyAllAndDie(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleWriteBlobResponse(
        const TEvPartitionCommonPrivate::TEvWriteBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAddFreshBlocksResponse(
        const TEvPartitionCommonPrivate::TEvAddFreshBlocksResponse::TPtr& ev,
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
        IBlockDigestGeneratorPtr blockDigestGenerator,
        TString diskId)
    : PartitionActorId(partitionActorId)
    , CommitId(commitId)
    , Channel(channel)
    , BlockCount(blockCount)
    , Requests(std::move(requests))
    , BlockRanges(std::move(blockRanges))
    , WriteHandlers(std::move(writeHandlers))
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , IsZeroRequest(
          Requests.size() == 1 &&
          Requests.front().RequestType == ERequestType::ZeroBlocks)
    , DiskId(std::move(diskId))
{
    if (!IsZeroRequest) {
        const bool hasAnyZeroRequest = AnyOf(
            Requests,
            [](auto r) { return r.RequestType == ERequestType::ZeroBlocks; });

        STORAGE_VERIFY(
            !hasAnyZeroRequest && BlockRanges.size() == WriteHandlers.size(),
            TWellKnownEntityTypes::DISK,
            DiskId);
    } else {
        STORAGE_VERIFY(
            WriteHandlers.empty() && BlockRanges.size() == 1,
            TWellKnownEntityTypes::DISK,
            DiskId);
    }
}

void TWriteFreshBlocksActor::Bootstrap(const TActorContext& ctx)
{
    TDeque<TRequestScope> timers;

    ui64 requestId = 0;

    for (const auto& r: Requests) {
        LWTRACK(
            RequestReceived_PartitionWorker,
            r.RequestInfo->CallContext->LWOrbit,
            IsZeroRequest ? "ZeroFreshBlocks" : "WriteFreshBlocks",
            r.RequestInfo->CallContext->RequestId);

        timers.emplace_back(*r.RequestInfo);

        if (!r.RequestInfo->CallContext->LWOrbit.Fork(CombinedContext->LWOrbit)) {
            LWTRACK(
                ForkFailed,
                r.RequestInfo->CallContext->LWOrbit,
                "TEvPartitionCommonPrivate::TEvWriteBlobRequest",
                r.RequestInfo->CallContext->RequestId);
        }

        if (r.RequestInfo->CallContext->LWOrbit.HasShuttles()) {
            requestId = r.RequestInfo->CallContext->RequestId;
        }
    }
    CombinedContext->RequestId = requestId;

    Become(&TThis::StateWork);

    WriteBlob(ctx);
}

NProto::TError TWriteFreshBlocksActor::BuildBlobContentAndComputeDigest()
{
    if (IsZeroRequest) {
        BlobContent = NPartition::BuildZeroFreshBlocksBlobContent(BlockRanges.front());
        BlobSize = BlobContent.size();

        return {};
    }

    TVector<NPartition::TGuardHolder> holders(Reserve(BlockRanges.size()));

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

    auto request = std::make_unique<TEvPartitionCommonPrivate::TEvWriteBlobRequest>(
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

    IEventBasePtr request =
        std::make_unique<TEvPartitionCommonPrivate::TEvAddFreshBlocksRequest>(
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

template <typename TEvent>
void TWriteFreshBlocksActor::NotifyCompleted(
    const TActorContext& ctx,
    std::unique_ptr<TEvent> ev)
{
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

void TWriteFreshBlocksActor::ReplyWrite(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto completeEvent = std::make_unique<
        TEvPartitionCommonPrivate::TEvWriteFreshBlocksCompleted>(error);
    NotifyCompleted(ctx, std::move(completeEvent));

    for (const auto& r: Requests) {
        IEventBasePtr response = CreateWriteBlocksResponse(
            r.RequestType == ERequestType::WriteBlocksLocal,
            error);

        LWTRACK(
            ResponseSent_Partition,
            r.RequestInfo->CallContext->LWOrbit,
            "WriteFreshBlocks",
            r.RequestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *r.RequestInfo, std::move(response));
    }
}

void TWriteFreshBlocksActor::ReplyZero(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto completeEvent = std::make_unique<
        TEvPartitionCommonPrivate::TEvZeroFreshBlocksCompleted>(error);
    NotifyCompleted(ctx, std::move(completeEvent));

    for (const auto& r: Requests) {
        IEventBasePtr response =
            std::make_unique<TEvService::TEvZeroBlocksResponse>(error);

        LWTRACK(
            ResponseSent_Partition,
            r.RequestInfo->CallContext->LWOrbit,
            "ZeroFreshBlocks",
            r.RequestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *r.RequestInfo, std::move(response));
    }
}

void TWriteFreshBlocksActor::ReplyAllAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (IsZeroRequest) {
        ReplyZero(ctx, error);
    } else {
        ReplyWrite(ctx, error);
    }

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TWriteFreshBlocksActor::HandleWriteBlobResponse(
    const TEvPartitionCommonPrivate::TEvWriteBlobResponse::TPtr& ev,
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
    const TEvPartitionCommonPrivate::TEvAddFreshBlocksResponse::TPtr& ev,
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
        HFunc(TEvPartitionCommonPrivate::TEvWriteBlobResponse, HandleWriteBlobResponse);
        HFunc(TEvPartitionCommonPrivate::TEvAddFreshBlocksResponse, HandleAddFreshBlocksResponse);

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

void TFreshBlocksCompanion::WriteFreshBlocks(
    const TActorContext& ctx,
    TArrayRef<TRequestInBuffer<TWriteBufferRequestData>> requestsInBuffer,
    ui64 commitId)
{
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
            r.Data.ReplyLocal ? ERequestType::WriteBlocksLocal
                              : ERequestType::WriteBlocks);

        if (!r.Weight) {
            continue;
        }

        blockCount += r.Weight;

        FlushState.IncrementFreshBlocksInFlight(r.Data.Range.Size());

        blockRanges.push_back(r.Data.Range);
        writeHandlers.push_back(r.Data.Handler);
    }

    TrimFreshLogState.AccessTrimFreshLogBarriers().AcquireBarrierN(
        commitId,
        blockCount);

    const ui32 channel = ChannelsState.PickNextChannel(
        EChannelDataKind::Fresh,
        EChannelPermission::UserWritesAllowed);

    auto actor = NCloud::Register<TWriteFreshBlocksActor>(
        ctx,
        ctx.SelfID,
        commitId,
        channel,
        blockCount,
        std::move(requests),
        std::move(blockRanges),
        std::move(writeHandlers),
        BlockDigestGenerator,
        PartitionConfig.GetDiskId());

    Actors.Insert(actor);
}

////////////////////////////////////////////////////////////////////////////////

void TFreshBlocksCompanion::HandleAddFreshBlocks(
    const TEvPartitionCommonPrivate::TEvAddFreshBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    STORAGE_VERIFY(
        msg->WriteHandlers.size() == msg->BlockRanges.size() ||
            msg->WriteHandlers.empty(),
        TWellKnownEntityTypes::DISK,
        PartitionConfig.GetDiskId());

    for (size_t i = 0; i < msg->BlockRanges.size(); ++i) {
        auto& blockRange = msg->BlockRanges[i];

        if (!msg->WriteHandlers) {
            FreshBlocksState.ZeroFreshBlocks(blockRange, msg->CommitId);
            FlushState.DecrementFreshBlocksInFlight(blockRange.Size());

            continue;
        }

        auto& writeHandler = msg->WriteHandlers[i];
        auto guardedSgList =
            (*writeHandler).GetBlocks(ConvertRangeSafe(blockRange));

        if (auto guard = guardedSgList.Acquire()) {
            const auto& sgList = guard.Get();
            FreshBlocksState.WriteFreshBlocks(blockRange, msg->CommitId, sgList);
            FlushState.DecrementFreshBlocksInFlight(blockRange.Size());
        } else {
            LOG_ERROR(
                ctx,
                TBlockStoreComponents::PARTITION,
                "%s Failed to lock a guardedSgList on AddFreshBlocks",
                LogTitle.GetWithTime().c_str());
            Client.Poison(ctx);
            return;
        }
    }

    FreshBlobState.AddFreshBlob({msg->CommitId, msg->BlobSize});
    FlushState.IncrementUnflushedFreshBlobCount(1);
    FlushState.IncrementUnflushedFreshBlobByteCount(msg->BlobSize);

    // TODO(NBS-1976): update used blocks map

    using TResponse = TEvPartitionCommonPrivate::TEvAddFreshBlocksResponse;
    auto response = std::make_unique<TResponse>();

    NCloud::Reply(ctx, *ev, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TFreshBlocksCompanion::ZeroFreshBlocks(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    TBlockRange32 writeRange,
    ui64 commitId)
{
    TVector<TWriteFreshBlocksActor::TRequest> requests;
    TVector<TBlockRange32> blockRanges;
    const ui64 blockCount = writeRange.Size();

    requests.emplace_back(requestInfo, ERequestType::ZeroBlocks);
    blockRanges.emplace_back(writeRange);

    TrimFreshLogState.AccessTrimFreshLogBarriers().AcquireBarrierN(commitId, blockCount);

    const ui32 channel = ChannelsState.PickNextChannel(
        EChannelDataKind::Fresh,
        EChannelPermission::UserWritesAllowed);

    auto actor = NCloud::Register<TWriteFreshBlocksActor>(
        ctx,
        ctx.SelfID,
        commitId,
        channel,
        blockCount,
        std::move(requests),
        std::move(blockRanges),
        TVector<IWriteBlocksHandlerPtr>{},
        BlockDigestGenerator,
        PartitionConfig.GetDiskId());

    Actors.Insert(actor);
}

void TFreshBlocksCompanion::WriteFreshBlocksCompleted(
    const NActors::TActorContext& ctx,
    const NProto::TError& error,
    ui64 commitId,
    ui64 blockCount,
    NActors::TActorId actorId)
{
    Y_UNUSED(ctx);
    if (HasError(error)) {
        TrimFreshLogState.AccessTrimFreshLogBarriers().ReleaseBarrierN(
            commitId,
            blockCount);
    }

    Actors.Erase(actorId);
}

void TFreshBlocksCompanion::ZeroFreshBlocksCompleted(
    const NActors::TActorContext& ctx,
    const NProto::TError& error,
    ui64 commitId,
    ui64 blockCount,
    NActors::TActorId actorId)
{
    WriteFreshBlocksCompleted(ctx, error, commitId, blockCount, actorId);
}

}   // namespace NCloud::NBlockStore::NStorage
