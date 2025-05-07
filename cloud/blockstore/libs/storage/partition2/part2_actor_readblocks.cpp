#include "part2_actor.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>

#include <cloud/storage/core/libs/common/helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

using TBlockMarks = TVector<TTxPartition::TReadBlocks::TBlockMark>;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

using TRangeReadStats =
    TEvPartitionPrivate::TReadBlocksCompleted::TCompactionRangeReadStats;

void FillReadStats(
    const TReadBlocksRequests& requests,
    const ui32 compactionRangeSize,
    TStackVec<TRangeReadStats, 2>* stats)
{
    NKikimr::TLogoBlobID blobId;
    ui32 blockCount = 0;
    ui32 blobCount = 0;
    ui32 prevCompactionRange = Max<ui32>();

    for (auto& r: requests) {
        const auto compactionRange =
            TCompactionMap::GetRangeStart(r.BlockIndex, compactionRangeSize);
        if (compactionRange != prevCompactionRange) {
            if (blockCount) {
                Y_ABORT_UNLESS(prevCompactionRange != Max<ui32>());
                stats->emplace_back(prevCompactionRange, blobCount, blockCount);
            }

            blockCount = 0;
            blobCount = 0;

            prevCompactionRange = compactionRange;
        }

        if (blobId != r.BlobId) {
            blobId = r.BlobId;
            ++blobCount;
        }

        ++blockCount;
    }

    if (blockCount) {
        Y_ABORT_UNLESS(prevCompactionRange != Max<ui32>());
        stats->emplace_back(prevCompactionRange, blobCount, blockCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

IReadBlocksHandlerPtr CreateReadHandler(
    const TBlockRange64& readRange,
    const NProto::TReadBlocksRequest& request,
    ui32 blockSize)
{
    Y_UNUSED(request);
    return NStorage::CreateReadBlocksHandler(
        readRange,
        blockSize
    );
}

IReadBlocksHandlerPtr CreateReadHandler(
    const TBlockRange64& readRange,
    const NProto::TReadBlocksLocalRequest& request,
    ui32 blockSize)
{
    return NStorage::CreateReadBlocksHandler(
        readRange,
        request.Sglist,
        blockSize
    );
}

IEventBasePtr CreateReadBlocksResponse(
    const TBlockRange32& readRange,
    const IBlockDigestGenerator& blockDigestGenerator,
    const ui32 blockSize,
    bool replyLocal,
    TVector<IProfileLog::TBlockInfo>& blockInfos,
    IReadBlocksHandler& handler)
{
    if (replyLocal) {
        auto response = std::make_unique<TEvService::TEvReadBlocksLocalResponse>();
        auto guardedSgList = handler.GetLocalResponse(response->Record);

        if (auto guard = guardedSgList.Acquire()) {
            const auto& sglist = guard.Get();

            for (const auto blockIndex: xrange(readRange)) {
                const auto digest = blockDigestGenerator.ComputeDigest(
                    blockIndex,
                    sglist[blockIndex - readRange.Start]
                );

                if (digest.Defined()) {
                    blockInfos.push_back({blockIndex, *digest});
                }
            }
        }
        return response;
    } else {
        auto response = std::make_unique<TEvService::TEvReadBlocksResponse>();
        handler.GetResponse(response->Record);
        for (const auto blockIndex: xrange(readRange)) {
            const auto& blockContent = response->Record.GetBlocks().GetBuffers(blockIndex - readRange.Start);
            TBlockDataRef blockData = TBlockDataRef::CreateZeroBlock(blockSize);
            if (!blockContent.empty()) {
                blockData = TBlockDataRef(blockContent.data(), blockContent.size());
            }
            const auto digest = blockDigestGenerator.ComputeDigest(
                blockIndex,
                blockData
            );

            if (digest.Defined()) {
                blockInfos.push_back({blockIndex, *digest});
            }
        }
        return response;
    }
}

IEventBasePtr CreateReadBlocksResponse(
    bool replyLocal,
    const NProto::TError& error)
{
    if (replyLocal) {
        return std::make_unique<TEvService::TEvReadBlocksLocalResponse>(error);
    } else {
        return std::make_unique<TEvService::TEvReadBlocksResponse>(error);
    }
}

void FillOperationCompleted(
    TEvPartitionPrivate::TOperationCompleted& operation,
    TRequestInfoPtr requestInfo,
    ui64 commitId,
    ui32 blocksCount,
    TVector<IProfileLog::TBlockInfo> blockInfos)
{
    auto execTime = CyclesToDurationSafe(requestInfo->GetExecCycles());
    auto waitTime = CyclesToDurationSafe(requestInfo->GetWaitCycles());

    auto& counters = *operation.Stats.MutableUserReadCounters();
    counters.SetRequestsCount(1);
    counters.SetBlocksCount(blocksCount);
    counters.SetExecTime(execTime.MicroSeconds());
    counters.SetWaitTime(waitTime.MicroSeconds());

    operation.CommitId = commitId;
    operation.ExecCycles = requestInfo->GetExecCycles();
    operation.TotalCycles = requestInfo->GetTotalCycles();

    operation.AffectedBlockInfos = std::move(blockInfos);
}

TEvPartitionPrivate::TReadBlocksCompleted CreateOperationCompleted(
    TRequestInfoPtr requestInfo,
    ui64 commitId,
    ui32 blocksCount,
    TVector<IProfileLog::TBlockInfo> blockInfos)
{
    TEvPartitionPrivate::TReadBlocksCompleted operation;
    FillOperationCompleted(
        operation,
        requestInfo,
        commitId,
        blocksCount,
        std::move(blockInfos)
    );
    return operation;
}

struct TCompareByBlobIdAndOffset
{
    bool operator() (const TReadBlocksRequest& l, const TReadBlocksRequest& r)
    {
        return l.BlobId < r.BlobId
            || l.BlobId == r.BlobId && l.BlobOffset < r.BlobOffset;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReadBlocksActor final
    : public TActorBootstrapped<TReadBlocksActor>
{
public:
    struct TBatchRequest
    {
        TLogoBlobID BlobId;
        TActorId Proxy;
        TVector<ui16> BlobOffsets;
        TVector<ui64> Requests;
        ui32 GroupId = 0;

        TBatchRequest() = default;

        TBatchRequest(
                const TLogoBlobID& blobId,
                TActorId proxy,
                TVector<ui16> blobOffsets,
                TVector<ui64> requests,
                ui32 groupId)
            : BlobId(blobId)
            , Proxy(proxy)
            , BlobOffsets(std::move(blobOffsets))
            , Requests(std::move(requests))
            , GroupId(groupId)
        {}
    };

private:
    const TRequestInfoPtr RequestInfo;

    const ui32 CompactionRangeSize;
    const ui32 BlockSize;
    const TString BaseDiskId;
    const TString BaseDiskCheckpointId;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;

    const TActorId Tablet;
    const ui64 CommitId;

    const TMaybe<TBlockRange32> DescribeBlocksRange;

    const IReadBlocksHandlerPtr ReadHandler;
    const TBlockRange32 ReadRange;
    const bool ReplyLocal;

    TBlockMarks BlockMarks;

    TReadBlocksRequests OwnRequests;
    TStackVec<TRangeReadStats, 2> ReadStats;
    TVector<IProfileLog::TBlockInfo> BlockInfos;

    TVector<TBatchRequest> BatchRequests;
    size_t RequestsScheduled = 0;
    size_t RequestsCompleted = 0;

    bool DescribeBlocksInProgress = false;

    TVector<TCallContextPtr> ForkedCallContexts;

public:
    TReadBlocksActor(
        TRequestInfoPtr requestInfo,
        ui32 compactionRangeSize,
        ui32 blockSize,
        const TString& baseDiskId,
        const TString& baseDiskCheckpointId,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        const TActorId& tablet,
        ui64 commitId,
        const TMaybe<TBlockRange32>& describeBlocksRange,
        IReadBlocksHandlerPtr readHandler,
        const TBlockRange32& readRange,
        bool replyLocal,
        TBlockMarks blockMarks,
        TReadBlocksRequests ownRequests,
        TVector<IProfileLog::TBlockInfo> blockInfos);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeBlocks(const TActorContext& ctx);
    TMaybe<TReadBlocksRequests> ProcessDescribeBlocksResponse(
        const TEvVolume::TEvDescribeBlocksResponse& response);
    void ReadBlocks(
        const TActorContext& ctx,
        TReadBlocksRequests requests,
        bool baseDisk);

    void NotifyCompleted(const TActorContext& ctx, const NProto::TError& error);
    bool HandleError(const TActorContext& ctx, const NProto::TError& error);

    void ReplyAndDie(
        const TActorContext& ctx,
        IEventBasePtr response,
        const NProto::TError& error = {});

private:
    STFUNC(StateWork);

    void HandleDescribeBlocksResponse(
        const TEvVolume::TEvDescribeBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleReadBlobResponse(
        const TEvPartitionPrivate::TEvReadBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TReadBlocksActor::TReadBlocksActor(
        TRequestInfoPtr requestInfo,
        ui32 compactionRangeSize,
        ui32 blockSize,
        const TString& baseDiskId,
        const TString& baseDiskCheckpointId,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        const TActorId& tablet,
        ui64 commitId,
        const TMaybe<TBlockRange32>& describeBlocksRange,
        IReadBlocksHandlerPtr readHandler,
        const TBlockRange32& readRange,
        bool replyLocal,
        TBlockMarks blockMarks,
        TReadBlocksRequests ownRequests,
        TVector<IProfileLog::TBlockInfo> blockInfos)
    : RequestInfo(std::move(requestInfo))
    , CompactionRangeSize(compactionRangeSize)
    , BlockSize(blockSize)
    , BaseDiskId(baseDiskId)
    , BaseDiskCheckpointId(baseDiskCheckpointId)
    , BlockDigestGenerator(std::move(blockDigestGenerator))
    , Tablet(tablet)
    , CommitId(commitId)
    , DescribeBlocksRange(describeBlocksRange)
    , ReadHandler(std::move(readHandler))
    , ReadRange(readRange)
    , ReplyLocal(replyLocal)
    , BlockMarks(std::move(blockMarks))
    , OwnRequests(std::move(ownRequests))
    , BlockInfos(std::move(blockInfos))
{}

void TReadBlocksActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "ReadBlocks",
        RequestInfo->CallContext->RequestId);

    if (DescribeBlocksRange.Defined()) {
        DescribeBlocks(ctx);
    }

    Sort(OwnRequests, TCompareByBlobIdAndOffset());
    FillReadStats(OwnRequests, CompactionRangeSize, &ReadStats);
    ReadBlocks(ctx, std::move(OwnRequests), false);
}

void TReadBlocksActor::DescribeBlocks(const TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(DescribeBlocksRange);
    Y_DEBUG_ABORT_UNLESS(DescribeBlocksRange->Size());
    Y_DEBUG_ABORT_UNLESS(BaseDiskId);
    Y_DEBUG_ABORT_UNLESS(BaseDiskCheckpointId);
    Y_DEBUG_ABORT_UNLESS(ReadRange.Size());
    Y_DEBUG_ABORT_UNLESS(BlockMarks.size() == ReadRange.Size());
    Y_DEBUG_ABORT_UNLESS(ReadRange.Contains(*DescribeBlocksRange));

    DescribeBlocksInProgress = true;

    auto request = std::make_unique<TEvVolume::TEvDescribeBlocksRequest>();

    request->Record.SetStartIndex(DescribeBlocksRange->Start);
    request->Record.SetBlocksCount(DescribeBlocksRange->Size());
    request->Record.SetDiskId(BaseDiskId);
    request->Record.SetCheckpointId(BaseDiskCheckpointId);

    const auto blockCountToRead = CountIf(
        BlockMarks,
        [] (const auto& m) {
            return m.Empty;
        }
    );
    request->Record.SetBlocksCountToRead(blockCountToRead);

    auto self = SelfId();

    TAutoPtr<IEventHandle> event = new IEventHandle(
        MakeVolumeProxyServiceId(),
        self,
        request.release());

    ctx.Send(event);
}

TMaybe<TReadBlocksRequests> TReadBlocksActor::ProcessDescribeBlocksResponse(
    const TEvVolume::TEvDescribeBlocksResponse& response)
{
    Y_ABORT_UNLESS(DescribeBlocksRange);

    auto& handler = *ReadHandler;
    const auto& record = response.Record;

    for (const auto& r : record.GetFreshBlockRanges()) {
        for (size_t i = 0; i < r.GetBlocksCount(); ++i) {
            const auto blockIndex = r.GetStartIndex() + i;
            Y_ABORT_UNLESS(DescribeBlocksRange->Contains(blockIndex));
            auto& mark = BlockMarks[blockIndex - ReadRange.Start];

            if (mark.Empty) {
                TBlockDataRef blockContent(
                    r.GetBlocksContent().data() + i * BlockSize,
                    BlockSize);
                if (!handler.SetBlock(blockIndex, blockContent, true)) {
                    return {};
                }
            }
        }
    }

    TReadBlocksRequests requests;

    for (const auto& p : record.GetBlobPieces()) {
        const auto& blobId = LogoBlobIDFromLogoBlobID(p.GetBlobId());
        const auto group = p.GetBSGroupId();

        for (const auto& r : p.GetRanges()) {
            for (size_t i = 0; i < r.GetBlocksCount(); ++i) {
                const auto blockIndex = r.GetBlockIndex() + i;
                Y_ABORT_UNLESS(DescribeBlocksRange->Contains(blockIndex));
                auto& mark = BlockMarks[blockIndex - ReadRange.Start];

                if (mark.Empty) {
                    requests.emplace_back(
                        blobId,
                        MakeBlobStorageProxyID(group),
                        r.GetBlobOffset() + i,
                        blockIndex,
                        group);
                }
            }
        }
    }

    return requests;
}

void TReadBlocksActor::ReadBlocks(
    const TActorContext& ctx,
    TReadBlocksRequests requests,
    bool baseDisk)
{
    Y_DEBUG_ABORT_UNLESS(IsSorted(
        requests.begin(),
        requests.end(),
        TCompareByBlobIdAndOffset()
    ));

    const ui32 batchRequestsOldSize = BatchRequests.size();

    TBatchRequest current;
    for (auto& req: requests) {
        if (current.BlobId != req.BlobId) {
            if (current.BlobOffsets) {
                BatchRequests.emplace_back(
                    current.BlobId,
                    current.Proxy,
                    std::move(current.BlobOffsets),
                    std::move(current.Requests),
                    current.GroupId);
            }
            current.GroupId = req.GroupId;
            current.BlobId = req.BlobId;
            current.Proxy = req.BSProxy;
        }

        current.BlobOffsets.push_back(req.BlobOffset);
        current.Requests.push_back(req.BlockIndex);
    }

    if (current.BlobOffsets) {
        BatchRequests.emplace_back(
            current.BlobId,
            current.Proxy,
            std::move(current.BlobOffsets),
            std::move(current.Requests),
            current.GroupId);
    }

    for (ui32 batchIndex = batchRequestsOldSize; batchIndex < BatchRequests.size(); ++batchIndex) {
        auto& batch = BatchRequests[batchIndex];

        RequestsScheduled += batch.Requests.size();

        auto request = std::make_unique<TEvPartitionPrivate::TEvReadBlobRequest>(
            batch.BlobId,
            batch.Proxy,
            std::move(batch.BlobOffsets),
            ReadHandler->GetGuardedSgList(batch.Requests, baseDisk),
            batch.GroupId,
            false,          // async
            TInstant::Max() // deadline
        );

        if (!RequestInfo->CallContext->LWOrbit.Fork(request->CallContext->LWOrbit)) {
            LWTRACK(
                ForkFailed,
                RequestInfo->CallContext->LWOrbit,
                "TEvPartitionPrivate::TEvReadBlobRequest",
                RequestInfo->CallContext->RequestId);
        }

        ForkedCallContexts.emplace_back(request->CallContext);

        NCloud::Send(
            ctx,
            Tablet,
            std::move(request),
            batchIndex);
    }
}

void TReadBlocksActor::NotifyCompleted(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto request = std::make_unique<TEvPartitionPrivate::TEvReadBlocksCompleted>(error);
    FillOperationCompleted(
        *request,
        RequestInfo,
        CommitId,
        RequestsCompleted,
        std::move(BlockInfos));
    request->ReadStats = ReadStats;

    NCloud::Send(ctx, Tablet, std::move(request));
}

bool TReadBlocksActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (FAILED(error.GetCode())) {
        auto response = CreateReadBlocksResponse(ReplyLocal, error);
        ReplyAndDie(ctx, std::move(response), error);
        return true;
    }

    return false;
}

void TReadBlocksActor::ReplyAndDie(
    const TActorContext& ctx,
    IEventBasePtr response,
    const NProto::TError& error)
{
    NotifyCompleted(ctx, error);

    LWTRACK(
        ResponseSent_Partition,
        RequestInfo->CallContext->LWOrbit,
        "ReadBlocks",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TReadBlocksActor::HandleDescribeBlocksResponse(
    const TEvVolume::TEvDescribeBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    DescribeBlocksInProgress = false;

    const auto* msg = ev->Get();

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    auto requests = ProcessDescribeBlocksResponse(msg->Record);
    if (!requests) {
        HandleError(ctx, MakeError(E_REJECTED, "DescribeBlocks response processing failed"));
        return;
    }

    if (requests->empty()) {
        if (RequestsCompleted == RequestsScheduled) {
            auto response = CreateReadBlocksResponse(
                ReadRange,
                *BlockDigestGenerator,
                BlockSize,
                ReplyLocal,
                BlockInfos,
                *ReadHandler
            );
            ReplyAndDie(ctx, std::move(response), {});
        }
        return;
    }

    Sort(*requests, TCompareByBlobIdAndOffset());
    ReadBlocks(ctx, std::move(*requests), true);
}

void TReadBlocksActor::HandleReadBlobResponse(
    const TEvPartitionPrivate::TEvReadBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    RequestInfo->AddExecCycles(msg->ExecCycles);

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    ui32 batchIndex = ev->Cookie;

    Y_ABORT_UNLESS(batchIndex < BatchRequests.size());
    auto& batch = BatchRequests[batchIndex];

    RequestsCompleted += batch.Requests.size();
    Y_ABORT_UNLESS(RequestsCompleted <= RequestsScheduled);
    if (RequestsCompleted < RequestsScheduled) {
        return;
    }

    if (DescribeBlocksInProgress) {
        return;
    }

    for (auto context: ForkedCallContexts) {
        RequestInfo->CallContext->LWOrbit.Join(context->LWOrbit);
    }

    auto response = CreateReadBlocksResponse(
        ReadRange,
        *BlockDigestGenerator,
        BlockSize,
        ReplyLocal,
        BlockInfos,
        *ReadHandler
    );
    ReplyAndDie(ctx, std::move(response));
}

void TReadBlocksActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(E_REJECTED, "tablet is shutting down");

    auto response = CreateReadBlocksResponse(ReplyLocal, error);
    ReplyAndDie(ctx, std::move(response), error);
}

STFUNC(TReadBlocksActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvVolume::TEvDescribeBlocksResponse, HandleDescribeBlocksResponse);
        HFunc(TEvPartitionPrivate::TEvReadBlobResponse, HandleReadBlobResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TReadBlocksVisitor final
    : public IFreshBlockVisitor
    , public IMergedBlockVisitor
{
private:
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const ui32 BlockSize;
    TTabletStorageInfo& TabletInfo;
    TTxPartition::TReadBlocks& Args;

public:
    TReadBlocksVisitor(
            IBlockDigestGeneratorPtr blockDigestGenerator,
            ui32 blockSize,
            TTabletStorageInfo& tabletInfo,
            TTxPartition::TReadBlocks& args)
        : BlockDigestGenerator(blockDigestGenerator)
        , BlockSize(blockSize)
        , TabletInfo(tabletInfo)
        , Args(args)
    {}

    void Visit(const TBlock& block, TStringBuf blockContent) override
    {
        if (AddBlock(block, {}, 0, 0)) {
            TBlockDataRef blockData = TBlockDataRef::CreateZeroBlock(BlockSize);
            if (!blockContent.empty()) {
                blockData = TBlockDataRef(blockContent.data(), blockContent.size());
            }
            Args.ReadHandler->SetBlock(
                block.BlockIndex,
                blockData,
                false);

            const auto digest = BlockDigestGenerator->ComputeDigest(
                block.BlockIndex,
                blockData);

            if (digest.Defined()) {
                Args.BlockInfos.push_back({block.BlockIndex, *digest});
            }
        }
    }

    void Visit(
        const TBlock& block,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        TLogoBlobID logoBlobId;
        ui32 group = 0;

        if (!block.Zeroed) {
            Y_ABORT_UNLESS(!IsDeletionMarker(blobId));
            logoBlobId = MakeBlobId(TabletInfo.TabletID, blobId);
            group = TabletInfo.GroupFor(
                blobId.Channel(), blobId.Generation());
        } else {
            Y_ABORT_UNLESS(blobOffset == ZeroBlobOffset);
        }

        if (AddBlock(block, logoBlobId, group, blobOffset)) {
            // will read from BS later
            Args.ReadHandler->SetBlock(block.BlockIndex, {}, false);
            ++Args.BlocksToRead;
        }
    }

private:
    bool AddBlock(
        const TBlock& block,
        const TLogoBlobID& blobId,
        ui32 group,
        ui16 blobOffset)
    {
        Y_ABORT_UNLESS(block.MinCommitId <= Args.CommitId
              && block.MaxCommitId > Args.CommitId);

        Y_ABORT_UNLESS(Args.ReadRange.Contains(block.BlockIndex));
        auto& mark = Args.Blocks[block.BlockIndex - Args.ReadRange.Start];

        // merge different versions
        if (mark.MinCommitId < block.MinCommitId) {
            mark.MinCommitId = block.MinCommitId;
            mark.MaxCommitId = block.MaxCommitId;
            mark.BlobId = blobId;
            mark.BSGroupId = group;
            mark.BlobOffset = blobOffset;
            mark.Empty = false;
            return true;
        }

        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

TMaybe<TBlockRange32> ComputeDescribeBlocksRange(
    const TBlockRange32& readRange,
    const TBlockMarks& blocks)
{
    Y_DEBUG_ABORT_UNLESS(readRange.Size() == blocks.size());

    const auto firstEmptyIt = FindIf(blocks, [](const auto& b) { return b.Empty; });
    if (firstEmptyIt == blocks.end()) {
        return {};
    }

    const auto firstEmptyIndex = std::distance(blocks.begin(), firstEmptyIt);
    const auto firstEmptyBlockIndex = readRange.Start + firstEmptyIndex;

    const auto lastEmptyRit = FindIf(
        blocks.rbegin(),
        blocks.rend(),
        [](const auto& b) {
            return b.Empty;
        }
    );
    const auto lastEmptyIndexFromEnd = std::distance(blocks.rbegin(), lastEmptyRit);
    const auto lastEmptyIndex = blocks.size() - 1 - lastEmptyIndexFromEnd;
    const auto lastEmptyBlockIndex = readRange.Start + lastEmptyIndex;

    return TBlockRange32::MakeClosedInterval(
        firstEmptyBlockIndex,
        lastEmptyBlockIndex);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleReadBlocks(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    HandleReadBlocksRequest<TEvService::TReadBlocksMethod>(ev, ctx, false);
}

void TPartitionActor::HandleReadBlocksLocal(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    HandleReadBlocksRequest<TEvService::TReadBlocksLocalMethod>(ev, ctx, true);
}

template <typename TMethod>
void TPartitionActor::HandleReadBlocksRequest(
    const typename TMethod::TRequest::TPtr& ev,
    const TActorContext& ctx,
    bool replyLocal)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvService::TReadBlocksMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "ReadBlocks",
        requestInfo->CallContext->RequestId);

    auto replyError = [=] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        ui32 errorCode,
        TString errorReason,
        ui32 flags = 0)
    {
        auto response = std::make_unique<TEvService::TEvReadBlocksResponse>(
            MakeError(errorCode, std::move(errorReason), flags));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "ReadBlocks",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    TBlockRange64 readRange;

    auto ok = InitReadWriteBlockRange(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount(),
        &readRange
    );

    if (!ok) {
        replyError(ctx, *requestInfo, E_ARGUMENT, TStringBuilder()
            << "invalid block range ["
            << "index: " << msg->Record.GetStartIndex()
            << ", count: " << msg->Record.GetBlocksCount()
            << "]");
        return;
    }

    ui64 commitId = 0;
    if (const auto& checkpointId = msg->Record.GetCheckpointId()) {
        commitId = State->GetCheckpoints().GetCommitId(checkpointId);
        if (!commitId) {
            ui32 flags = 0;
            SetProtoFlag(flags, NProto::EF_SILENT);
            replyError(
                ctx,
                *requestInfo,
                E_NOT_FOUND,
                TStringBuilder()
                    << "checkpoint not found: " << checkpointId.Quote(),
                flags);
            return;
        }
    }

    auto readHandler = CreateReadHandler(
        readRange,
        msg->Record,
        State->GetBlockSize());

    ReadBlocks(
        ctx,
        requestInfo,
        commitId,
        ConvertRangeSafe(readRange),
        std::move(readHandler),
        replyLocal);
}

void TPartitionActor::ReadBlocks(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    ui64 commitId,
    const TBlockRange32& readRange,
    IReadBlocksHandlerPtr readHandler,
    bool replyLocal)
{
    LOG_TRACE(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Start read blocks @%lu (range: %s)",
        TabletID(),
        commitId,
        DescribeRange(readRange).data());

    AddTransaction(*requestInfo, requestInfo->CancelRoutine);

    ExecuteTx<TReadBlocks>(
        ctx,
        requestInfo,
        commitId,
        readRange,
        std::move(readHandler),
        replyLocal);
}

void TPartitionActor::HandleReadBlocksCompleted(
    const TEvPartitionPrivate::TEvReadBlocksCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    Actors.erase(ev->Sender);
    State->ReleaseCollectBarrier(ev->Get()->CommitId);
    FinalizeReadBlocks(ctx, std::move(*ev->Get()));
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareReadBlocks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TReadBlocks& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    args.CommitId = args.CheckpointId;
    if (!args.CommitId) {
        // will read latest state
        args.CommitId = State->GetLastCommitId();
    }

    if (!State->InitIndex(db, args.ReadRange)) {
        return false;
    }

    TReadBlocksVisitor visitor(
        BlockDigestGenerator,
        State->GetBlockSize(),
        *Info(),
        args
    );
    State->FindFreshBlocks(args.CommitId, args.ReadRange, visitor);

    return State->FindMergedBlocks(db, args.CommitId, args.ReadRange, visitor);
}

void TPartitionActor::ExecuteReadBlocks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TReadBlocks& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteReadBlocks(
    const TActorContext& ctx,
    TTxPartition::TReadBlocks& args)
{
    TRequestScope timer(*args.RequestInfo);
    RemoveTransaction(*args.RequestInfo);

    TReadBlocksRequests requests(Reserve(args.BlocksToRead));

    ui32 blockIndex = args.ReadRange.Start;
    for (const auto& mark: args.Blocks) {
        if (mark.BlobId) {
            requests.emplace_back(
                mark.BlobId,
                MakeBlobStorageProxyID(mark.BSGroupId),
                mark.BlobOffset,
                blockIndex,
                mark.BSGroupId);
        }
        ++blockIndex;
    }

    TMaybe<TBlockRange32> describeBlocksRange;

    if (State->GetBaseDiskId()) {
        describeBlocksRange = ComputeDescribeBlocksRange(args.ReadRange, args.Blocks);
    }

    if (describeBlocksRange.Defined() || requests) {
        State->AcquireCollectBarrier(args.CommitId);

        TBlockMarks blocks;
        if (describeBlocksRange.Defined()) {
            blocks = std::move(args.Blocks);
        }

        auto actor = NCloud::Register<TReadBlocksActor>(
            ctx,
            args.RequestInfo,
            State->GetCompactionMap().GetRangeSize(),
            State->GetBlockSize(),
            State->GetBaseDiskId(),
            State->GetBaseDiskCheckpointId(),
            BlockDigestGenerator,
            SelfId(),
            args.CommitId,
            describeBlocksRange,
            args.ReadHandler,
            args.ReadRange,
            args.ReplyLocal,
            std::move(blocks),
            std::move(requests),
            std::move(args.BlockInfos));

        Actors.insert(actor);
        return;
    }

    auto response = CreateReadBlocksResponse(
        args.ReadRange,
        *BlockDigestGenerator,
        State->GetBlockSize(),
        args.ReplyLocal,
        args.BlockInfos,
        *args.ReadHandler
    );
    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "ReadBlocks",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    FinalizeReadBlocks(
        ctx,
        CreateOperationCompleted(
            args.RequestInfo,
            args.CommitId,
            args.ReadRange.Size(),
            std::move(args.BlockInfos)
        )
    );
}

void TPartitionActor::FinalizeReadBlocks(
    const TActorContext& ctx,
    TEvPartitionPrivate::TReadBlocksCompleted operation)
{
    LOG_TRACE(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Complete read blocks @%lu",
        TabletID(),
        operation.CommitId);

    UpdateStats(operation.Stats);

    ui64 blocksCount = operation.Stats.GetUserReadCounters().GetBlocksCount();
    ui64 requestBytes = blocksCount * State->GetBlockSize();

    UpdateCPUUsageStat(ctx, operation.ExecCycles);

    auto time = CyclesToDurationSafe(operation.TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.ReadBlocks.AddRequest(time, requestBytes);

    LogBlockInfos(
        ctx,
        EBlockStoreRequest::ReadBlocks,
        std::move(operation.AffectedBlockInfos),
        operation.CommitId
    );

    for (const auto& stat: operation.ReadStats) {
        State->GetCompactionMap().RegisterRead(
            stat.BlockIndex,
            stat.BlobCount,
            stat.BlockCount
        );
    }

    EnqueueUpdateIndexStructuresIfNeeded(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
