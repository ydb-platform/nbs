#include "part_actor.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/common/helpers.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;
using namespace NTabletFlatExecutor::NFlatExecutorSetup;

using TFreshMark = TTxPartition::TReadBlocks::TFreshMark;
using TZeroMark = TTxPartition::TReadBlocks::TZeroMark;
using TEmptyMark = TTxPartition::TReadBlocks::TEmptyMark;
using TBlobMark = TTxPartition::TReadBlocks::TBlobMark;

using TBlockMark = TTxPartition::TReadBlocks::TBlockMark;
using TBlockMarks = TTxPartition::TReadBlocks::TBlockMarks;

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
                Y_VERIFY(prevCompactionRange != Max<ui32>());
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
        Y_VERIFY(prevCompactionRange != Max<ui32>());
        stats->emplace_back(prevCompactionRange, blobCount, blockCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

ui64 GetCommitId(const NProto::TReadBlocksRequest& request)
{
    Y_UNUSED(request);
    return 0;
}

ui64 GetCommitId(const NProto::TReadBlocksLocalRequest& request)
{
    return request.CommitId;
}

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
            if (!blockContent.Empty()) {
                blockData = TBlockDataRef(blockContent.Data(), blockContent.Size());
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
    operation.ExecCycles = requestInfo->GetExecCycles();
    operation.TotalCycles = requestInfo->GetTotalCycles();
    operation.CommitId = commitId;

    auto execTime = CyclesToDurationSafe(requestInfo->GetExecCycles());
    auto waitTime = CyclesToDurationSafe(requestInfo->GetWaitCycles());

    auto& counters = *operation.Stats.MutableUserReadCounters();
    counters.SetRequestsCount(1);
    counters.SetBlocksCount(blocksCount);
    counters.SetExecTime(execTime.MicroSeconds());
    counters.SetWaitTime(waitTime.MicroSeconds());

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
        NKikimr::TLogoBlobID BlobId;
        TActorId Proxy;
        TVector<ui16> BlobOffsets;
        TVector<ui64> Requests; // block indices, ui64 needed for integration
                                // with sglist-related code
        ui32 GroupId = 0;

        TBatchRequest() = default;

        TBatchRequest(
                const NKikimr::TLogoBlobID& blobId,
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

    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const ui32 BlockSize;
    const ui32 CompactionRangeSize;
    const TString BaseDiskId;
    const TString BaseDiskCheckpointId;

    const TActorId Tablet;

    const TMaybe<TBlockRange32> DescribeBlocksRange;

    const IReadBlocksHandlerPtr ReadHandler;
    const TBlockRange32 ReadRange;
    const bool ReplyLocal;

    const ui64 CommitId;

    TBlockMarks BlockMarks;

    bool DescribeBlocksInProgress = false;

    TVector<TCallContextPtr> ForkedCallContexts;

    TReadBlocksRequests OwnRequests;
    TStackVec<TRangeReadStats, 2> ReadStats;
    TVector<IProfileLog::TBlockInfo> BlockInfos;
    TVector<TBatchRequest> BatchRequests;
    size_t RequestsScheduled = 0;
    size_t RequestsCompleted = 0;

public:
    TReadBlocksActor(
        TRequestInfoPtr requestInfo,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ui32 blockSize,
        ui32 compactionRangeSize,
        const TString& baseDiskId,
        const TString& baseDiskCheckpointId,
        const TActorId& tablet,
        const TMaybe<TBlockRange32>& describeBlocksRange,
        IReadBlocksHandlerPtr readHandler,
        const TBlockRange32& readRange,
        bool replyLocal,
        ui64 commitId,
        TBlockMarks inputBlockMarks,
        TReadBlocksRequests ownRequests,
        TVector<IProfileLog::TBlockInfo> blockInfos);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeBlocks(const TActorContext& ctx);
    NProto::TError ValidateDescribeBlocksResponse(
        const TEvVolume::TEvDescribeBlocksResponse& response);
    TMaybe<TReadBlocksRequests> ProcessDescribeBlocksResponse(
        const TEvVolume::TEvDescribeBlocksResponse& response);
    void HandleDescribeBlocksResponse(
        const TEvVolume::TEvDescribeBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReadBlocks(
        const TActorContext& ctx,
        TReadBlocksRequests requests,
        bool baseDisk);

    void NotifyCompleted(const TActorContext& ctx, const NProto::TError& error);
    bool HandleError(const TActorContext& ctx, const NProto::TError& error);

    void ReplyAndDie(
        const TActorContext& ctx,
        IEventBasePtr response,
        const NProto::TError& error);

private:
    STFUNC(StateWork);

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
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ui32 blockSize,
        ui32 compactionRangeSize,
        const TString& baseDiskId,
        const TString& baseDiskCheckpointId,
        const TActorId& tablet,
        const TMaybe<TBlockRange32>& describeBlocksRange,
        IReadBlocksHandlerPtr readHandler,
        const TBlockRange32& readRange,
        bool replyLocal,
        ui64 commitId,
        TBlockMarks inputBlockMarks,
        TReadBlocksRequests ownRequests,
        TVector<IProfileLog::TBlockInfo> blockInfos)
    : RequestInfo(std::move(requestInfo))
    , BlockDigestGenerator(blockDigestGenerator)
    , BlockSize(blockSize)
    , CompactionRangeSize(compactionRangeSize)
    , BaseDiskId(baseDiskId)
    , BaseDiskCheckpointId(baseDiskCheckpointId)
    , Tablet(tablet)
    , DescribeBlocksRange(describeBlocksRange)
    , ReadHandler(std::move(readHandler))
    , ReadRange(readRange)
    , ReplyLocal(replyLocal)
    , CommitId(commitId)
    , BlockMarks(std::move(inputBlockMarks))
    , OwnRequests(std::move(ownRequests))
    , BlockInfos(std::move(blockInfos))
{
    ActivityType = TBlockStoreActivities::PARTITION_WORKER;
}

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
    Y_VERIFY_DEBUG(DescribeBlocksRange);
    Y_VERIFY_DEBUG(DescribeBlocksRange->Size());
    Y_VERIFY_DEBUG(BaseDiskId);
    Y_VERIFY_DEBUG(BaseDiskCheckpointId);
    Y_VERIFY_DEBUG(ReadRange.Size());
    Y_VERIFY_DEBUG(BlockMarks.size() == ReadRange.Size());
    Y_VERIFY_DEBUG(ReadRange.Contains(*DescribeBlocksRange));

    DescribeBlocksInProgress = true;

    auto request = std::make_unique<TEvVolume::TEvDescribeBlocksRequest>();

    request->Record.SetStartIndex(DescribeBlocksRange->Start);
    request->Record.SetBlocksCount(DescribeBlocksRange->Size());
    request->Record.SetDiskId(BaseDiskId);
    request->Record.SetCheckpointId(BaseDiskCheckpointId);

    request->Record.SetBlocksCountToRead(
        CountIf(BlockMarks, [](const auto& mark) { return std::holds_alternative<TEmptyMark>(mark); }));

    TAutoPtr<IEventHandle> event = new IEventHandle(
        MakeVolumeProxyServiceId(),
        SelfId(),
        request.release());

    ctx.Send(event);
}

NProto::TError TReadBlocksActor::ValidateDescribeBlocksResponse(
    const TEvVolume::TEvDescribeBlocksResponse& response)
{
    Y_VERIFY_DEBUG(DescribeBlocksRange);

    const auto& record = response.Record;

    for (const auto& range: record.GetFreshBlockRanges()) {
        const auto rangeToValidate =
            TBlockRange32::WithLength(
                range.GetStartIndex(), range.GetBlocksCount());

        if (!DescribeBlocksRange->Contains(rangeToValidate)) {
            return MakeError(
                E_FAIL,
                TStringBuilder() <<
                "DescribeBlocks error. Fresh block is out of range"
                " DescribeBlocksRange: " << DescribeRange(*DescribeBlocksRange) <<
                " rangeToValidate start: " << DescribeRange(rangeToValidate));

        }

        const auto& contentToValidate = range.GetBlocksContent();

        if (contentToValidate.size() != rangeToValidate.Size() * BlockSize) {
            return MakeError(
                E_FAIL,
                TStringBuilder() <<
                "DescribeBlocks error. Fresh block content has invalid size."
                " rangeToValidate: " << DescribeRange(rangeToValidate) <<
                " BlockSize: " << BlockSize <<
                " contentToValidate size: " << contentToValidate.size());
        }
    }

    for (const auto& piece: record.GetBlobPieces()) {
        for (const auto& range: piece.GetRanges()) {
            const auto blockRange = TBlockRange32::WithLength(
                range.GetBlockIndex(),
                range.GetBlocksCount());
            if (!DescribeBlocksRange->Contains(blockRange)) {
                return MakeError(
                    E_FAIL,
                    TStringBuilder() <<
                    "DescribeBlocks error. Blob range is out of bounds."
                    " DescribeBlocksRange: " << DescribeRange(*DescribeBlocksRange) <<
                    " blockRange:" << DescribeRange(blockRange)
                );
            }
        }
    }

    return {};
}

TMaybe<TReadBlocksRequests> TReadBlocksActor::ProcessDescribeBlocksResponse(
    const TEvVolume::TEvDescribeBlocksResponse& response)
{
    Y_VERIFY_DEBUG(DescribeBlocksRange);

    const auto startIndex = ReadRange.Start;
    auto& handler = *ReadHandler;
    const auto& record = response.Record;

    for (const auto& range : record.GetFreshBlockRanges()) {
        for (size_t index = 0; index < range.GetBlocksCount(); ++index) {
            const char* startingByte =
                range.GetBlocksContent().data() + index * BlockSize;

            TStringBuf blockContent(startingByte, BlockSize);

            const auto blockIndex = range.GetStartIndex() + index;
            const auto blockMarkIndex = blockIndex - startIndex;

            if (std::holds_alternative<TEmptyMark>(BlockMarks[blockMarkIndex])) {
                BlockMarks[blockMarkIndex] = TFreshMark();
                if (!handler.SetBlock(
                        blockIndex,
                        TBlockDataRef(blockContent.Data(),
                        blockContent.Size()),
                        true))
                {
                    return {};
                }
            }
        }
    }

    TReadBlocksRequests requests;

    for (const auto& piece: record.GetBlobPieces()) {
        const auto& blobId = LogoBlobIDFromLogoBlobID(piece.GetBlobId());
        const auto group = piece.GetBSGroupId();

        for (const auto& range: piece.GetRanges()) {
            for (size_t i = 0; i < range.GetBlocksCount(); ++i) {
                const auto blobOffset = range.GetBlobOffset() + i;
                const auto blockIndex = range.GetBlockIndex() + i;
                const auto blockMarkIndex = blockIndex - startIndex;

                if (std::holds_alternative<TEmptyMark>(BlockMarks[blockMarkIndex])) {
                    requests.emplace_back(
                        blobId,
                        MakeBlobStorageProxyID(group),
                        blobOffset,
                        blockIndex,
                        group);
                }
            }
        }
    }

    return requests;
}

void TReadBlocksActor::HandleDescribeBlocksResponse(
    const TEvVolume::TEvDescribeBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    DescribeBlocksInProgress = false;

    const auto* msg = ev->Get();

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    if (HandleError(ctx, ValidateDescribeBlocksResponse(msg->Record))) {
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

void TReadBlocksActor::ReadBlocks(
    const TActorContext& ctx,
    TReadBlocksRequests requests,
    bool baseDisk)
{
    const ui32 batchRequestsOldSize = BatchRequests.size();
    Y_VERIFY_DEBUG(IsSorted(
        requests.begin(),
        requests.end(),
        TCompareByBlobIdAndOffset()
    ));

    TBatchRequest current;
    for (auto& r: requests) {
        if (current.BlobId != r.BlobId) {
            if (current.BlobOffsets) {
                BatchRequests.emplace_back(
                    current.BlobId,
                    current.Proxy,
                    std::move(current.BlobOffsets),
                    std::move(current.Requests),
                    current.GroupId);
            }
            current.BlobId = r.BlobId;
            current.Proxy = r.BSProxy;
            current.GroupId = r.GroupId;
        }

        current.BlobOffsets.push_back(r.BlobOffset);
        current.Requests.push_back(r.BlockIndex);
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
            batch.GroupId);

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
    auto request =
        std::make_unique<TEvPartitionPrivate::TEvReadBlocksCompleted>(error);
    FillOperationCompleted(
        *request,
        RequestInfo,
        CommitId,
        RequestsCompleted,
        std::move(BlockInfos)
    );
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

void TReadBlocksActor::HandleReadBlobResponse(
    const TEvPartitionPrivate::TEvReadBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    RequestInfo->AddExecCycles(msg->ExecCycles);

    const auto& error = msg->GetError();
    if (HandleError(ctx, error)) {
        return;
    }

    ui32 batchIndex = ev->Cookie;

    Y_VERIFY(batchIndex < BatchRequests.size());
    auto& batch = BatchRequests[batchIndex];

    RequestsCompleted += batch.Requests.size();
    Y_VERIFY(RequestsCompleted <= RequestsScheduled);
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
    ReplyAndDie(ctx, std::move(response), {});
}

void TReadBlocksActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(E_REJECTED, "Tablet is dead");

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
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TReadBlocksVisitor final
    : public IFreshBlocksIndexVisitor
    , public IBlocksIndexVisitor
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
        : BlockDigestGenerator(std::move(blockDigestGenerator))
        , BlockSize(blockSize)
        , TabletInfo(tabletInfo)
        , Args(args)
    {}

    bool Visit(const TFreshBlock& block) override
    {
        if (Args.Interrupted) {
            return false;
        }

        if (Args.MarkBlock(block.Meta.BlockIndex, block.Meta.CommitId, TFreshMark())) {
            TBlockDataRef blockData = TBlockDataRef::CreateZeroBlock(BlockSize);
            if (!block.Content.Empty()) {
                blockData = TBlockDataRef(block.Content.Data(), block.Content.Size());
            }
            if (!Args.ReadHandler->SetBlock(
                    block.Meta.BlockIndex,
                    blockData,
                    false))
            {
                Args.Interrupted = true;
                return false;
            }

            const auto digest = BlockDigestGenerator->ComputeDigest(
                block.Meta.BlockIndex,
                blockData
            );

            if (digest.Defined()) {
                Args.BlockInfos.push_back({block.Meta.BlockIndex, *digest});
            }
        }

        return true;
    }

    bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        if (Args.Interrupted) {
            return false;
        }

        TBlockMark blockMark = TZeroMark();

        if (!IsDeletionMarker(blobId)) {
            const auto group = TabletInfo.GroupFor(
                blobId.Channel(), blobId.Generation());
            const auto logoBlobId = MakeBlobId(TabletInfo.TabletID, blobId);
            blockMark = TBlobMark(logoBlobId, group, blobOffset);
        }

        if (Args.MarkBlock(blockIndex, commitId, std::move(blockMark))) {
            // Will read from BS later.
            if (!Args.ReadHandler->SetBlock(blockIndex, {}, false)) {
                Args.Interrupted = true;
                return false;
            }
        }

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

TMaybe<TBlockRange32> ComputeDescribeBlocksRange(
    const TBlockRange32& readRange,
    const TBlockMarks& marks)
{
    Y_VERIFY_DEBUG(readRange.Size() == marks.size());

    const auto empty = [] (const auto& mark) {
        return std::holds_alternative<TEmptyMark>(mark);
    };

    const auto firstEmptyIter = FindIf(marks, empty);

    if (firstEmptyIter == marks.end()) {
        return {};
    }

    const auto lastEmptyReverseIter =
        FindIf(marks.rbegin(), marks.rend(), empty);
    const auto lastEmptyIndexFromEnd = lastEmptyReverseIter - marks.rbegin();
    const auto lastEmptyIndexFromBegin =
        marks.size() - 1 - lastEmptyIndexFromEnd;

    const auto startBlockIndex = readRange.Start;
    const auto firstEmptyIndexFromBegin = firstEmptyIter - marks.begin();
    const auto firstEmptyBlockIndex = startBlockIndex + firstEmptyIndexFromBegin;
    const auto lastEmptyBlockIndex = startBlockIndex + lastEmptyIndexFromBegin;

    return TBlockRange32(firstEmptyBlockIndex, lastEmptyBlockIndex);
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

    auto requestInfo = CreateRequestInfo<TMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "ReadBlocks",
        requestInfo->CallContext->RequestId);

    TBlockRange64 readRange;

    auto ok = InitReadWriteBlockRange(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount(),
        &readRange
    );

    if (!ok) {
        auto response = std::make_unique<typename TMethod::TResponse>(
            MakeError(E_ARGUMENT, TStringBuilder()
                << "invalid block range ["
                << "index: " << msg->Record.GetStartIndex()
                << ", count: " << msg->Record.GetBlocksCount()
                << "]"));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo->CallContext->LWOrbit,
            "ReadBlocks",
            requestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    auto commitId = GetCommitId(msg->Record);

    if (!commitId) {
        auto result = VerifyReadBlocksCheckpoint(
            ctx, msg->Record.GetCheckpointId(), *requestInfo, replyLocal);

        if (!result.Defined()) {
            return;
        }

        commitId = *result;
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

TMaybe<ui64> TPartitionActor::VerifyReadBlocksCheckpoint(
    const TActorContext& ctx,
    const TString& checkpointId,
    TRequestInfo& requestInfo,
    bool replyLocal)
{
    ui64 commitId = State->GetLastCommitId();

    if (checkpointId) {
        commitId = State->GetCheckpoints().GetCommitId(checkpointId, false);
        if (!commitId) {
            ui32 flags = 0;
            SetProtoFlag(flags, NProto::EF_SILENT);
            auto response = CreateReadBlocksResponse(
                replyLocal,
                MakeError(
                    E_NOT_FOUND,
                    TStringBuilder()
                        << "checkpoint not found: " << checkpointId.Quote(),
                    flags));

            LWTRACK(
                ResponseSent_Partition,
                requestInfo.CallContext->LWOrbit,
                "ReadBlocks",
                requestInfo.CallContext->RequestId);

            NCloud::Reply(ctx, requestInfo, std::move(response));
            return {};
        }
    }

    return TMaybe<ui64>(commitId);
}

void TPartitionActor::ReadBlocks(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    ui64 commitId,
    const TBlockRange32& readRange,
    IReadBlocksHandlerPtr readHandler,
    bool replyLocal)
{
    State->GetCleanupQueue().AcquireBarrier(commitId);

    LOG_TRACE(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Start read blocks @%lu (range: %s)",
        TabletID(),
        commitId,
        DescribeRange(readRange).data());

    AddTransaction(*requestInfo);

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

    for (const auto& entry: State->GetUnconfirmedBlobs()) {
        for (const auto& blob: entry.second) {
            if (blob.BlockRange.Overlaps(args.ReadRange)) {
                args.Interrupted = true;
                return true;
            }
        }
    }

    // NOTE: we should also look in confirmed blobs because they are not added
    // yet
    for (const auto& entry: State->GetConfirmedBlobs()) {
        for (const auto& blob: entry.second) {
            if (blob.BlockRange.Overlaps(args.ReadRange)) {
                args.Interrupted = true;
                return true;
            }
        }
    }

    ui64 commitId = args.CommitId;

    TReadBlocksVisitor visitor(
        BlockDigestGenerator,
        State->GetBlockSize(),
        *Info(),
        args
    );
    State->FindFreshBlocks(visitor, args.ReadRange, commitId);
    auto ready = db.FindMixedBlocks(
        visitor,
        args.ReadRange,
        false,  // precharge
        commitId
    );
    ready &= db.FindMergedBlocks(
        visitor,
        args.ReadRange,
        false,  // precharge
        State->GetMaxBlocksInBlob(),
        commitId
    );

    return ready;
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

    if (args.Interrupted) {
        auto response = CreateReadBlocksResponse(
            args.ReplyLocal,
            MakeError(E_REJECTED, "ReadBlocks transaction was interrupted")
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
                {}
            )
        );
        return;
    }

    TReadBlocksRequests requests(Reserve(args.BlockMarks.size()));

    ui32 blockIndex = args.ReadRange.Start;
    for (const auto& mark: args.BlockMarks) {
        if (std::holds_alternative<TBlobMark>(mark)) {
            const auto& value = std::get<TBlobMark>(mark);
            requests.emplace_back(
                value.BlobId,
                MakeBlobStorageProxyID(value.BSGroupId),
                value.BlobOffset,
                blockIndex,
                value.BSGroupId);
        }
        ++blockIndex;
    }

    TMaybe<TBlockRange32> describeBlocksRange;

    if (State->GetBaseDiskId()) {
        describeBlocksRange =
            ComputeDescribeBlocksRange(args.ReadRange, args.BlockMarks);
    }

    if (describeBlocksRange.Defined() || requests) {
        TBlockMarks marks;

        if (describeBlocksRange.Defined()) {
            marks = std::move(args.BlockMarks);
        }

        auto actor = NCloud::Register<TReadBlocksActor>(
            ctx,
            args.RequestInfo,
            BlockDigestGenerator,
            State->GetBlockSize(),
            State->GetCompactionMap().GetRangeSize(),
            State->GetBaseDiskId(),
            State->GetBaseDiskCheckpointId(),
            SelfId(),
            describeBlocksRange,
            args.ReadHandler,
            args.ReadRange,
            args.ReplyLocal,
            args.CommitId,
            std::move(marks),
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
    ui64 commitId = operation.CommitId;
    LOG_TRACE(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Complete read blocks @%lu",
        TabletID(),
        commitId);

    const auto& stats = operation.Stats;
    const auto& counters = stats.GetUserReadCounters();
    const auto blocksCount = counters.GetBlocksCount();

    UpdateStats(stats);

    const ui64 requestBytes = State->GetBlockSize() * blocksCount;

    UpdateNetworkStat(ctx.Now(), requestBytes);
    UpdateCPUUsageStat(CyclesToDurationSafe(operation.ExecCycles).MicroSeconds());
    UpdateExecutorStats(ctx);

    State->GetCleanupQueue().ReleaseBarrier(commitId);

    auto time = CyclesToDurationSafe(operation.TotalCycles).MicroSeconds();
    PartCounters->RequestCounters.ReadBlocks.AddRequest(time, requestBytes);

    if (operation.AffectedBlockInfos) {
        IProfileLog::TReadWriteRequestBlockInfos request;
        request.RequestType = EBlockStoreRequest::ReadBlocks;
        request.BlockInfos = std::move(operation.AffectedBlockInfos);
        request.CommitId = commitId;

        IProfileLog::TRecord record;
        record.DiskId = State->GetConfig().GetDiskId();
        record.Ts = ctx.Now();
        record.Request = std::move(request);

        ProfileLog->Write(std::move(record));
    }

    for (const auto& stat: operation.ReadStats) {
        State->GetCompactionMap().RegisterRead(
            stat.BlockIndex,
            stat.BlobCount,
            stat.BlockCount
        );
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
