#include "part_actor.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/partition_common/actor_describe_base_disk_blocks.h>

#include <cloud/storage/core/libs/common/helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NBlobMarkers;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;
using namespace NTabletFlatExecutor::NFlatExecutorSetup;

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

    for (const auto& r: requests) {
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
    const NProto::TError& error,
    TVector<TString>&& failedBlobs)
{
    if (replyLocal) {
        auto response =
            std::make_unique<TEvService::TEvReadBlocksLocalResponse>(error);
        response->Record.FailInfo.FailedRanges = std::move(failedBlobs);
        return response;
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
        TVector<ui32> Checksums;
        TVector<ui64> Requests; // block indices, ui64 needed for integration
                                // with sglist-related code
        ui32 GroupId = 0;

        TBatchRequest() = default;

        TBatchRequest(
                const NKikimr::TLogoBlobID& blobId,
                TActorId proxy,
                TVector<ui16> blobOffsets,
                TVector<ui32> checksums,
                TVector<ui64> requests,
                ui32 groupId)
            : BlobId(blobId)
            , Proxy(proxy)
            , BlobOffsets(std::move(blobOffsets))
            , Checksums(std::move(checksums))
            , Requests(std::move(requests))
            , GroupId(groupId)
        {}
    };

private:
    const TRequestInfoPtr RequestInfo;

    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const ui32 BlockSize;
    const ui32 CompactionRangeSize;

    const TActorId Tablet;

    const IReadBlocksHandlerPtr ReadHandler;
    const TBlockRange32 ReadRange;
    const bool ReplyLocal;
    const bool ChecksumsEnabled;
    const bool ReportBlobIdsOnFailure;

    const ui64 CommitId;

    TVector<TCallContextPtr> ForkedCallContexts;

    TReadBlocksRequests OwnRequests;
    TStackVec<TRangeReadStats, 2> ReadStats;
    TVector<IProfileLog::TBlockInfo> BlockInfos;
    TVector<TBatchRequest> BatchRequests;
    size_t RequestsScheduled = 0;
    size_t RequestsCompleted = 0;

    bool WaitBaseDiskRequests = false;

    TVector<TString> FailedBlobs;

    const TString DiskId;

public:
    TReadBlocksActor(
        TRequestInfoPtr requestInfo,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ui32 blockSize,
        ui32 compactionRangeSize,
        const TActorId& tablet,
        IReadBlocksHandlerPtr readHandler,
        const TBlockRange32& readRange,
        bool replyLocal,
        bool checksumsEnabled,
        bool reportBlobIdsOnFailure,
        ui64 commitId,
        TReadBlocksRequests ownRequests,
        TVector<IProfileLog::TBlockInfo> blockInfos,
        bool waitBaseDiskRequests,
        TString  diskId);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReadBlocks(
        const TActorContext& ctx,
        TReadBlocksRequests requests,
        bool baseDisk);

    void NotifyCompleted(const TActorContext& ctx, const NProto::TError& error);

    bool HandleError(
        const TActorContext& ctx,
        const NProto::TError& error,
        const TBatchRequest& batch);

    bool HandleError(
        const TActorContext& ctx,
        const NProto::TError& error);

    void ReplyAndDie(
        const TActorContext& ctx,
        IEventBasePtr response,
        const NProto::TError& error);

    bool VerifyChecksums(
        const TActorContext& ctx,
        const TVector<ui32>& actualChecksums,
        const TBatchRequest& batch);

private:
    STFUNC(StateWork);

    void HandleReadBlobResponse(
        const TEvPartitionCommonPrivate::TEvReadBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDescribeBlocksCompleted(
        const TEvPartitionCommonPrivate::TEvDescribeBlocksCompleted::TPtr& ev,
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
        const TActorId& tablet,
        IReadBlocksHandlerPtr readHandler,
        const TBlockRange32& readRange,
        bool replyLocal,
        bool checksumsEnabled,
        bool reportBlobIdsOnFailure,
        ui64 commitId,
        TReadBlocksRequests ownRequests,
        TVector<IProfileLog::TBlockInfo> blockInfos,
        bool waitBaseDiskRequests,
        TString  diskId)
    : RequestInfo(std::move(requestInfo))
    , BlockDigestGenerator(blockDigestGenerator)
    , BlockSize(blockSize)
    , CompactionRangeSize(compactionRangeSize)
    , Tablet(tablet)
    , ReadHandler(std::move(readHandler))
    , ReadRange(readRange)
    , ReplyLocal(replyLocal)
    , ChecksumsEnabled(checksumsEnabled)
    , ReportBlobIdsOnFailure(reportBlobIdsOnFailure)
    , CommitId(commitId)
    , OwnRequests(std::move(ownRequests))
    , BlockInfos(std::move(blockInfos))
    , WaitBaseDiskRequests(waitBaseDiskRequests)
    , DiskId(std::move(diskId))
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

    Sort(OwnRequests, TCompareByBlobIdAndOffset());
    FillReadStats(OwnRequests, CompactionRangeSize, &ReadStats);
    ReadBlocks(ctx, std::move(OwnRequests), false);
}

void TReadBlocksActor::ReadBlocks(
    const TActorContext& ctx,
    TReadBlocksRequests requests,
    bool baseDisk)
{
    const ui32 batchRequestsOldSize = BatchRequests.size();
    Y_DEBUG_ABORT_UNLESS(IsSorted(
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
                    std::move(current.Checksums),
                    std::move(current.Requests),
                    current.GroupId);
            }
            current.BlobId = r.BlobId;
            current.Proxy = r.BSProxy;
            current.GroupId = r.GroupId;
        }

        current.BlobOffsets.push_back(r.BlobOffset);
        current.Checksums.push_back(r.BlockChecksum);
        current.Requests.push_back(r.BlockIndex);
    }

    if (current.BlobOffsets) {
        BatchRequests.emplace_back(
            current.BlobId,
            current.Proxy,
            std::move(current.BlobOffsets),
            std::move(current.Checksums),
            std::move(current.Requests),
            current.GroupId);
    }

    for (ui32 batchIndex = batchRequestsOldSize;
            batchIndex < BatchRequests.size(); ++batchIndex)
    {
        auto& batch = BatchRequests[batchIndex];

        RequestsScheduled += batch.Requests.size();

        auto request = std::make_unique<TEvPartitionCommonPrivate::TEvReadBlobRequest>(
            batch.BlobId,
            batch.Proxy,
            batch.BlobOffsets,
            ReadHandler->GetGuardedSgList(batch.Requests, baseDisk),
            batch.GroupId,
            false,           // async
            TInstant::Max(), // deadline
            ChecksumsEnabled);

        if (!RequestInfo->CallContext->LWOrbit.Fork(request->CallContext->LWOrbit)) {
            LWTRACK(
                ForkFailed,
                RequestInfo->CallContext->LWOrbit,
                "TEvPartitionCommonPrivate::TEvReadBlobRequest",
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
    const NProto::TError& error,
    const TBatchRequest& batch)
{
    if (!HasError(error)) {
        return false;
    }

    if (ReportBlobIdsOnFailure && ReplyLocal) {
        FailedBlobs.emplace_back(batch.BlobId.ToString());

        // check range should try to read all the data and report broken blobs
        if (RequestsCompleted < RequestsScheduled || WaitBaseDiskRequests) {
            return false;
        }
    }

    auto response =
        CreateReadBlocksResponse(ReplyLocal, error, std::move(FailedBlobs));
    ReplyAndDie(ctx, std::move(response), error);
    return true;
}

bool TReadBlocksActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (FAILED(error.GetCode())) {
        auto response =
            CreateReadBlocksResponse(ReplyLocal, error, std::move(FailedBlobs));
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

bool TReadBlocksActor::VerifyChecksums(
    const TActorContext& ctx,
    const TVector<ui32>& actualChecksums,
    const TBatchRequest& batch)
{
    const auto n = Min(batch.Requests.size(), actualChecksums.size());
    for (ui32 i = 0; i < n; ++i) {
        auto error = VerifyBlockChecksum(
            actualChecksums[i],
            batch.BlobId,
            batch.Requests[i],
            batch.BlobOffsets[i],
            batch.Checksums[i],
            DiskId);

        if (HasError(error)) {
            HandleError(ctx, error);
            return false;
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TReadBlocksActor::HandleReadBlobResponse(
    const TEvPartitionCommonPrivate::TEvReadBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    RequestInfo->AddExecCycles(msg->ExecCycles);

    const size_t batchIndex = ev->Cookie;

    Y_ABORT_UNLESS(batchIndex < BatchRequests.size());
    auto& batch = BatchRequests[batchIndex];

    RequestsCompleted += batch.Requests.size();
    Y_ABORT_UNLESS(RequestsCompleted <= RequestsScheduled);

    const auto& error = msg->GetError();
    if (HandleError(ctx, error, batch)) {
        return;
    }

    if (!VerifyChecksums(ctx, msg->BlockChecksums, batch)) {
        return;
    }

    if (RequestsCompleted < RequestsScheduled) {
        return;
    }

    if (WaitBaseDiskRequests) {
        return;
    }

    for (auto& context: ForkedCallContexts) {
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

void TReadBlocksActor::HandleDescribeBlocksCompleted(
    const TEvPartitionCommonPrivate::TEvDescribeBlocksCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (HandleError(ctx, error)) {
        return;
    }

    WaitBaseDiskRequests = false;

    TReadBlocksRequests requests;
    for (auto&& blockMark: std::move(msg->BlockMarks)) {
        if (std::holds_alternative<TFreshMarkOnBaseDisk>(blockMark)) {
            auto& value = std::get<TFreshMarkOnBaseDisk>(blockMark);
            ReadHandler->SetBlock(
                value.BlockIndex,
                std::move(value.RefToData),
                true);
        }
        if (std::holds_alternative<TBlobMarkOnBaseDisk>(blockMark)) {
            auto& value = std::get<TBlobMarkOnBaseDisk>(blockMark);
            requests.emplace_back(
                value.BlobId,
                MakeBlobStorageProxyID(value.BSGroupId),
                value.BlobOffset,
                value.BlockIndex,
                value.BSGroupId,
                0 /* checksum */);
        }
    }

    if (requests.empty()) {
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

    Sort(requests, TCompareByBlobIdAndOffset());
    ReadBlocks(ctx, std::move(requests), true);
}

void TReadBlocksActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(E_REJECTED, "tablet is shutting down");

    auto response =
        CreateReadBlocksResponse(ReplyLocal, error, std::move(FailedBlobs));
    ReplyAndDie(ctx, std::move(response), error);
}

STFUNC(TReadBlocksActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvPartitionCommonPrivate::TEvReadBlobResponse,
            HandleReadBlobResponse);
        HFunc(TEvPartitionCommonPrivate::TEvDescribeBlocksCompleted,
            HandleDescribeBlocksCompleted);

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
            if (!block.Content.empty()) {
                blockData = TBlockDataRef(block.Content.data(), block.Content.size());
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

TMaybe<TBlockRange64> ComputeDescribeBlocksRange(
    const TBlockRange32& readRange,
    const TBlockMarks& marks)
{
    Y_DEBUG_ABORT_UNLESS(readRange.Size() == marks.size());

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

    return TBlockRange64::MakeClosedInterval(
        firstEmptyBlockIndex,
        lastEmptyBlockIndex);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleDescribeBlocksCompleted(
    const TEvPartitionCommonPrivate::TEvDescribeBlocksCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    Actors.Erase(ev->Sender);
}

void TPartitionActor::HandleReadBlocks(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    HandleReadBlocksRequest<TEvService::TReadBlocksMethod>(
        ev,
        ctx,
        false,    // replyLocal
        false);   // shouldReportFailedRangesOnFailure
}

void TPartitionActor::HandleReadBlocksLocal(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    HandleReadBlocksRequest<TEvService::TReadBlocksLocalMethod>(
        ev,
        ctx,
        true,   // replyLocal
        ev->Get()->Record.ShouldReportFailedRangesOnFailure);
}

template <typename TMethod>
void TPartitionActor::HandleReadBlocksRequest(
    const typename TMethod::TRequest::TPtr& ev,
    const TActorContext& ctx,
    bool replyLocal,
    bool shouldReportBlobIdsOnFailure)
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
        replyLocal,
        shouldReportBlobIdsOnFailure);
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
                    flags),
                {});   // failedBlobs

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
    bool replyLocal,
    bool shouldReportBlobIdsOnFailure)
{
    State->GetCleanupQueue().AcquireBarrier(commitId);

    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Start read blocks @%lu (range: %s)",
        LogTitle.GetWithTime().c_str(),
        commitId,
        DescribeRange(readRange).c_str());

    AddTransaction(*requestInfo, requestInfo->CancelRoutine);

    ExecuteTx(
        ctx,
        CreateTx<TReadBlocks>(
            requestInfo,
            commitId,
            readRange,
            std::move(readHandler),
            replyLocal,
            shouldReportBlobIdsOnFailure));
}

void TPartitionActor::HandleReadBlocksCompleted(
    const TEvPartitionPrivate::TEvReadBlocksCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    Actors.Erase(ev->Sender);

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

    ui64 commitId = args.CommitId;

    if (State->OverlapsUnconfirmedBlobs(0, commitId, args.ReadRange)) {
        args.Interrupted = true;
        return true;
    }

    // NOTE: we should also look in confirmed blobs because they are not added
    // yet
    if (State->OverlapsConfirmedBlobs(0, commitId, args.ReadRange)) {
        args.Interrupted = true;
        return true;
    }

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

    const ui32 checksumBoundary =
        Config->GetDiskPrefixLengthWithBlockChecksumsInBlobs()
        / State->GetBlockSize();
    args.ChecksumsEnabled = args.ReadRange.Start < checksumBoundary
        && Config->GetCheckBlockChecksumsInBlobsUponRead();

    if (args.ChecksumsEnabled) {
        for (auto& mark: args.BlockMarks) {
            if (!std::holds_alternative<TBlobMark>(mark)) {
                continue;
            }

            const auto& value = std::get<TBlobMark>(mark);

            TMaybe<NProto::TBlobMeta> meta;
            auto blobId = MakePartialBlobId(value.BlobId);
            if (db.ReadBlobMeta(blobId, meta)) {
                Y_ABORT_UNLESS(meta.Defined(),
                    "Could not read blob meta for blob: %s",
                    ToString(value.BlobId).data());
            } else {
                ready = false;
            }

            if (ready) {
                args.BlobId2Meta[blobId] = std::move(meta.GetRef());
            }
        }
    }

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
        ui32 flags = 0;
        SetProtoFlag(flags, NProto::EF_SILENT);
        auto error = MakeError(
            E_REJECTED,
            "ReadBlocks transaction was interrupted",
            flags);
        auto response = CreateReadBlocksResponse(
            args.ReplyLocal,
            error,
            {});   // failedBlobs

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

            ui32 checksum = 0;

            if (args.ChecksumsEnabled) {
                const auto* meta =
                    args.BlobId2Meta.FindPtr(MakePartialBlobId(value.BlobId));
                Y_DEBUG_ABORT_UNLESS(meta);

                if (meta && value.BlobOffset < meta->BlockChecksumsSize()) {
                    checksum = meta->GetBlockChecksums(value.BlobOffset);
                }
            }

            requests.emplace_back(
                value.BlobId,
                MakeBlobStorageProxyID(value.BSGroupId),
                value.BlobOffset,
                blockIndex,
                value.BSGroupId,
                checksum);
        }
        ++blockIndex;
    }

    TMaybe<TBlockRange64> describeBlocksRange;

    if (State->GetBaseDiskId()) {
        describeBlocksRange =
            ComputeDescribeBlocksRange(args.ReadRange, args.BlockMarks);
    }

    if (describeBlocksRange.Defined() || requests) {
        const auto readBlocksActorId = NCloud::Register<TReadBlocksActor>(
            ctx,
            args.RequestInfo,
            BlockDigestGenerator,
            State->GetBlockSize(),
            State->GetCompactionMap().GetRangeSize(),
            SelfId(),
            args.ReadHandler,
            args.ReadRange,
            args.ReplyLocal,
            args.ChecksumsEnabled,
            args.ShouldReportBlobIdsOnFailure,
            args.CommitId,
            std::move(requests),
            std::move(args.BlockInfos),
            describeBlocksRange.Defined(),
            State->GetBaseDiskId());
        Actors.Insert(readBlocksActorId);

        if (describeBlocksRange.Defined()) {
            auto requestInfo = CreateRequestInfo(
                readBlocksActorId,
                args.RequestInfo->Cookie,
                args.RequestInfo->CallContext);

            const auto describeBlocksActorId =
                NCloud::Register<TDescribeBaseDiskBlocksActor>(
                    ctx,
                    requestInfo,
                    State->GetBaseDiskId(),
                    State->GetBaseDiskCheckpointId(),
                    ConvertRangeSafe(args.ReadRange),
                    *describeBlocksRange,
                    std::move(args.BlockMarks),
                    State->GetBlockSize(),
                    SelfId());

            Actors.Insert(describeBlocksActorId);
        }

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
    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Finalizing ReadBlocks @%lu",
        LogTitle.GetWithTime().c_str(),
        commitId);

    const auto& stats = operation.Stats;
    const auto& counters = stats.GetUserReadCounters();
    const auto blocksCount = counters.GetBlocksCount();

    UpdateStats(stats);

    const ui64 requestBytes = State->GetBlockSize() * blocksCount;

    UpdateCPUUsageStat(ctx.Now(), operation.ExecCycles);

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
