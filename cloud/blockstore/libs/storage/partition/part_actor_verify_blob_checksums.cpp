#include "part_actor.h"

#include <cloud/blockstore/libs/storage/partition_common/actor_read_blob.h>

#include <cloud/storage/core/libs/common/alloc.h>
#include <cloud/storage/core/libs/common/block_buffer.h>
#include <cloud/storage/core/libs/common/guarded_sglist.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultMaxInFlight = 4;
constexpr auto DefaultDeadline = TDuration::Seconds(10);
constexpr ui32 MaxReportedMismatchesPerBlob = 16;
constexpr ui32 UnknownBlockIndex = Max<ui32>();

// Lets the reply at the deadline come first, so that a read cut short by the
// deadline is "not read" and not "read failed".
constexpr auto ReadDeadlineSlack = TDuration::Seconds(5);

using TBlobChecksumsResult = NProto::TBlobChecksumsResult;

////////////////////////////////////////////////////////////////////////////////

struct TBlockToVerify
{
    ui16 BlobOffset = 0;
    ui32 BlockIndex = UnknownBlockIndex;
    ui32 ExpectedChecksum = 0;
};

struct TBlobToVerify
{
    ui32 ResultIndex = 0;
    TLogoBlobID BlobId;
    ui32 BSGroupId = 0;
    std::optional<NProto::TCompressionMetadata> CompressionMetadata;
    TVector<TBlockToVerify> Blocks;
};

////////////////////////////////////////////////////////////////////////////////

class TMergedBlobSkipMaskVisitor final
    : public IBlobsVisitor
    , public IBlocksIndexVisitor
{
private:
    const TPartialBlobId BlobId;

public:
    std::optional<TBlockMask> SkipMask;

    explicit TMergedBlobSkipMaskVisitor(const TPartialBlobId& blobId)
        : BlobId(blobId)
    {}

    bool NeedsCompressionMetadata() const override
    {
        return false;
    }

    bool Visit(
        TBlockRange32 blockRange,
        const TPartialBlobId& blobId,
        const TBlockMask& skipMask,
        const std::optional<NProto::TCompressionMetadata>& compressionMetadata)
        override
    {
        Y_UNUSED(blockRange);
        Y_UNUSED(compressionMetadata);

        if (blobId != BlobId) {
            return true;
        }

        SkipMask = skipMask;
        return false;
    }

    bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        Y_UNUSED(blockIndex);
        Y_UNUSED(commitId);
        Y_UNUSED(blobId);
        Y_UNUSED(blobOffset);
        return true;
    }
};

// The meta of a merged blob doesn't say which blocks of its range it skips,
// only the merged blocks index does.
bool FindBlockIndices(
    TPartitionDatabase& db,
    ui32 maxBlocksInBlob,
    const TPartialBlobId& blobId,
    const NProto::TBlobMeta& blobMeta,
    TVector<ui32>& blockIndices)
{
    if (blobMeta.HasMixedBlocks()) {
        const auto& blocks = blobMeta.GetMixedBlocks().GetBlocks();
        blockIndices.assign(blocks.begin(), blocks.end());
        return true;
    }

    if (!blobMeta.HasMergedBlocks()) {
        return true;
    }

    const auto& mergedBlocks = blobMeta.GetMergedBlocks();
    const auto range = TBlockRange32::MakeClosedInterval(
        mergedBlocks.GetStart(),
        mergedBlocks.GetEnd());

    TBlockMask skipMask;
    if (mergedBlocks.GetSkipped()) {
        TMergedBlobSkipMaskVisitor visitor(blobId);
        const bool ready = db.FindMergedBlocks(
            visitor,
            visitor,
            range,
            true,   // precharge
            maxBlocksInBlob);
        if (!ready) {
            return false;
        }
        if (!visitor.SkipMask) {
            return true;
        }
        skipMask = *visitor.SkipMask;
    }

    for (ui32 blockIndex: xrange(range)) {
        if (!skipMask.Get(blockIndex - range.Start)) {
            blockIndices.push_back(blockIndex);
        }
    }

    return true;
}

TVector<TBlockToVerify> SelectBlocksToVerify(
    const NProto::TBlobMeta& blobMeta,
    const TVector<ui32>& blockIndices)
{
    TVector<TBlockToVerify> blocks;

    const auto& checksums = blobMeta.GetBlockChecksums();
    for (int blobOffset = 0; blobOffset < checksums.size(); ++blobOffset) {
        // Zero means "no checksum", as in VerifyBlockChecksum.
        if (!checksums[blobOffset]) {
            continue;
        }

        blocks.push_back({
            .BlobOffset = static_cast<ui16>(blobOffset),
            .BlockIndex = static_cast<size_t>(blobOffset) < blockIndices.size()
                              ? blockIndices[blobOffset]
                              : UnknownBlockIndex,
            .ExpectedChecksum = checksums[blobOffset],
        });
    }

    return blocks;
}

////////////////////////////////////////////////////////////////////////////////

// Reads the blobs itself instead of sending TEvReadBlobRequest to the tablet:
// the reads skip the channel IO queue with guest IO, and their errors don't
// count towards the tablet's MaxReadBlobErrorsBeforeSuicide.
class TVerifyBlobChecksumsActor final
    : public TActorBootstrapped<TVerifyBlobChecksumsActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TActorId Tablet;
    const ui64 TabletId;
    const ui32 BlockSize;
    const ui32 MaxInFlight;
    const TInstant Deadline;
    const TString LogTitle;
    const TVector<TBlobToVerify> Blobs;

    NProto::TVerifyBlobChecksumsResponse Response;

    ui32 NextBlobToRead = 0;
    ui32 ReadsInFlight = 0;
    THashMap<ui32, TGuardedBuffer<TBlockBuffer>> ReadBuffers;

public:
    TVerifyBlobChecksumsActor(
        TRequestInfoPtr requestInfo,
        const TActorId& tablet,
        ui64 tabletId,
        ui32 blockSize,
        ui32 maxInFlight,
        TInstant deadline,
        TString logTitle,
        TVector<TBlobToVerify> blobs,
        NProto::TVerifyBlobChecksumsResponse response);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReadBlobs(const TActorContext& ctx);
    void ReadBlob(const TActorContext& ctx, ui32 blobIndex);

    void VerifyChecksums(
        const TActorContext& ctx,
        const TBlobToVerify& blob,
        const TVector<ui32>& actualChecksums);

    void ReplyAndDie(const TActorContext& ctx, NProto::TError error = {});

private:
    STFUNC(StateWork);

    void HandleReadBlobResponse(
        const TEvPartitionCommonPrivate::TEvReadBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TVerifyBlobChecksumsActor::TVerifyBlobChecksumsActor(
    TRequestInfoPtr requestInfo,
    const TActorId& tablet,
    ui64 tabletId,
    ui32 blockSize,
    ui32 maxInFlight,
    TInstant deadline,
    TString logTitle,
    TVector<TBlobToVerify> blobs,
    NProto::TVerifyBlobChecksumsResponse response)
    : RequestInfo(std::move(requestInfo))
    , Tablet(tablet)
    , TabletId(tabletId)
    , BlockSize(blockSize)
    , MaxInFlight(maxInFlight)
    , Deadline(deadline)
    , LogTitle(std::move(logTitle))
    , Blobs(std::move(blobs))
    , Response(std::move(response))
{}

void TVerifyBlobChecksumsActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    if (Blobs.empty()) {
        ReplyAndDie(ctx);
        return;
    }

    ctx.Schedule(Deadline - ctx.Now(), new TEvents::TEvWakeup());
    ReadBlobs(ctx);
}

void TVerifyBlobChecksumsActor::ReadBlobs(const TActorContext& ctx)
{
    while (ReadsInFlight < MaxInFlight && NextBlobToRead < Blobs.size()) {
        ReadBlob(ctx, NextBlobToRead++);
    }
}

void TVerifyBlobChecksumsActor::ReadBlob(
    const TActorContext& ctx,
    ui32 blobIndex)
{
    const auto& blob = Blobs[blobIndex];

    TBlockBuffer buffer(TProfilingAllocator::Instance());
    TVector<ui16> blobOffsets;
    blobOffsets.reserve(blob.Blocks.size());
    for (const auto& block: blob.Blocks) {
        buffer.AddBlock(BlockSize, 0);
        blobOffsets.push_back(block.BlobOffset);
    }

    const auto& readBuffer =
        ReadBuffers
            .emplace(blobIndex, TGuardedBuffer<TBlockBuffer>(std::move(buffer)))
            .first->second;

    auto request =
        std::make_unique<TEvPartitionCommonPrivate::TEvReadBlobRequest>(
            blob.BlobId,
            MakeBlobStorageProxyID(blob.BSGroupId),
            std::move(blobOffsets),
            readBuffer.GetGuardedSgList(),
            blob.BSGroupId,
            true,   // async
            Deadline + ReadDeadlineSlack,
            true,   // shouldCalculateChecksums
            blob.CompressionMetadata);

    // The read reports completion to this actor, not to the tablet.
    NCloud::Register(
        ctx,
        std::make_unique<TReadBlobActor>(
            CreateRequestInfo(
                SelfId(),
                blobIndex,
                MakeIntrusive<TCallContext>()),
            SelfId(),   // partitionActorId
            SelfId(),   // volumeActorId
            TabletId,
            BlockSize,
            true,   // shouldCalculateChecksums
            EStorageAccessMode::Default,
            std::move(request),
            TDuration::Max(),   // longRunningThreshold
            0,                  // bsGroupOperationId
            false));            // passTraceIdToBlobstorage

    ++ReadsInFlight;
}

void TVerifyBlobChecksumsActor::VerifyChecksums(
    const TActorContext& ctx,
    const TBlobToVerify& blob,
    const TVector<ui32>& actualChecksums)
{
    auto& result = *Response.MutableResults(blob.ResultIndex);

    if (actualChecksums.size() != blob.Blocks.size()) {
        result.SetStatus(TBlobChecksumsResult::READ_FAILED);
        *result.MutableError() = MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "read returned " << actualChecksums.size()
                << " checksums for " << blob.Blocks.size() << " blocks");
        return;
    }

    ui32 mismatchedBlockCount = 0;
    for (ui32 i = 0; i < blob.Blocks.size(); ++i) {
        const auto& block = blob.Blocks[i];
        if (actualChecksums[i] == block.ExpectedChecksum) {
            continue;
        }

        if (++mismatchedBlockCount <= MaxReportedMismatchesPerBlob) {
            auto& mismatch = *result.AddMismatches();
            mismatch.SetBlockIndex(block.BlockIndex);
            mismatch.SetBlobOffset(block.BlobOffset);
            mismatch.SetExpectedChecksum(block.ExpectedChecksum);
            mismatch.SetActualChecksum(actualChecksums[i]);
        }
    }

    if (!mismatchedBlockCount) {
        result.SetStatus(TBlobChecksumsResult::VERIFIED);
        return;
    }

    result.SetStatus(TBlobChecksumsResult::MISMATCH);
    result.SetMismatchedBlockCount(mismatchedBlockCount);

    LOG_WARN(
        ctx,
        TBlockStoreComponents::PARTITION_WORKER,
        "%s VerifyBlobChecksums: %u blocks of blob %s mismatch their "
        "checksums",
        LogTitle.c_str(),
        mismatchedBlockCount,
        blob.BlobId.ToString().c_str());
}

void TVerifyBlobChecksumsActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response =
        HasError(error)
            ? std::make_unique<TEvVolume::TEvVerifyBlobChecksumsResponse>(
                  std::move(error))
            : std::make_unique<TEvVolume::TEvVerifyBlobChecksumsResponse>(
                  std::move(Response));

    LWTRACK(
        ResponseSent_Partition,
        RequestInfo->CallContext->LWOrbit,
        "VerifyBlobChecksums",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    NCloud::Send(
        ctx,
        Tablet,
        std::make_unique<
            TEvPartitionPrivate::TEvVerifyBlobChecksumsCompleted>());

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TVerifyBlobChecksumsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvPartitionCommonPrivate::TEvReadBlobResponse,
            HandleReadBlobResponse);
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        IgnoreFunc(TEvPartitionCommonPrivate::TEvReadBlobCompleted);
        IgnoreFunc(TEvPartitionCommonPrivate::TEvLongRunningOperation);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TVerifyBlobChecksumsActor::HandleReadBlobResponse(
    const TEvPartitionCommonPrivate::TEvReadBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const ui32 blobIndex = ev->Cookie;
    const auto& blob = Blobs[blobIndex];

    --ReadsInFlight;
    ReadBuffers.erase(blobIndex);

    if (HasError(msg->GetError())) {
        // TReadBlobActor fails every read with E_REJECTED, which the client
        // would retry, so the error belongs to the blob and not the request.
        auto& result = *Response.MutableResults(blob.ResultIndex);
        result.SetStatus(TBlobChecksumsResult::READ_FAILED);
        *result.MutableError() = msg->GetError();

        LOG_WARN(
            ctx,
            TBlockStoreComponents::PARTITION_WORKER,
            "%s VerifyBlobChecksums: failed to read blob %s: %s",
            LogTitle.c_str(),
            blob.BlobId.ToString().c_str(),
            FormatError(msg->GetError()).c_str());
    } else {
        VerifyChecksums(ctx, blob, msg->BlockChecksums);
    }

    if (!ReadsInFlight && NextBlobToRead == Blobs.size()) {
        ReplyAndDie(ctx);
        return;
    }

    ReadBlobs(ctx);
}

void TVerifyBlobChecksumsActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    // The blobs not read by now keep NOT_READ.
    ReplyAndDie(ctx);
}

void TVerifyBlobChecksumsActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleVerifyBlobChecksums(
    const TEvVolume::TEvVerifyBlobChecksumsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvVolume::TVerifyBlobChecksumsMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "VerifyBlobChecksums",
        requestInfo->CallContext->RequestId);

    const auto& record = msg->Record;

    TVector<TLogoBlobID> blobIds;
    blobIds.reserve(record.BlobIdsSize());
    for (const auto& blobId: record.GetBlobIds()) {
        blobIds.push_back(LogoBlobIDFromLogoBlobID(blobId));
    }

    const ui32 maxInFlight =
        record.GetMaxInFlight() ? record.GetMaxInFlight() : DefaultMaxInFlight;
    const auto deadline =
        ctx.Now() + (record.GetDeadlineMs()
                         ? TDuration::MilliSeconds(record.GetDeadlineMs())
                         : DefaultDeadline);

    AddTransaction<TEvVolume::TVerifyBlobChecksumsMethod>(*requestInfo);

    ExecuteTx(
        ctx,
        CreateTx<TVerifyBlobChecksums>(
            requestInfo,
            blobIds,
            maxInFlight,
            deadline));
}

void TPartitionActor::HandleVerifyBlobChecksumsCompleted(
    const TEvPartitionPrivate::TEvVerifyBlobChecksumsCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    Actors.Erase(ev->Sender);
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareVerifyBlobChecksums(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TVerifyBlobChecksums& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);

    TPartitionDatabase db(tx.DB);

    bool ready = true;
    for (auto& blob: args.Blobs) {
        // Blobs of other tablets, of a base disk for example, can't be in
        // this blob index.
        if (blob.BlobId.TabletID() != TabletID()) {
            continue;
        }

        const auto blobId = MakePartialBlobId(blob.BlobId);
        if (!db.ReadBlobMeta(blobId, blob.BlobMeta)) {
            ready = false;
            continue;
        }

        if (!blob.BlobMeta || !blob.BlobMeta->BlockChecksumsSize()) {
            continue;
        }

        ready &= FindBlockIndices(
            db,
            State->GetMaxBlocksInBlob(),
            blobId,
            *blob.BlobMeta,
            blob.BlockIndices);
    }

    return ready;
}

void TPartitionActor::ExecuteVerifyBlobChecksums(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TVerifyBlobChecksums& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteVerifyBlobChecksums(
    const TActorContext& ctx,
    TTxPartition::TVerifyBlobChecksums& args)
{
    TRequestScope timer(*args.RequestInfo);

    RemoveTransaction(*args.RequestInfo);

    NProto::TVerifyBlobChecksumsResponse response;
    TVector<TBlobToVerify> blobsToVerify;

    for (const auto& blob: args.Blobs) {
        auto& result = *response.AddResults();
        LogoBlobIDFromLogoBlobID(blob.BlobId, result.MutableBlobId());

        if (!blob.BlobMeta) {
            result.SetStatus(TBlobChecksumsResult::NOT_IN_INDEX);
            continue;
        }

        auto blocks = SelectBlocksToVerify(*blob.BlobMeta, blob.BlockIndices);
        if (blocks.empty()) {
            result.SetStatus(TBlobChecksumsResult::VERIFIED);
            continue;
        }

        std::optional<NProto::TCompressionMetadata> compressionMetadata;
        const auto& mergedBlocks = blob.BlobMeta->GetMergedBlocks();
        if (mergedBlocks.HasCompressionMetadata()) {
            compressionMetadata = mergedBlocks.GetCompressionMetadata();
        }

        result.SetStatus(TBlobChecksumsResult::NOT_READ);
        blobsToVerify.push_back({
            .ResultIndex = static_cast<ui32>(response.ResultsSize() - 1),
            .BlobId = blob.BlobId,
            .BSGroupId = Info()->GroupFor(
                blob.BlobId.Channel(),
                blob.BlobId.Generation()),
            .CompressionMetadata = std::move(compressionMetadata),
            .Blocks = std::move(blocks),
        });
    }

    const auto actorId = NCloud::Register(
        ctx,
        std::make_unique<TVerifyBlobChecksumsActor>(
            args.RequestInfo,
            SelfId(),
            TabletID(),
            State->GetBlockSize(),
            args.MaxInFlight,
            args.Deadline,
            LogTitle.GetWithTime(),
            std::move(blobsToVerify),
            std::move(response)));

    Actors.Insert(actorId);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
