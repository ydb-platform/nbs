#include "actor_read_blocks_from_base_disk.h"

#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NBlobMarkers;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

TReadBlocksFromBaseDiskActor::TReadBlocksFromBaseDiskActor(
        TRequestInfoPtr requestInfo,
        TString baseDiskId,
        TString baseDiskCheckpointId,
        TBlockRange64 blocksRange,
        TBlockRange64 baseDiskBlocksRange,
        TBlockMarks blockMarks,
        ui32 blockSize)
    : RequestInfo(std::move(requestInfo))
    , BaseDiskId(std::move(baseDiskId))
    , BaseDiskCheckpointId(std::move(baseDiskCheckpointId))
    , BlocksRange(std::move(blocksRange))
    , BaseDiskBlocksRange(std::move(baseDiskBlocksRange))
    , BlockSize(blockSize)
    , BlockMarks(std::move(blockMarks))
{
    Y_VERIFY_DEBUG(BaseDiskBlocksRange.Size());
    Y_VERIFY_DEBUG(BaseDiskId);
    Y_VERIFY_DEBUG(BaseDiskCheckpointId);
    Y_VERIFY_DEBUG(BlocksRange.Size());
    Y_VERIFY_DEBUG(BlockMarks.size() == BlocksRange.Size());
    Y_VERIFY_DEBUG(BlocksRange.Contains(BaseDiskBlocksRange));
}

void TReadBlocksFromBaseDiskActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "ReadBlocks",
        RequestInfo->CallContext->RequestId);

    DescribeBlocks(ctx);
}

void TReadBlocksFromBaseDiskActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "ReadBlocks",
        RequestInfo->CallContext->RequestId);

    using TEvent = TEvPartitionCommonPrivate::TEvBaseDiskDescribeCompleted;
    auto response = std::make_unique<TEvent>(
        std::move(error),
        std::move(BlockMarks));

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TReadBlocksFromBaseDiskActor::DescribeBlocks(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvDescribeBlocksRequest>();

    request->Record.SetStartIndex(BaseDiskBlocksRange.Start);
    request->Record.SetBlocksCount(BaseDiskBlocksRange.Size());
    request->Record.SetDiskId(BaseDiskId);
    request->Record.SetCheckpointId(BaseDiskCheckpointId);
    request->Record.SetBlocksCountToRead(
        CountIf(BlockMarks, [](const auto& mark)
            { return std::holds_alternative<TEmptyMark>(mark); }));

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request));
}

NProto::TError TReadBlocksFromBaseDiskActor::ValidateDescribeBlocksResponse(
    const TEvVolume::TEvDescribeBlocksResponse& response)
{
    const auto& record = response.Record;

    for (const auto& range: record.GetFreshBlockRanges()) {
        const auto rangeToValidate =
            TBlockRange64::WithLength(
                range.GetStartIndex(), range.GetBlocksCount());

        if (!BaseDiskBlocksRange.Contains(rangeToValidate)) {
            return MakeError(
                E_FAIL,
                TStringBuilder() <<
                "DescribeBlocks error. Fresh block is out of range"
                " BaseDiskBlocksRange: " << DescribeRange(BaseDiskBlocksRange) <<
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
            const auto blockRange = TBlockRange64::WithLength(
                range.GetBlockIndex(),
                range.GetBlocksCount());
            if (!BaseDiskBlocksRange.Contains(blockRange)) {
                return MakeError(
                    E_FAIL,
                    TStringBuilder() <<
                    "DescribeBlocks error. Blob range is out of bounds."
                    " BaseDiskBlocksRange: " << DescribeRange(BaseDiskBlocksRange) <<
                    " blockRange:" << DescribeRange(blockRange)
                );
            }
        }
    }

    return {};
}

void TReadBlocksFromBaseDiskActor::ProcessDescribeBlocksResponse(
    TEvVolume::TEvDescribeBlocksResponse&& response)
{
    const auto startIndex = BlocksRange.Start;
    auto& record = response.Record;

    for (auto&& range : std::move(*record.MutableFreshBlockRanges())) {
        auto sharedRange = std::make_shared<NProto::TFreshBlockRange>(std::move(range));
        for (size_t index = 0; index < sharedRange->GetBlocksCount(); ++index) {
            const auto blockIndex = sharedRange->GetStartIndex() + index;
            const auto blockMarkIndex = blockIndex - startIndex;

            if (std::holds_alternative<TEmptyMark>(BlockMarks[blockMarkIndex])) {
                const char* startingByte =
                    sharedRange->GetBlocksContent().data() + index * BlockSize;

                BlockMarks[blockMarkIndex] = TFreshMarkOnBaseDisk(
                    sharedRange,
                    TBlockDataRef(startingByte, BlockSize),
                    blockIndex);
            }
        }
    }

    for (const auto& piece: record.GetBlobPieces()) {
        const auto& blobId = LogoBlobIDFromLogoBlobID(piece.GetBlobId());
        const auto group = piece.GetBSGroupId();

        for (const auto& range: piece.GetRanges()) {
            for (size_t i = 0; i < range.GetBlocksCount(); ++i) {
                const auto blobOffset = range.GetBlobOffset() + i;
                const auto blockIndex = range.GetBlockIndex() + i;
                const auto blockMarkIndex = blockIndex - startIndex;

                if (std::holds_alternative<TEmptyMark>(BlockMarks[blockMarkIndex])) {
                    BlockMarks[blockMarkIndex] = TBlobMarkOnBaseDisk(
                        blobId,
                        blockIndex,
                        group,
                        blobOffset);
                }
            }
        }
    }
}

void TReadBlocksFromBaseDiskActor::HandleDescribeBlocksResponse(
    const TEvVolume::TEvDescribeBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (auto error = msg->GetError();
        FAILED(error.GetCode()))
    {
        return ReplyAndDie(ctx, error);
    }

    if (auto error = ValidateDescribeBlocksResponse(msg->Record);
        FAILED(error.GetCode()))
    {
        return ReplyAndDie(ctx, error);
    }

    ProcessDescribeBlocksResponse(std::move(msg->Record));
    return ReplyAndDie(ctx);
}

void TReadBlocksFromBaseDiskActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeError(E_REJECTED, "Tablet is dead"));
}

STFUNC(TReadBlocksFromBaseDiskActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvVolume::TEvDescribeBlocksResponse, HandleDescribeBlocksResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_COMMON);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage

