#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/tablet/model/block.h>
#include <cloud/filestore/libs/storage/tablet/model/block_buffer.h>
#include <cloud/filestore/libs/storage/tablet/model/range.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReadDataActor final: public TActorBootstrapped<TReadDataActor>
{
private:
    // Original request arguments
    const TRequestInfoPtr RequestInfo;
    NProto::THeaders Headers;
    const TString FileSystemId;
    const ui64 NodeId;
    const ui64 Handle;
    const ui64 Offset;
    const ui64 Length;

    // Filesystem-specific params
    const ui32 BlockSize;

    // Response data
    const TByteRange OriginByteRange;
    const TByteRange AlignedByteRange;
    IBlockBufferPtr BlockBuffer;
    NProtoPrivate::TDescribeDataResponse DescribeResponse;
    ui32 RemainingBlobsToRead;

public:
    TReadDataActor(
        TRequestInfoPtr requestInfo,
        const TEvService::TEvReadDataRequest::TPtr& ev,
        ui32 blockSize);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void DescribeData(const TActorContext& ctx);

    void HandleDescribeDataResponse(
        const TEvIndexTablet::TEvDescribeDataResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReadBlobIfNeeded(const TActorContext& ctx);

    void HandleReadBlobResponse(
        const TEvBlobStorage::TEvGetResult::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});
};

////////////////////////////////////////////////////////////////////////////////

TReadDataActor::TReadDataActor(
    TRequestInfoPtr requestInfo,
    const TEvService::TEvReadDataRequest::TPtr& ev,
    ui32 blockSize)
    : RequestInfo(std::move(requestInfo))
    , FileSystemId(ev->Get()->Record.GetFileSystemId())
    , NodeId(ev->Get()->Record.GetNodeId())
    , Handle(ev->Get()->Record.GetHandle())
    , Offset(ev->Get()->Record.GetOffset())
    , Length(ev->Get()->Record.GetLength())
    , BlockSize(blockSize)
    , OriginByteRange(Offset, Length, BlockSize)
    , AlignedByteRange(OriginByteRange.AlignedSuperRange())
    , BlockBuffer(CreateBlockBuffer(AlignedByteRange))
    , RemainingBlobsToRead(0)
{
    const auto& record = ev->Get()->Record;

    Headers.CopyFrom(record.GetHeaders());
}

void TReadDataActor::Bootstrap(const TActorContext& ctx)
{
    DescribeData(ctx);
    Become(&TThis::StateWork);
}

void TReadDataActor::DescribeData(const TActorContext& ctx)
{
    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "Executing DescribeData for %lu, %lu, %lu, %lu",
        NodeId,
        Handle,
        Offset,
        Length);

    auto request = std::make_unique<TEvIndexTablet::TEvDescribeDataRequest>();

    request->Record.MutableHeaders()->CopyFrom(Headers);
    request->Record.SetFileSystemId(FileSystemId);
    request->Record.SetNodeId(NodeId);
    request->Record.SetHandle(Handle);
    request->Record.SetOffset(Offset);
    request->Record.SetLength(Length);

    // forward request through tablet proxy
    ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
}

////////////////////////////////////////////////////////////////////////////////

void ApplyFreshDataRange(
    const TActorContext& ctx,
    const NProtoPrivate::TFreshDataRange& sourceFreshData,
    IBlockBufferPtr targetBuffer,
    TByteRange alignedTargetByteRange,
    ui32 BlockSize)
{
    Y_UNUSED(targetBuffer);
    if (sourceFreshData.GetContent().empty()) {
        return;
    }
    TByteRange sourceByteRange(
        sourceFreshData.GetOffset(),
        sourceFreshData.GetContent().size(),
        BlockSize);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "common byte range found: source: %s, target: %s",
        sourceByteRange.Describe().c_str(),
        alignedTargetByteRange.Describe().c_str());

    char* targetData = const_cast<char*>(targetBuffer->GetContentRef().data());
    auto commonRange = sourceByteRange.Intersect(alignedTargetByteRange);

    Y_ABORT_UNLESS(sourceByteRange == commonRange);
    if (commonRange.Length == 0) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "common range is empty: source: %s, target: %s",
            sourceByteRange.Describe().c_str(),
            alignedTargetByteRange.Describe().c_str());
        return;
    }
    targetData += commonRange.Offset - alignedTargetByteRange.Offset;

    // NB: we assume that underlying target data is a continuous buffer
    memcpy(
        targetData,
        sourceFreshData.GetContent().data() +
            (commonRange.Offset - sourceByteRange.Offset),
        commonRange.Length);
}

////////////////////////////////////////////////////////////////////////////////

// TODO(debnatkh): get rid of copy-paste
void CopyFileData(
    const TByteRange origin,
    const TByteRange aligned,
    const ui64 fileSize,
    TStringBuf content,
    TString* out)
{
    auto end = Min(fileSize, origin.End());
    if (end < aligned.End()) {
        ui64 delta = Min(aligned.End() - end, content.size());
        content.Chop(delta);
    }

    Y_ABORT_UNLESS(origin.Offset >= aligned.Offset);
    content.Skip(origin.Offset - aligned.Offset);

    out->assign(content.data(), content.size());
}

////////////////////////////////////////////////////////////////////////////////

void TReadDataActor::HandleDescribeDataResponse(
    const TEvIndexTablet::TEvDescribeDataResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "DescribeData succeeded %lu freshdata + %lu blobpieces",
        msg->Record.FreshDataRangesSize(),
        msg->Record.BlobPiecesSize());

    DescribeResponse.CopyFrom(msg->Record);
    ReadBlobIfNeeded(ctx);
}

void TReadDataActor::ReadBlobIfNeeded(const TActorContext& ctx)
{
    RemainingBlobsToRead = DescribeResponse.GetBlobPieces().size();
    ui32 blobPieceId = 0;
    for (auto blobPiece: DescribeResponse.GetBlobPieces()) {
        NKikimr::TLogoBlobID blobId =
            LogoBlobIDFromLogoBlobID(blobPiece.GetBlobId());
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "Processing blob piece: %s, size: %lu",
            blobPiece.DebugString().c_str(),
            blobId.BlobSize());
        NKikimr::TActorId proxy =
            MakeBlobStorageProxyID(blobPiece.GetBSGroupId());
        using TEvGetQuery = TEvBlobStorage::TEvGet::TQuery;
        TArrayHolder<TEvGetQuery> queries(
            new TEvGetQuery[blobPiece.RangesSize()]);
        for (size_t i = 0; i < blobPiece.RangesSize(); ++i) {
            LOG_DEBUG(
                ctx,
                TFileStoreComponents::SERVICE,
                "Adding query for blobId: %s, offset: %lu, length: %lu, "
                "blobsize: %lu",
                blobId.ToString().c_str(),
                blobPiece.GetRanges(i).GetBlobOffset(),
                blobPiece.GetRanges(i).GetLength(),
                blobId.BlobSize());
            queries[i].Set(
                blobId,
                blobPiece.GetRanges(i).GetBlobOffset(),
                blobPiece.GetRanges(i).GetLength());
        }
        auto request = std::make_unique<TEvBlobStorage::TEvGet>(
            queries,
            blobPiece.RangesSize(),
            TInstant::Max(),
            NKikimrBlobStorage::FastRead);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "Sending ReadBlob request, size: %lu, blobId: %s",
            blobPiece.RangesSize(),
            blobId.ToString().c_str());

        SendToBSProxy(ctx, proxy, request.release(), blobPieceId++);
    }

    if (RemainingBlobsToRead == 0) {
        ReplyAndDie(ctx);
    }
}

void TReadDataActor::HandleReadBlobResponse(
    const TEvBlobStorage::TEvGetResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (msg->Status != NKikimrProto::OK) {
        ReplyAndDie(ctx, MakeError(msg->Status, "blob read failed"));
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "ReadBlobResponse count: %lu, status: %lu, cookie: %lu",
        msg->ResponseSz,
        (ui64)(msg->Status),
        ev->Cookie);

    Y_ABORT_UNLESS(ev->Cookie < DescribeResponse.BlobPiecesSize());

    for (size_t i = 0; i < msg->ResponseSz; ++i) {
        Y_ABORT_UNLESS(i < DescribeResponse.GetBlobPieces(i).RangesSize());

        const auto& blobPiece = DescribeResponse.GetBlobPieces(ev->Cookie);
        const auto& blobRange = blobPiece.GetRanges(i);
        const auto& response = msg->Responses[i];
        auto dataIter = response.Buffer.begin();
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "ReadBlobResponse: blobId: %s, offset: %lu, length: %lu, size: "
            "%lu, target: "
            "%s",
            blobPiece.GetBlobId().DebugString().c_str(),
            blobRange.GetBlobOffset(),
            blobRange.GetLength(),
            response.Buffer.size(),
            AlignedByteRange.Describe().c_str());
        Y_ABORT_UNLESS(blobRange.GetBlobOffset() >= AlignedByteRange.Offset);
        Y_ABORT_UNLESS(
            blobRange.GetBlobOffset() + blobRange.GetLength() <
            AlignedByteRange.End());
        char* targetData =
            const_cast<char*>(BlockBuffer->GetContentRef().data()) +
            (blobRange.GetBlobOffset() - AlignedByteRange.Offset);

        Y_ABORT_UNLESS(blobRange.GetLength() == response.Buffer.size());
        dataIter.ExtractPlainDataAndAdvance(targetData, blobRange.GetLength());
    }

    --RemainingBlobsToRead;
    if (RemainingBlobsToRead == 0) {
        ReplyAndDie(ctx);
    }
}

void TReadDataActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TReadDataActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    Y_UNUSED(ctx);
    auto response = std::make_unique<TEvService::TEvReadDataResponse>(error);
    if (SUCCEEDED(error.GetCode())) {
        // we apply fresh data ranges to the buffer only after all blobs are
        // read and applied
        for (const auto& freshDataRange: DescribeResponse.GetFreshDataRanges())
        {
            ui64 offset = freshDataRange.GetOffset();
            const TString& content = freshDataRange.GetContent();

            ApplyFreshDataRange(
                ctx,
                freshDataRange,
                BlockBuffer,
                AlignedByteRange,
                BlockSize);

            LOG_DEBUG(
                ctx,
                TFileStoreComponents::SERVICE,
                "processed fresh data range size: %lu, offset: %lu",
                content.size(),
                offset);
        }

        CopyFileData(
            OriginByteRange,
            AlignedByteRange,
            DescribeResponse.GetTotalSize(),
            BlockBuffer->GetContent(),
            response->Record.MutableBuffer());
    }

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
}

STFUNC(TReadDataActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTablet::TEvDescribeDataResponse,
            HandleDescribeDataResponse);

        HFunc(TEvBlobStorage::TEvGetResult, HandleReadBlobResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleReadData(
    const TEvService::TEvReadDataRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!StorageConfig->GetTwoStageReadEnabled()) {
        // If two-stage read is disabled, forward the request to the tablet in
        // the same way as all other requests.
        ForwardRequest<TEvService::TReadDataMethod>(ctx, ev);
        return;
    }

    const auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "read data %s",
        msg->Record.DebugString().c_str());

    auto [cookie, inflight] = CreateInFlightRequest(
        TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT,
        StatsRegistry->GetRequestStats(),
        ctx.Now());

    const auto* filestore =
        State->GetLocalFileStores().FindPtr(msg->Record.GetFileSystemId());

    Y_ABORT_UNLESS(filestore);
    const auto& config = filestore->Config;

    InitProfileLogRequestInfo(inflight->ProfileLogRequest, msg->Record);

    auto requestInfo = CreateRequestInfo(SelfId(), cookie, msg->CallContext);

    auto actor = std::make_unique<TReadDataActor>(
        std::move(requestInfo),
        ev,
        config.GetBlockSize());

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
