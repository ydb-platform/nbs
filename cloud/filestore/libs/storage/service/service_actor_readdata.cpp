#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/tablet/model/block.h>
#include <cloud/filestore/libs/storage/tablet/model/block_buffer.h>
#include <cloud/filestore/libs/storage/tablet/model/range.h>

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
    ui64 TotalSize;

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
    , TotalSize(0)
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
        "DescribeData succeeded %s",
        msg->Record.DebugString().c_str());

    TotalSize = msg->Record.GetTotalSize();

    // WIP: get remaining data from BS

    for (const auto& freshDataRange: msg->Record.GetFreshDataRanges()) {
        ui64 offset = freshDataRange.GetOffset();
        const TString& content = freshDataRange.GetContent();

        Y_ABORT_UNLESS(
            content.size() + offset <= BlockBuffer->GetContentRef().size());

        memcpy(
            const_cast<char*>(BlockBuffer->GetContentRef().Data()) + offset,
            content.data(),
            content.size());

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "processed fresh data range size: %lu, offset: %lu",
            content.size(),
            offset);
    }

    ReplyAndDie(ctx);
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
        CopyFileData(
            OriginByteRange,
            AlignedByteRange,
            TotalSize,
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

    Y_VERIFY_S(
        filestore,
        "filestore" + msg->Record.GetFileSystemId() + "not found");
    const auto& config = filestore->Config;

    // TODO(debnatkh): If option disabled, forward request to index node

    InitProfileLogRequestInfo(inflight->ProfileLogRequest, msg->Record);

    auto requestInfo = CreateRequestInfo(SelfId(), cookie, msg->CallContext);

    auto actor = std::make_unique<TReadDataActor>(
        std::move(requestInfo),
        ev,
        config.GetBlockSize());

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
