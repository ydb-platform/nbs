#include "part_actor.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/common/guarded_sglist.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NKikimr;

using namespace NMonitoringUtils;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class THttpReadBlockActor final
    : public TActorBootstrapped<THttpReadBlockActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const bool DataDumpAllowed;
    const TActorId Tablet;
    const ui32 BlockIndex;
    const ui64 CommitId;
    const bool Binary;

    TGuardedBuffer<TString> BufferHolder;

public:
    THttpReadBlockActor(
        TRequestInfoPtr requestInfo,
        bool dataDumpAllowed,
        const TActorId& tablet,
        ui32 blockSize,
        ui32 blockIndex,
        ui64 commitId,
        bool binary);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReadBlocks(const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx, const TString& buffer);
    void ReplyAndDie(const TActorContext& ctx, const NProto::TError& error);

    void SendBinaryResponse(
        const TActorContext& ctx,
        const TString& buffer);

    void SendHttpResponse(
        const TActorContext& ctx,
        const NProto::TError& error,
        const TString& buffer);

private:
    STFUNC(StateWork);

    void HandleReadBlockResponse(
        const TEvService::TEvReadBlocksLocalResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleReadBlockRequest(
        const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

THttpReadBlockActor::THttpReadBlockActor(
        TRequestInfoPtr requestInfo,
        bool dataDumpAllowed,
        const TActorId& tablet,
        ui32 blockSize,
        ui32 blockIndex,
        ui64 commitId,
        bool binary)
    : RequestInfo(std::move(requestInfo))
    , DataDumpAllowed(dataDumpAllowed)
    , Tablet(tablet)
    , BlockIndex(blockIndex)
    , CommitId(commitId)
    , Binary(binary)
    , BufferHolder(TString::Uninitialized(blockSize))
{}

void THttpReadBlockActor::Bootstrap(const TActorContext& ctx)
{
    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "HttpInfo",
        RequestInfo->CallContext->RequestId);

    ReadBlocks(ctx);

    Become(&TThis::StateWork);
}

void THttpReadBlockActor::ReadBlocks(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvReadBlocksLocalRequest>();
    request->Record.SetStartIndex(BlockIndex);
    request->Record.SetBlocksCount(1);
    request->Record.Sglist = BufferHolder.GetGuardedSgList();
    request->Record.CommitId = CommitId;
    request->Record.BlockSize = BufferHolder.Get().size();

    NCloud::SendWithUndeliveryTracking(
        ctx,
        Tablet,
        std::move(request));
}

void THttpReadBlockActor::ReplyAndDie(
    const TActorContext& ctx,
    const TString& buffer)
{
    if (Binary) {
        SendBinaryResponse(ctx, buffer);
    } else {
        SendHttpResponse(ctx, {}, buffer);
    }

    Die(ctx);
}

void THttpReadBlockActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    SendHttpResponse(ctx, error, {});
    Die(ctx);
}

void THttpReadBlockActor::SendBinaryResponse(
    const TActorContext& ctx,
    const TString& buffer)
{
    TStringStream out;

    out << "HTTP/1.1 200 OK\r\n"
        << "Content-Type: application/octet-stream\r\n"
        << "Content-Disposition: attachment; filename=\"Block_"
        << BlockIndex << "_" << CommitId << "\"\r\n"
        << "Connection: close\r\n"
        << "\r\n";
    out << buffer;

    auto response = std::make_unique<NMon::TEvRemoteBinaryInfoRes>(out.Str());

    LWTRACK(
        ResponseSent_Partition,
        RequestInfo->CallContext->LWOrbit,
        "HttpInfo",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
}

void THttpReadBlockActor::SendHttpResponse(
    const TActorContext& ctx,
    const NProto::TError& error,
    const TString& buffer)
{
    TStringStream out;

    HTML(out) {
        DIV_CLASS("panel panel-info") {
            DIV_CLASS("panel-heading") {
                out << "Block Content";
            }
            DIV_CLASS("panel-body") {
                DIV() {
                    out << "BlockIndex: ";
                    STRONG() {
                        out << BlockIndex;
                    }
                }
                DIV() {
                    out << "CommitId: ";
                    STRONG() {
                        out << CommitId;
                    }
                }
                DIV() {
                    out << "Status: ";
                    STRONG() {
                        out << FormatError(error);
                    }
                }
                if (buffer) {
                    if (DataDumpAllowed) {
                        DumpBlockContent(out, buffer);
                    } else {
                        DumpDataHash(out, buffer);
                    }
                }
            }
        }
    }

    auto response = std::make_unique<NMon::TEvRemoteHttpInfoRes>(out.Str());

    LWTRACK(
        ResponseSent_Partition,
        RequestInfo->CallContext->LWOrbit,
        "HttpInfo",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void THttpReadBlockActor::HandleReadBlockResponse(
    const TEvService::TEvReadBlocksLocalResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    ReplyAndDie(ctx, BufferHolder.Extract());
}

void THttpReadBlockActor::HandleReadBlockRequest(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void THttpReadBlockActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

STFUNC(THttpReadBlockActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvService::TEvReadBlocksLocalResponse, HandleReadBlockResponse);
        HFunc(TEvService::TEvReadBlocksLocalRequest, HandleReadBlockRequest);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleHttpInfo_View(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    if (params.Get("block") && params.Has("commitid")) {
        ui32 blockIndex = 0;
        ui64 commitId = 0;

        if (TryFromString(params.Get("block"), blockIndex) &&
            TryFromString(params.Get("commitid"), commitId)) {

            NCloud::Register<THttpReadBlockActor>(
                ctx,
                std::move(requestInfo),
                Config->GetUserDataDebugDumpAllowed(),
                SelfId(),
                State->GetBlockSize(),
                blockIndex,
                commitId,
                params.Has("binary"));
        } else {
            RejectHttpRequest(
                ctx,
                *requestInfo,
                "invalid index specified");
        }
        return;
    }

    TStringStream out;
    DumpDefaultHeader(out, *Info(), SelfId().NodeId(), *DiagnosticsConfig);

    SendHttpResponse(ctx, *requestInfo, std::move(out.Str()));
}

void TPartitionActor::HandleHttpInfo_GetTransactionsLatency(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);

    auto json = TransactionTimeTracker.GetStatJson(
        GetCycleCount(),
        [](const TString& name)
        { return !name.StartsWith("WriteBlob_Group"); });
    NCloud::Reply(
        ctx,
        *requestInfo,
        std::make_unique<NMon::TEvRemoteJsonInfoRes>(json));
}

void TPartitionActor::HandleHttpInfo_GetGroupLatencies(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);

    auto json = TransactionTimeTracker.GetStatJson(
        GetCycleCount(),
        [](const TString& name) { return name.StartsWith("WriteBlob_Group"); });
    NCloud::Reply(
        ctx,
        *requestInfo,
        std::make_unique<NMon::TEvRemoteJsonInfoRes>(json));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
