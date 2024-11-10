#include "tablet_actor.h"

#include "helpers.h"

#include <cloud/filestore/libs/storage/tablet/model/profile_log_events.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/stream/str.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEvGetQueryInfo
{
    ui32 BlocksCount = 0;
};

struct TReadBlobRequest
{
    TActorId Proxy;
    TLogoBlobID BlobId;
    TVector<TReadBlob::TBlock> Blocks;
    TVector<TEvGetQueryInfo> QueryInfos;
    std::unique_ptr<TEvBlobStorage::TEvGet> Request;

    TReadBlobRequest(
            const TActorId& proxy,
            const TLogoBlobID& blobId,
            TVector<TReadBlob::TBlock> blocks,
            TVector<TEvGetQueryInfo> queryInfos,
            std::unique_ptr<TEvBlobStorage::TEvGet> request)
        : Proxy(proxy)
        , BlobId(blobId)
        , Blocks(std::move(blocks))
        , QueryInfos(std::move(queryInfos))
        , Request(std::move(request))
    {}
};

////////////////////////////////////////////////////////////////////////////////

TString DumpBlobIds(const TVector<TReadBlobRequest>& requests)
{
    TStringStream out;

    out << requests[0].BlobId;
    for (size_t i = 1; i < requests.size(); ++i) {
        out << ", " << requests[i].BlobId;
    }

    return out.Str();
}

////////////////////////////////////////////////////////////////////////////////

class TReadBlobActor final
    : public TActorBootstrapped<TReadBlobActor>
{
private:
    const TString LogTag;
    const TActorId Tablet;
    const TRequestInfoPtr RequestInfo;
    const IBlockBufferPtr Buffer;
    const ui32 BlockSize;
    const TString FileSystemId;
    const IProfileLogPtr ProfileLog;

    TVector<TReadBlobRequest> Requests;
    size_t RequestsCompleted = 0;
    ui32 TotalSize = 0;
    NProto::TProfileLogRequestInfo ProfileLogRequest;

public:
    TReadBlobActor(
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        IBlockBufferPtr buffer,
        ui32 blockSize,
        TString fileSystemId,
        IProfileLogPtr profileLog,
        TVector<TReadBlobRequest> requests,
        NProto::TProfileLogRequestInfo profileLogRequest);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void SendRequests(const TActorContext& ctx);
    void HandleGetResult(
        const TEvBlobStorage::TEvGetResult::TPtr& ev,
        const TActorContext& ctx);
    void ReadUncompressedResponse(
        const TRope& responseBuffer,
        auto blockIter,
        ui32 blocksCount);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});

    void ReplyError(
        const TActorContext& ctx,
        const TEvBlobStorage::TEvGetResult& response,
        const TString& reason);
};

////////////////////////////////////////////////////////////////////////////////

TReadBlobActor::TReadBlobActor(
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        IBlockBufferPtr buffer,
        ui32 blockSize,
        TString fileSystemId,
        IProfileLogPtr profileLog,
        TVector<TReadBlobRequest> requests,
        NProto::TProfileLogRequestInfo profileLogRequest)
    : LogTag(std::move(logTag))
    , Tablet(tablet)
    , RequestInfo(std::move(requestInfo))
    , Buffer(std::move(buffer))
    , BlockSize(blockSize)
    , FileSystemId(std::move(fileSystemId))
    , ProfileLog(std::move(profileLog))
    , Requests(std::move(requests))
    , ProfileLogRequest(std::move(profileLogRequest))
{}

void TReadBlobActor::Bootstrap(const TActorContext& ctx)
{
    FILESTORE_TRACK(
        RequestReceived_TabletWorker,
        RequestInfo->CallContext,
        "ReadBlob");

    SendRequests(ctx);
    Become(&TThis::StateWork);
}

void TReadBlobActor::SendRequests(const TActorContext& ctx)
{
    size_t requestIndex = 0;
    for (auto& request: Requests) {
        SendToBSProxy(
            ctx,
            request.Proxy,
            request.Request.release(),
            requestIndex++);
    }
}

void TReadBlobActor::HandleGetResult(
    const TEvBlobStorage::TEvGetResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (msg->Status != NKikimrProto::OK) {
        ReplyError(ctx, *msg, msg->ErrorReason);
        return;
    }

    RequestInfo->CallContext->LWOrbit.Join(msg->Orbit);

    size_t requestIndex = ev->Cookie;
    TABLET_VERIFY(requestIndex < Requests.size());

    const auto& request = Requests[requestIndex];

    auto blockIter = request.Blocks.begin();
    TABLET_VERIFY(blockIter != request.Blocks.end());

    if (msg->ResponseSz != request.QueryInfos.size()) {
        ReplyError(ctx, *msg, "invalid number of responses");
        return;
    }

    for (size_t i = 0; i < msg->ResponseSz; ++i) {
        auto& response = msg->Responses[i];

        if (response.Status != NKikimrProto::OK) {
            ReplyError(ctx, *msg, "read error");
            return;
        }

        if (response.Id != request.BlobId) {
            ReplyError(ctx, *msg, "invalid blob id");
            return;
        }

        const auto blocksCount = request.QueryInfos[i].BlocksCount;
        TABLET_VERIFY(blockIter + blocksCount <= request.Blocks.end());

        if (response.Buffer.size() / BlockSize != blocksCount) {
            ReplyError(ctx, *msg, "invalid response buffer size");
            return;
        }

        ReadUncompressedResponse(response.Buffer, blockIter, blocksCount);
        blockIter += blocksCount;
    }

    TABLET_VERIFY(blockIter == request.Blocks.end());

    TABLET_VERIFY(RequestsCompleted < Requests.size());
    if (++RequestsCompleted == Requests.size()) {
        ReplyAndDie(ctx);
    }
}

void TReadBlobActor::ReadUncompressedResponse(
    const TRope& responseBuffer,
    auto blockIter,
    ui32 blocksCount)
{
    TotalSize += responseBuffer.size();

    char buffer[BlockSize];
    auto iter = responseBuffer.begin();

    for (size_t j = 0; j < blocksCount; ++j) {
        TStringBuf view;

        if (iter.ContiguousSize() >= BlockSize) {
            view = TStringBuf(iter.ContiguousData(), BlockSize);
            iter += BlockSize;
        } else {
            iter.ExtractPlainDataAndAdvance(buffer, BlockSize);
            view = TStringBuf(buffer, BlockSize);
        }

        Buffer->SetBlock(blockIter->BlockOffset, view);
        ++blockIter;
    }
}

void TReadBlobActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TReadBlobActor::ReplyError(
        const TActorContext& ctx,
        const TEvBlobStorage::TEvGetResult& response,
        const TString& message)
{
    LOG_ERROR(ctx, TFileStoreComponents::TABLET,
        "%s TEvBlobStorage::TEvGet failed: %s\n%s",
        LogTag.c_str(),
        message.c_str(),
        response.Print(false).c_str());

    auto error = MakeError(E_REJECTED, "TEvBlobStorage::TEvGet failed: " + message);
    ReplyAndDie(ctx, error);
}

void TReadBlobActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    const auto t = ctx.Now()
        - TInstant::MicroSeconds(ProfileLogRequest.GetTimestampMcs());

    // log request
    FinalizeProfileLogRequestInfo(
        std::move(ProfileLogRequest),
        ctx.Now(),
        FileSystemId,
        error,
        ProfileLog);

    {
        // notify tablet
        using TCompletion = TEvIndexTabletPrivate::TEvReadBlobCompleted;
        auto response = std::make_unique<TCompletion>(error, 1, TotalSize, t);
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    FILESTORE_TRACK(
        ResponseSent_TabletWorker,
        RequestInfo->CallContext,
        "ReadBlob");

    if (RequestInfo->Sender != Tablet) {
        // reply to caller
        using TCompletion = TEvIndexTabletPrivate::TEvReadBlobResponse;
        auto response = std::make_unique<TCompletion>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    Die(ctx);
}

STFUNC(TReadBlobActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvBlobStorage::TEvGetResult, HandleGetResult);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleReadBlob(
    const TEvIndexTabletPrivate::TEvReadBlobRequest::TPtr& ev,
    const TActorContext& ctx)
{
    NProto::TProfileLogRequestInfo profileLogRequest;
    InitProfileLogRequestInfo(
        profileLogRequest,
        EFileStoreSystemRequest::ReadBlob,
        ctx.Now());

    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    FILESTORE_TRACK(
        BackgroundRequestReceived_Tablet,
        msg->CallContext,
        "ReadBlob");

    const auto blockSize = GetBlockSize();

    TVector<TReadBlobRequest> requests(Reserve(msg->Blobs.size()));
    for (auto& blob: msg->Blobs) {
        auto blobId = MakeBlobId(TabletID(), blob.BlobId);

        auto proxy = Info()->BSProxyIDForChannel(
            blob.BlobId.Channel(),
            blob.BlobId.Generation());

        const auto blocksCount = blob.Blocks.size();

        using TEvGetQuery = TEvBlobStorage::TEvGet::TQuery;

        TArrayHolder<TEvGetQuery> queries(new TEvGetQuery[blocksCount]);
        size_t queriesCount = 0;
        TVector<TEvGetQueryInfo> queryInfos;

        struct TBlockRange
        {
            ui32 BlockOffset = 0;
            ui32 BlocksCount = 0;
        };

        auto addRangeToProfileLog = [&](const TBlockRange& range) {
            if (range.BlocksCount) {
                AddRange(
                    blob.BlobId.CommitId(),
                    static_cast<ui64>(range.BlockOffset) * blockSize,
                    static_cast<ui64>(range.BlocksCount) * blockSize,
                    profileLogRequest);
            }
        };

        TBlockRange curBlockRange;

        for (size_t i = 0; i < blocksCount; ++i) {
            const auto& curBlock = blob.Blocks[i];

            if (i && curBlock.BlobOffset == blob.Blocks[i - 1].BlobOffset + 1) {
                const auto& prevBlock = blob.Blocks[i - 1];

                // extend range
                queries[queriesCount - 1].Size += blockSize;
                ++queryInfos.back().BlocksCount;

                if (curBlock.BlockOffset == prevBlock.BlockOffset + 1) {
                    ++curBlockRange.BlocksCount;
                } else {
                    addRangeToProfileLog(curBlockRange);
                    curBlockRange = { curBlock.BlockOffset, 1 };
                }
            } else {
                queries[queriesCount++].Set(
                    blobId,
                    curBlock.BlobOffset * blockSize,
                    blockSize);
                queryInfos.push_back(TEvGetQueryInfo{ .BlocksCount = 1 });

                addRangeToProfileLog(curBlockRange);
                curBlockRange = { curBlock.BlockOffset, 1 };
            }
        }

        auto request = std::make_unique<TEvBlobStorage::TEvGet>(
            queries,
            queriesCount,
            blob.Deadline,
            blob.Async
                ? NKikimrBlobStorage::AsyncRead
                : NKikimrBlobStorage::FastRead);

        if (!msg->CallContext->LWOrbit.Fork(request->Orbit)) {
            FILESTORE_TRACK(
                ForkFailed,
                msg->CallContext,
                "TEvBlobStorage::TEvGet");
        }

        requests.emplace_back(
            proxy,
            blobId,
            std::move(blob.Blocks),
            std::move(queryInfos),
            std::move(request));
    }

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s ReadBlob started (%s)",
        LogTag.c_str(),
        DumpBlobIds(requests).c_str());

    auto actor = std::make_unique<TReadBlobActor>(
        LogTag,
        ctx.SelfID,
        std::move(requestInfo),
        std::move(msg->Buffer),
        blockSize,
        GetFileSystemId(),
        ProfileLog,
        std::move(requests),
        std::move(profileLogRequest));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

void TIndexTabletActor::HandleReadBlobCompleted(
    const TEvIndexTabletPrivate::TEvReadBlobCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s ReadBlob completed (%s)",
        LogTag.c_str(),
        FormatError(msg->GetError()).c_str());

    WorkerActors.erase(ev->Sender);

    Metrics.ReadBlob.Update(msg->Count, msg->Size, msg->Time);
}

}   // namespace NCloud::NFileStore::NStorage
