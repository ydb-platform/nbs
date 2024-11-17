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

struct TQuery
{
    bool Compressed = false;
    TUncompressedRange UncompressedRange;
    TCompressedRange CompressedRange;
    TVector<TUncompressedBlock> Blocks;
};

void MergeOverlappingCompressedQueries(TVector<TQuery>& queries)
{
    Sort(queries, [] (const auto& l, const auto& r) {
        return l.CompressedRange.Offset < r.CompressedRange.Offset
            || (l.CompressedRange.Offset == r.CompressedRange.Offset
                && l.CompressedRange.Length < r.CompressedRange.Length);
    });

    auto pivotQueryIter = queries.begin();
    for (size_t i = 1; i < queries.size(); i++) {
        auto& curQuery = queries[i];
        auto& pivotQuery = *pivotQueryIter;

        if (pivotQuery.CompressedRange.Overlaps(curQuery.CompressedRange)) {
            pivotQuery.CompressedRange.Merge(curQuery.CompressedRange);

            for (auto& block : curQuery.Blocks) {
                pivotQuery.Blocks.push_back(std::move(block));
            }
            curQuery.Blocks.clear();

            pivotQuery.Compressed =
                pivotQuery.Compressed || curQuery.Compressed;
        } else {
            ++pivotQueryIter;
        }
    }

    EraseIf(queries, [] (const auto& query) { return query.Blocks.empty(); });
}

////////////////////////////////////////////////////////////////////////////////

struct TReadBlobRequest
{
    TActorId Proxy;
    TLogoBlobID BlobId;
    TBlobCompressionInfo BlobCompressionInfo;
    TVector<TQuery> Queries;
    std::unique_ptr<TEvBlobStorage::TEvGet> Request;

    TReadBlobRequest(
            const TActorId& proxy,
            const TLogoBlobID& blobId,
            TBlobCompressionInfo blobCompressionInfo,
            TVector<TQuery> queries,
            std::unique_ptr<TEvBlobStorage::TEvGet> request)
        : Proxy(proxy)
        , BlobId(blobId)
        , BlobCompressionInfo(std::move(blobCompressionInfo))
        , Queries(std::move(queries))
        , Request(std::move(request))
    {}
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TEvBlobStorage::TEvGet> CreateGetRequest(
    const TLogoBlobID& blobId,
    const TVector<TQuery>& queries,
    TInstant deadline,
    bool async
) {
    using TEvGetQuery = TEvBlobStorage::TEvGet::TQuery;
    TArrayHolder<TEvGetQuery> qs(new TEvGetQuery[queries.size()]);

    size_t queryIndex = 0;
    for (const auto& query : queries) {
        if (query.Compressed) {
            qs[queryIndex].Set(
                blobId,
                query.CompressedRange.Offset,
                query.CompressedRange.Length);
        } else {
            qs[queryIndex].Set(
                blobId,
                query.UncompressedRange.Offset,
                query.UncompressedRange.Length);
        }
        ++queryIndex;
    }

    return std::make_unique<TEvBlobStorage::TEvGet>(
        qs,
        queries.size(),
        deadline,
        async ? NKikimrBlobStorage::AsyncRead : NKikimrBlobStorage::FastRead
    );
}

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
    const IBlockBufferPtr BlockBuffer;
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
    void ReadQueryResponseUncompressed(
        const TRope& responseBuffer,
        const TQuery& query);

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
    , BlockBuffer(std::move(buffer))
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

    if (msg->ResponseSz != request.Queries.size()) {
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

        TotalSize += response.Buffer.size();

        const auto& query = request.Queries[i];

        if (query.Compressed) {
            if (response.Buffer.size() != query.CompressedRange.Length) {
                ReplyError(
                    ctx,
                    *msg,
                    "invalid compressed response buffer size");
                return;
            }

            Decompress(
                request.BlobCompressionInfo,
                BlockSize,
                response.Buffer,
                query.CompressedRange.Offset,
                query.Blocks,
                BlockBuffer.get());
        } else {
            if (response.Buffer.size() / BlockSize != query.Blocks.size()) {
                ReplyError(ctx, *msg, "invalid response buffer size");
                return;
            }

            ReadQueryResponseUncompressed(response.Buffer, query);
        }
    }

    TABLET_VERIFY(RequestsCompleted < Requests.size());
    if (++RequestsCompleted == Requests.size()) {
        ReplyAndDie(ctx);
    }
}

void TReadBlobActor::ReadQueryResponseUncompressed(
    const TRope& responseBuffer,
    const TQuery& query)
{
    char buffer[BlockSize];
    auto iter = responseBuffer.begin();

    for (const auto& block : query.Blocks) {
        TStringBuf view;

        if (iter.ContiguousSize() >= BlockSize) {
            view = TStringBuf(iter.ContiguousData(), BlockSize);
            iter += BlockSize;
        } else {
            iter.ExtractPlainDataAndAdvance(buffer, BlockSize);
            view = TStringBuf(buffer, BlockSize);
        }

        BlockBuffer->SetBlock(block.BlockOffset, view);
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

        TVector<TQuery> queries;
        TBlockRange curBlockRange;

        const auto blocksCount = blob.Blocks.size();
        for (size_t i = 0; i < blocksCount; ++i) {
            const auto& curBlock = blob.Blocks[i];

            if (i && curBlock.BlobOffset == blob.Blocks[i - 1].BlobOffset + 1) {
                const auto& prevBlock = blob.Blocks[i - 1];

                queries.back().UncompressedRange.Extend(blockSize);
                queries.back().Blocks.push_back(
                    TUncompressedBlock(
                        curBlock.BlobOffset,
                        curBlock.BlockOffset));

                if (curBlock.BlockOffset == prevBlock.BlockOffset + 1) {
                    // extend range
                    ++curBlockRange.BlocksCount;
                } else {
                    addRangeToProfileLog(curBlockRange);
                    // new range
                    curBlockRange = { curBlock.BlockOffset, 1 };
                }
            } else {
                queries.push_back(TQuery {
                    .UncompressedRange = TUncompressedRange(
                        curBlock.BlobOffset * blockSize,
                        blockSize),
                    .Blocks = {{ curBlock.BlobOffset, curBlock.BlockOffset }},
                });

                addRangeToProfileLog(curBlockRange);
                // new range
                curBlockRange = { curBlock.BlockOffset, 1 };
            }
        }

        if (blob.BlobCompressionInfo.BlobCompressed()) {
            for (auto& query : queries) {
                query.CompressedRange = blob.BlobCompressionInfo.CompressedRange(
                    query.UncompressedRange);
                query.Compressed = true;
            }

            MergeOverlappingCompressedQueries(queries);
        }

        auto blobId = MakeBlobId(TabletID(), blob.BlobId);
        auto request = CreateGetRequest(
            blobId,
            queries,
            blob.Deadline,
            blob.Async);

        if (!msg->CallContext->LWOrbit.Fork(request->Orbit)) {
            FILESTORE_TRACK(
                ForkFailed,
                msg->CallContext,
                "TEvBlobStorage::TEvGet");
        }

        auto proxy = Info()->BSProxyIDForChannel(
            blob.BlobId.Channel(),
            blob.BlobId.Generation());

        requests.emplace_back(
            proxy,
            blobId,
            std::move(blob.BlobCompressionInfo),
            std::move(queries),
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
