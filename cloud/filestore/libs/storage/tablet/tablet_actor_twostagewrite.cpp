#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/throttler_info_serializer.h>
#include <cloud/filestore/libs/diagnostics/trace_serializer.h>
#include <cloud/filestore/libs/storage/tablet/model/split_range.h>

#include <library/cpp/iterator/enumerate.h>

#include <util/generic/algorithm.h>
#include <util/generic/cast.h>
#include <util/generic/hash.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleIssueBlob(
    const TEvIndexTablet::TEvIssueBlobRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s %s: %s",
        LogTag.c_str(),
        "IssueBlob",
        DumpMessage(msg->Record).c_str());

    ui64 commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "IssueBlob");
    }

    // We schedule this event for the case if the client will not call
    // MarkWriteCompleted, due to connection loss. Thus we ensure that the
    // collect barrier will be released eventually.
    ctx.Schedule(
        Config->GetIssueBlobReleaseCollectBarrierTimeout(),
        new TEvIndexTabletPrivate::TEvReleaseCollectBarrier(commitId));
    AcquireCollectBarrier(commitId);

    auto response = std::make_unique<TEvIndexTablet::TEvIssueBlobResponse>();
    for (auto [blobIndex, length]: Enumerate(msg->Record.GetLengths())) {
        TPartialBlobId partialBlobId;
        // TODO(debnatkh): better selection of channel
        const auto ok =
            GenerateBlobId(commitId, length, blobIndex, &partialBlobId);

        if (!ok) {
            ReassignDataChannelsIfNeeded(ctx);

            auto response =
                std::make_unique<TEvIndexTablet::TEvIssueBlobResponse>(
                    MakeError(E_FS_OUT_OF_SPACE, "failed to generate blobId"));

            NCloud::Reply(ctx, *ev, std::move(response));
            return;
        }
        LogoBlobIDFromLogoBlobID(
            MakeBlobId(TabletID(), partialBlobId),
            response->Record.MutableBlobIds()->Add());
        if (blobIndex == 0) {
            response->Record.SetBSGroupId(Info()->GroupFor(
                partialBlobId.Channel(),
                partialBlobId.Generation()));
        }
    }

    // TODO(debnatkh): Throttling

    response->Record.SetCommitId(commitId);

    NCloud::Reply(ctx, *ev, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleMarkWriteCompleted(
    const TEvIndexTablet::TEvMarkWriteCompletedRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (!CompactionStateLoadStatus.Finished) {
        // TODO(debnatkh): Support two-stage write in case of unfinished
        // compaction state loading

        auto response =
            std::make_unique<TEvIndexTablet::TEvMarkWriteCompletedResponse>(
                MakeError(E_REJECTED, "compaction state not loaded yet"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto commitId = msg->Record.GetCommitId();
    if (!IsCollectBarrierAcquired(commitId)) {
        // The client has sent the MarkWriteCompleted request too late, after
        // the lease has expired.
        auto response =
            std::make_unique<TEvIndexTablet::TEvMarkWriteCompletedResponse>(
                MakeError(E_REJECTED, "collect barrier expired"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
    // We acquire the collect barrier for the second time in order to prolong an
    // already acquired lease.
    AcquireCollectBarrier(commitId);

    const TByteRange range(
        msg->Record.GetOffset(),
        msg->Record.GetLength(),
        GetBlockSize());

    auto validator =
        [&](const NProtoPrivate::TMarkWriteCompletedRequest& request)
    {
        if (auto error = ValidateRange(range); HasError(error)) {
            return error;
        }

        auto* handle = FindHandle(request.GetHandle());
        if (!handle || handle->GetSessionId() != GetSessionId(request)) {
            return ErrorInvalidHandle(request.GetHandle());
        }

        if (!IsWriteAllowed(BuildBackpressureThresholds())) {
            if (CompactionStateLoadStatus.Finished &&
                ++BackpressureErrorCount >=
                    Config->GetMaxBackpressureErrorsBeforeSuicide())
            {
                LOG_WARN(
                    ctx,
                    TFileStoreComponents::TABLET_WORKER,
                    "%s Suiciding after %u backpressure errors",
                    LogTag.c_str(),
                    BackpressureErrorCount);

                Suicide(ctx);
            }

            return MakeError(E_REJECTED, "rejected due to backpressure");
        }

        return NProto::TError{};
    };

    if (!AcceptRequest<TEvIndexTablet::TMarkWriteCompletedMethod>(
            ev,
            ctx,
            validator))
    {
        return;
    }

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    // TODO(debnatkh): Make sure that this block buffer can be empty and get rid of it
    auto blockBuffer = CreateBlockBuffer(
        range,
        TString(msg->Record.GetLength(), 'A' + (msg->Record.GetOffset() % 26)));

    TVector<NKikimr::TLogoBlobID> blobIds;
    for (const auto& blobId: msg->Record.GetBlobIds()) {
        blobIds.push_back(LogoBlobIDFromLogoBlobID(blobId));
    }

    TABLET_VERIFY(!blobIds.empty());

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s %s: blobId: %s,... (total: %lu)",
        LogTag.c_str(),
        "MarkWriteCompleted",
        blobIds[0].ToString().c_str(),
        blobIds.size());

    AddTransaction<TEvService::TWriteDataMethod>(*requestInfo);

    ExecuteTx<TWriteData>(
        ctx,
        std::move(requestInfo),
        Config->GetWriteBlobThreshold(),
        msg->Record,
        range,
        std::move(blockBuffer),
        // Client has already wrote the data to the storage, so we pass the
        // blobId to the TWriteData transaction, so it skips the BlobStorage
        // write
        msg->Record.GetCommitId(),
        std::move(blobIds));
}

}   // namespace NCloud::NFileStore::NStorage
