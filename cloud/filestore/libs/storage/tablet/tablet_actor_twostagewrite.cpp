#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/throttler_info_serializer.h>
#include <cloud/filestore/libs/diagnostics/trace_serializer.h>
#include <cloud/filestore/libs/storage/tablet/model/split_range.h>

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

    // TODO(debnatkh): better logic for lease expiration
    AcquireCollectBarrier(commitId);

    // TODO(debnatkh): better selection of channel
    ui32 blobIndex = 0;
    TPartialBlobId partialBlobId;
    const auto ok = GenerateBlobId(
        commitId,
        msg->Record.GetLength(),
        blobIndex++,
        &partialBlobId);

    if (!ok) {
        ReassignDataChannelsIfNeeded(ctx);

        auto response = std::make_unique<TEvIndexTablet::TEvIssueBlobResponse>(
            MakeError(E_FS_OUT_OF_SPACE, "failed to generate blobId"));

        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    // TODO(debnatkh): Throttling

    auto response = std::make_unique<TEvIndexTablet::TEvIssueBlobResponse>();

    response->Record.SetBSGroupId(
        Info()->GroupFor(partialBlobId.Channel(), partialBlobId.Generation()));
    LogoBlobIDFromLogoBlobID(
        MakeBlobId(TabletID(), partialBlobId),
        response->Record.MutableBlobId());
    response->Record.SetCommitId(commitId);

    NCloud::Reply(ctx, *ev, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleMarkWriteCompleted(
    const TEvIndexTablet::TEvMarkWriteCompletedRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s %s: %s",
        LogTag.c_str(),
        "TwoStageWrite",
        DumpMessage(msg->Record).c_str());

    if (!CompactionStateLoadStatus.Finished) {
        // TODO(debnatkh): Support two-stage write in case of unfinished
        // compaction state load

        auto response =
            std::make_unique<TEvIndexTablet::TEvMarkWriteCompletedResponse>(
                MakeError(E_REJECTED, "compaction state not loaded yet"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    // TODO(debnatkh): do the same validation as in TIndexTabletActor::HandleWriteData

    const TByteRange range(
        msg->Record.GetOffset(),
        msg->Record.GetLength(),
        GetBlockSize());

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    // TODO(debnatkh): Make sure that this block buffer can be empty and get rid of it
    auto blockBuffer = CreateBlockBuffer(
        range,
        TString(msg->Record.GetLength(), 'A' + (msg->Record.GetOffset() % 26)));

    AddTransaction<TEvService::TWriteDataMethod>(*requestInfo);

    auto blobId = LogoBlobIDFromLogoBlobID(msg->Record.GetBlobId());

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s %s: blobId: %s",
        LogTag.c_str(),
        "MarkWriteCompleted",
        blobId.ToString().c_str());

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
        std::make_optional(blobId));
}

}   // namespace NCloud::NFileStore::NStorage
