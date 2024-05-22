#include "part_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/partition_common/actor_read_blob.h>

#include <cloud/storage/core/libs/common/alloc.h>

#include <ydb/core/base/blobstorage.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleReadBlob(
    const TEvPartitionCommonPrivate::TEvReadBlobRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto msg = ev->Release();

    auto requestInfo = CreateRequestInfo<TEvPartitionCommonPrivate::TReadBlobMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "ReadBlob",
        requestInfo->CallContext->RequestId);

    const auto blob = msg->BlobId;

    auto readBlobActor = std::make_unique<TReadBlobActor>(
        requestInfo,
        SelfId(),
        VolumeActorId,
        TabletID(),
        State->GetBlockSize(),
        msg->ShouldCalculateChecksums,
        StorageAccessMode,
        std::unique_ptr<TEvPartitionCommonPrivate::TEvReadBlobRequest>(
            msg.Release()),
        GetDowntimeThreshold(
            *DiagnosticsConfig,
            PartitionConfig.GetStorageMediaKind()));

    if (blob.TabletID() != TabletID()) {
        // Treat this situation as we were reading from base disk.
        // TODO(svartmetal): verify that |blobTabletId| corresponds to base
        // disk partition tablet.
        auto actorId = NCloud::Register(ctx, std::move(readBlobActor));
        Actors.Insert(actorId);
        return;
    }

    ui32 channel = blob.Channel();

    State->EnqueueIORequest(channel, std::move(readBlobActor));
    ProcessIOQueue(ctx, channel);
}

void TPartitionActor::HandleReadBlobCompleted(
    const TEvPartitionCommonPrivate::TEvReadBlobCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    Actors.Erase(ev->Sender);

    const auto& blobTabletId = msg->BlobId.TabletID();

    if (FAILED(msg->GetStatus())) {
        if (blobTabletId != TabletID()) {
            // Treat this situation as we were reading from base disk.
            // TODO(svartmetal): verify that |blobTabletId| corresponds to base
            // disk partition tablet.

            LOG_DEBUG(
                ctx,
                TBlockStoreComponents::PARTITION,
                "[%lu] Failed to read blob from base disk, blob tablet: %lu error: %s",
                TabletID(),
                blobTabletId,
                FormatError(msg->GetError()).data());
            return;
        }

        if (State->IncrementReadBlobErrorCount()
                >= Config->GetMaxReadBlobErrorsBeforeSuicide())
        {
            LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
                "[%lu] Stop tablet because of too many ReadBlob errors: %s",
                TabletID(),
                FormatError(msg->GetError()).data());

            ReportTabletBSFailure();
            Suicide(ctx);
        } else {
            LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
                "[%lu] ReadBlob error happened: %s",
                TabletID(),
                FormatError(msg->GetError()).data());
        }
        return;
    }

    const ui32 channel = msg->BlobId.Channel();
    const ui32 groupId = msg->GroupId;
    const bool isOverlayDisk = blobTabletId != TabletID();
    UpdateNetworkStat(ctx.Now(), msg->BytesCount);
    UpdateExecutorStats(ctx);
    if (groupId == Max<ui32>()) {
        Y_DEBUG_ABORT_UNLESS(
            0,
            "HandleReadBlobCompleted: invalid blob id received");
    } else {
        UpdateReadThroughput(
            ctx.Now(),
            channel,
            groupId,
            msg->BytesCount,
            isOverlayDisk);
        State->RegisterSuccess(ctx.Now(), groupId);
    }

    if (isOverlayDisk) {
        // Treat this situation as we were reading from base disk.
        // TODO(svartmetal): verify that |blobTabletId| corresponds to base
        // disk partition tablet.

        PartCounters->RequestCounters.ReadBlob.AddRequest(
            msg->RequestTime.MicroSeconds(),
            msg->BytesCount,
            1,
            EChannelDataKind::External);

        return;
    }

    PartCounters->RequestCounters.ReadBlob.AddRequest(
        msg->RequestTime.MicroSeconds(),
        msg->BytesCount,
        1,
        State->GetChannelDataKind(channel));

    State->CompleteIORequest(channel);

    ProcessIOQueue(ctx, channel);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
