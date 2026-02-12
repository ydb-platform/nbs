#include "io_companion.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/partition_common/actor_read_blob.h>

#include <cloud/storage/core/libs/common/alloc.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TIOCompanion::HandleReadBlob(
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

    const auto& blob = msg->BlobId;
    ui32 channel = blob.Channel();
    ui32 groupId = Info()->GroupFor(channel, msg->BlobId.Generation());
    ui64 bsGroupOperationId = BSGroupOperationId++;

    auto readBlobActor = std::make_unique<TReadBlobActor>(
        requestInfo,
        ctx.SelfID,
        VolumeActorId,
        TabletID,
        PartitionConfig.GetBlockSize(),
        msg->ShouldCalculateChecksums,
        StorageAccessMode,
        std::unique_ptr<TEvPartitionCommonPrivate::TEvReadBlobRequest>(
            msg.Release()),
        GetDowntimeThreshold(
            *DiagnosticsConfig,
            PartitionConfig.GetStorageMediaKind()),
        bsGroupOperationId,
        DiagnosticsConfig->GetPassTraceIdToBlobstorage());

    if (blob.TabletID() != TabletID) {
        // Treat this situation as we were reading from base disk.
        // TODO(svartmetal): verify that |blobTabletId| corresponds to base
        // disk partition tablet.
        auto actorId = NCloud::Register(ctx, std::move(readBlobActor));
        Actors.Insert(actorId);
        BSGroupOperationTimeTracker.OnStarted(
            bsGroupOperationId,
            groupId,
            TBSGroupOperationTimeTracker::EOperationType::Read,
            GetCycleCount(),
            PartitionConfig.GetBlockSize());
        return;
    }

    ChannelsState.EnqueueIORequest(
        channel,
        std::move(readBlobActor),
        bsGroupOperationId,
        groupId,
        TBSGroupOperationTimeTracker::EOperationType::Read,
        PartitionConfig.GetBlockSize());
    ProcessIOQueue(ctx, channel);
}

void TIOCompanion::HandleReadBlobCompleted(
    const TEvPartitionCommonPrivate::TEvReadBlobCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    BSGroupOperationTimeTracker.OnFinished(
        msg->BSGroupOperationId,
        GetCycleCount());

    Actors.Erase(ev->Sender);

    const auto& blobTabletId = msg->BlobId.TabletID();

    const ui32 channel = msg->BlobId.Channel();
    const ui32 groupId = msg->GroupId;
    const bool isOverlayDisk = blobTabletId != TabletID;
    Client.UpdateNetworkStat(ctx.Now(), msg->BytesCount);
    if (groupId == Max<ui32>()) {
        Y_DEBUG_ABORT_UNLESS(
            0,
            "HandleReadBlobCompleted: invalid blob id received");
    } else {
        Client.UpdateReadThroughput(
            ctx.Now(),
            channel,
            groupId,
            msg->BytesCount,
            isOverlayDisk);
    }

    if (isOverlayDisk) {
        // Treat this situation as we were reading from base disk.
        // TODO(svartmetal): verify that |blobTabletId| corresponds to base
        // disk partition tablet.

        Client.GetPartCounters().RequestCounters.ReadBlob.AddRequest(
            msg->RequestTime.MicroSeconds(),
            msg->BytesCount,
            1,
            EChannelDataKind::External);

        return;
    }

    Client.GetPartCounters().RequestCounters.ReadBlob.AddRequest(
        msg->RequestTime.MicroSeconds(),
        msg->BytesCount,
        1,
        ChannelsState.GetChannelDataKind(channel));

    ChannelsState.CompleteIORequest(channel);

    if (FAILED(msg->GetStatus())) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s ReadBlob error happened: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(msg->GetError()).c_str());

        if (blobTabletId != TabletID) {
            // Treat this situation as we were reading from base disk.
            // TODO(svartmetal): verify that |blobTabletId| corresponds to base
            // disk partition tablet.

            LOG_WARN(
                ctx,
                TBlockStoreComponents::PARTITION,
                "%s Failed to read blob from base disk, blob tablet: %lu "
                "error: %s",
                LogTitle.GetWithTime().c_str(),
                blobTabletId,
                FormatError(msg->GetError()).c_str());
        }

        if (msg->DeadlineSeen) {
            Client.GetPartCounters().Simple.ReadBlobDeadlineCount.Increment(1);
        }

        if (++ReadBlobErrorCount >= Config->GetMaxReadBlobErrorsBeforeSuicide())
        {
            ReportTabletBSFailure(
                TStringBuilder()
                    << "Stop tablet because of too many ReadBlob errors"
                    << FormatError(msg->GetError()),
                {{"disk", PartitionConfig.GetDiskId()},
                 {"actor", ev->Sender.ToString()},
                 {"group", groupId}});
            Client.Poison(ctx);
            return;
        }
    } else {
        Client.RegisterSuccess(ctx.Now(), groupId);
    }

    ProcessIOQueue(ctx, channel);
}

}   // namespace NCloud::NBlockStore::NStorage
