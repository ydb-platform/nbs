#include "part_mirror_resync_actor.h"

#include "part_mirror_resync_util.h"
#include "resync_range.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/common/verify.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::ContinueResyncIfNeeded(
    const NActors::TActorContext& ctx)
{
    if (ResyncFinished) {
        return;
    }

    if (!State.SkipResyncedRanges()) {
        ResyncFinished = true;

        NCloud::Send(
            ctx,
            PartConfig->GetParentActorId(),
            std::make_unique<TEvVolume::TEvResyncFinished>()
        );

        return;
    }

    if (State.GetActiveResyncRangeSet().empty()) {
        const auto range = State.BuildResyncRange();
        const auto rangeId =
            BlockRange2RangeId(range, PartConfig->GetBlockSize());
        State.AddPendingResyncRange(rangeId.first);
    }

    ScheduleResyncNextRange(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::ScheduleResyncNextRange(const TActorContext& ctx)
{
    ctx.Schedule(
        ResyncNextRangeInterval,
        new TEvNonreplPartitionPrivate::TEvResyncNextRange());
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::ResyncNextRange(const TActorContext& ctx)
{
    ui32 rangeId = 0;
    if (!State.StartNextResyncRange(&rangeId)) {
        return;
    }

    const auto resyncRange =
        RangeId2BlockRange(rangeId, PartConfig->GetBlockSize());

    for (const auto& [key, requestInfo] :
         WriteAndZeroRequestsInProgress.AllRequests())
    {
        const auto& requestRange = requestInfo.Value;
        if (resyncRange.Overlaps(requestRange)) {
            LOG_DEBUG(
                ctx,
                TBlockStoreComponents::PARTITION,
                "[%s] Resyncing range %s rejected due to inflight write to %s",
                PartConfig->GetName().c_str(),
                DescribeRange(resyncRange).c_str(),
                DescribeRange(requestRange).c_str());

            // Reschedule range
            State.FinishResyncRange(rangeId);
            State.AddPendingResyncRange(rangeId);
            ScheduleResyncNextRange(ctx);
            return;
        }
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Resyncing range %s",
        PartConfig->GetName().c_str(),
        DescribeRange(resyncRange).c_str());

    auto requestInfo = CreateRequestInfo(
        SelfId(),
        0,  // cookie
        MakeIntrusive<TCallContext>()
    );

    TVector<TReplicaDescriptor> replicas;
    // filtering out replicas with fresh devices
    for (ui32 i = 0; i < Replicas.size(); ++i) {
        if (State.DevicesReadyForReading(i, resyncRange))
        {
            replicas.push_back(Replicas[i]);
        }
    }

    auto resyncActor = MakeResyncRangeActor(
        std::move(requestInfo),
        PartConfig->GetBlockSize(),
        resyncRange,
        std::move(replicas),
        State.GetRWClientId(),
        BlockDigestGenerator,
        ResyncPolicy,
        EBlockRangeChecksumStatus::Unknown,
        State.GetReplicaInfos()[0].Config->GetParentActorId(),
        Config->GetAssignIdToWriteAndZeroRequestsEnabled());
    ctx.Register(resyncActor.release());
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::HandleResyncNextRange(
    const TEvNonreplPartitionPrivate::TEvResyncNextRange::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ResyncNextRange(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::HandleRangeResynced(
    const TEvNonreplPartitionPrivate::TEvRangeResynced::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto range = msg->Range;
    const auto rangeId = BlockRange2RangeId(range, PartConfig->GetBlockSize());

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Range %s resync finished: %s",
        PartConfig->GetName().c_str(),
        DescribeRange(range).c_str(),
        FormatError(msg->GetError()).c_str());

    STORAGE_VERIFY(
        rangeId.first == rangeId.second,
        TWellKnownEntityTypes::DISK,
        PartConfig->GetName());

    State.FinishResyncRange(rangeId.first);

    ProfileLog->Write({
        .DiskId = PartConfig->GetName(),
        .Ts = msg->ChecksumStartTs,
        .Request = IProfileLog::TSysReadWriteRequest{
            .RequestType = ESysRequestType::Resync,
            .Duration = msg->ChecksumDuration + msg->ReadDuration,
            .Ranges = {msg->Range},
        },
    });

    ProfileLog->Write({
        .DiskId = PartConfig->GetName(),
        .Ts = msg->WriteStartTs,
        .Request = IProfileLog::TSysReadWriteRequest{
            .RequestType = ESysRequestType::Resync,
            .Duration = msg->WriteDuration,
            .Ranges = {msg->Range},
        },
    });

    if (msg->AffectedBlockInfos) {
        ProfileLog->Write({
            .DiskId = PartConfig->GetName(),
            .Ts = msg->WriteStartTs,
            .Request = IProfileLog::TSysReadWriteRequestBlockInfos{
                .RequestType = ESysRequestType::Resync,
                .BlockInfos = std::move(msg->AffectedBlockInfos),
                .CommitId = 0,
            },
        });
    }

    CpuUsage += CyclesToDurationSafe(msg->ExecCycles);

    if (HasError(msg->GetError())) {
        LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
            "[%s] Range %s resync failed: %s",
            PartConfig->GetName().c_str(),
            DescribeRange(range).c_str(),
            FormatError(msg->GetError()).c_str());

        if (GetErrorKind(msg->GetError()) == EErrorKind::ErrorRetriable) {
            // Reschedule range
            State.AddPendingResyncRange(rangeId.first);
            ScheduleResyncNextRange(ctx);
        } else {
            ReportResyncFailed(
                FormatError(msg->GetError()),
                {{"disk", PartConfig->GetName()},
                 {"range", DescribeRange(range)}});
        }

        TDeque<TPostponedRead> postponedReads;
        for (auto& pr: PostponedReads) {
            const auto readRangeId = BlockRange2RangeId(
                pr.BlockRange,
                PartConfig->GetBlockSize());
            if (readRangeId == rangeId) {
                RejectPostponedRead(pr);
            } else {
                postponedReads.push_back(std::move(pr));
            }
        }
        PostponedReads.swap(postponedReads);

        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Range %s resynced",
        PartConfig->GetName().c_str(),
        DescribeRange(range).c_str());

    if (CritOnChecksumMismatch && msg->WriteStartTs > TInstant()) {
        ReportMirroredDiskResyncChecksumMismatch(
            "Checksum mismatch during resync in range",
            {{"disk", PartConfig->GetName()}, {"range", DescribeRange(range)}});
    }

    auto resyncRange = State.BuildResyncRange();
    const auto currentIndex = State.GetLastReportedResyncIndex();
    const auto step = Config->GetResyncIndexCachingInterval();

    if (currentIndex.value_or(0) + step < resyncRange.Start) {
        State.SetLastReportedResyncIndex(resyncRange.Start);

        NCloud::Send(
            ctx,
            PartConfig->GetParentActorId(),
            std::make_unique<TEvVolume::TEvUpdateResyncState>(
                resyncRange.Start));
    }

    State.MarkResynced(range);
    while (PostponedReads.size()) {
        auto& pr = PostponedReads.front();
        const auto readRangeId = BlockRange2RangeId(
            pr.BlockRange,
            PartConfig->GetBlockSize());

        bool resynced = true;
        for (ui32 id = readRangeId.first; id <= readRangeId.second; ++id) {
            const auto blockRange =
                RangeId2BlockRange(id, PartConfig->GetBlockSize());
            if (!State.IsResynced(blockRange)) {
                resynced = false;
                break;
            }
        }

        if (!resynced) {
            break;
        }

        TAutoPtr<IEventHandle> eh(pr.Ev.release());
        Receive(eh);

        PostponedReads.pop_front();
    }

    ContinueResyncIfNeeded(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::HandleResyncStateUpdated(
    const TEvVolume::TEvResyncStateUpdated::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Y_UNUSED(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::HandleRWClientIdChanged(
    const TEvVolume::TEvRWClientIdChanged::TPtr& ev,
    const TActorContext& ctx)
{
    auto rwClientId = ev->Get()->RWClientId;

    NCloud::Send(
        ctx,
        MirrorActorId,
        std::make_unique<TEvVolume::TEvRWClientIdChanged>(rwClientId));

    for (const auto& replica: Replicas) {
        NCloud::Send(
            ctx,
            replica.ActorId,
            std::make_unique<TEvVolume::TEvRWClientIdChanged>(rwClientId));
    }

    State.SetRWClientId(std::move(ev->Get()->RWClientId));
}

}   // namespace NCloud::NBlockStore::NStorage
