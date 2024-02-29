#include "part_nonrepl_migration_common_actor.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/copy_range.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::InitWork(
    const NActors::TActorContext& ctx,
    NActors::TActorId srcActorId,
    NActors::TActorId dstActorId)
{
    SrcActorId = srcActorId;
    DstActorId = dstActorId;

    PoisonPillHelper.TakeOwnership(ctx, SrcActorId);
    PoisonPillHelper.TakeOwnership(ctx, DstActorId);

    if (DstActorId == NActors::TActorId{}) {
        ProcessingBlocks.AbortProcessing();
    } else {
        ProcessingBlocks.SkipProcessedRanges();
    }

}

void TNonreplicatedPartitionMigrationCommonActor::StartWork(
    const NActors::TActorContext& ctx)
{
    MigrationStarted = true;
    ContinueMigrationIfNeeded(ctx);
}

void TNonreplicatedPartitionMigrationCommonActor::ContinueMigrationIfNeeded(
    const NActors::TActorContext& ctx)
{
    if (!MigrationStarted || MigrationInProgress ||
        !ProcessingBlocks.IsProcessingStarted())
    {
        return;
    }

    MigrationInProgress = true;
    ScheduleMigrateNextRange(ctx);
}

void TNonreplicatedPartitionMigrationCommonActor::MigrateNextRange(
    const NActors::TActorContext& ctx)
{
    auto requestInfo = CreateRequestInfo(
        SelfId(),
        0,  // cookie
        MakeIntrusive<TCallContext>()
    );

    const auto migrationRange = ProcessingBlocks.BuildProcessingRange();

    for (const auto& [key, requestInfo] :
         WriteAndZeroRequestsInProgress.AllRequests())
    {
        const auto& requestRange = requestInfo.Value;
        if (migrationRange.Overlaps(requestRange)) {
            LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
                "[%s] Range migration rejected, range: %s, inflight request: %s",
                DiskId.c_str(),
                DescribeRange(migrationRange).c_str(),
                DescribeRange(requestRange).c_str());
            MigrationInProgress = false;
            return;
        }
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Migrating range: %lu, count=%u",
        DiskId.c_str(),
        migrationRange.Start,
        migrationRange.Size());

    LastRangeMigrationStartTs = ctx.Now();

    NCloud::Register<TCopyRangeActor>(
        ctx,
        std::move(requestInfo),
        BlockSize,
        migrationRange,
        SrcActorId,
        DstActorId,
        RWClientId,
        BlockDigestGenerator);
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::HandleRangeMigrated(
    const TEvNonreplPartitionPrivate::TEvRangeMigrated::TPtr& ev,
    const TActorContext& ctx)
{
    Y_ABORT_UNLESS(MigrationInProgress);

    if (!ProcessingBlocks.IsProcessingStarted()) {
        // migration cancelled
        MigrationInProgress = false;
        return;
    }

    auto* msg = ev->Get();

    NetworkBytes += 2 * msg->Range.Size() * BlockSize;
    CpuUsage += CyclesToDurationSafe(msg->ExecCycles);

    ProfileLog->Write({
        .DiskId = DiskId,
        .Ts = msg->ReadStartTs,
        .Request = IProfileLog::TSysReadWriteRequest{
            .RequestType = ESysRequestType::Migration,
            .Duration = msg->ReadDuration,
            .Ranges = {msg->Range},
        },
    });

    ProfileLog->Write({
        .DiskId = DiskId,
        .Ts = msg->WriteStartTs,
        .Request = IProfileLog::TSysReadWriteRequest{
            .RequestType = ESysRequestType::Migration,
            .Duration = msg->WriteDuration,
            .Ranges = {msg->Range},
        },
    });

    if (msg->AffectedBlockInfos) {
        ProfileLog->Write({
            .DiskId = DiskId,
            .Ts = msg->WriteStartTs,
            .Request = IProfileLog::TSysReadWriteRequestBlockInfos{
                .RequestType = ESysRequestType::Migration,
                .BlockInfos = std::move(msg->AffectedBlockInfos),
                .CommitId = 0,
            },
        });
    }

    bool indexUpdated = false;

    if (HasError(msg->GetError())) {
        LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
            "[%s] Range migration failed: %lu, count=%u, error: %s",
            DiskId.c_str(),
            msg->Range.Start,
            msg->Range.Size(),
            FormatError(msg->GetError()).c_str());

        if (GetErrorKind(msg->GetError()) != EErrorKind::ErrorRetriable) {
            ReportMigrationFailed();
            MigrationOwner->OnMigrationError(ctx);
            MigrationInProgress = false;
            return;
        }
    } else {
        LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
            "[%s] Range migrated: %lu, count=%u",
            DiskId.c_str(),
            msg->Range.Start,
            msg->Range.Size());

        if (!ProcessingBlocks.AdvanceProcessingIndex()) {
            MigrationOwner->OnMigrationFinished(ctx);
            MigrationInProgress = false;
            return;
        }

        indexUpdated = true;
    }

    if (indexUpdated) {
        auto migrationRange = ProcessingBlocks.BuildProcessingRange();
        const auto currentIndex =
            ProcessingBlocks.GetLastReportedProcessingIndex();
        const auto step = Config->GetMigrationIndexCachingInterval();

        if (currentIndex + step < migrationRange.Start) {
            ProcessingBlocks.SetLastReportedProcessingIndex(
                migrationRange.Start);

            MigrationOwner->OnMigrationProgress(ctx, migrationRange.Start);
        }
    }

    ScheduleMigrateNextRange(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::ScheduleMigrateNextRange(
    const TActorContext& ctx)
{
    const auto deadline =
        LastRangeMigrationStartTs + MigrationOwner->CalculateMigrationTimeout();

    if (ctx.Now() >= deadline) {
        MigrateNextRange(ctx);
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Postponing range migration till %s",
        DiskId.c_str(),
        deadline.ToString().Quote().c_str());

    ctx.Schedule(
        deadline,
        new TEvNonreplPartitionPrivate::TEvMigrateNextRange());
}

void TNonreplicatedPartitionMigrationCommonActor::HandleMigrateNextRange(
    const TEvNonreplPartitionPrivate::TEvMigrateNextRange::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    if (!ProcessingBlocks.IsProcessingStarted()) {
        // migration cancelled
        MigrationInProgress = false;
        return;
    }

    MigrateNextRange(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::HandleRWClientIdChanged(
    const TEvVolume::TEvRWClientIdChanged::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    RWClientId = std::move(ev->Get()->RWClientId);
}

}   // namespace NCloud::NBlockStore::NStorage
