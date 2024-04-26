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

    if (DstActorId) {
        ProcessingBlocks.SkipProcessedRanges();
    } else {
        ProcessingBlocks.AbortProcessing();
    }
}

void TNonreplicatedPartitionMigrationCommonActor::StartWork(
    const NActors::TActorContext& ctx)
{
    MigrationEnabled = true;
    ScheduleRangeMigration(ctx);
}

void TNonreplicatedPartitionMigrationCommonActor::StartRangeMigration(
    const NActors::TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(IsMigrationAllowed() && !IsIoDepthLimitReached());

    // Migrate failed ranges first.
    if (StartDeferredMigration(ctx)) {
        ScheduleRangeMigration(ctx);
        return;
    }

    if (!ProcessingBlocks.IsProcessingStarted()) {
        // Migration of last range already started.
        return;
    }

    const auto migrationRange = ProcessingBlocks.BuildProcessingRange();

    // Check migration intersects with inflight requests.
    if (IsOverlapsWithInflightWriteAndZero(migrationRange)) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "[%s] Range migration rejected, range: %s",
            DiskId.c_str(),
            DescribeRange(migrationRange).c_str());
        return;
    }

    ProcessingBlocks.AdvanceProcessingIndex();
    MigrateRange(ctx, migrationRange);
    ScheduleRangeMigration(ctx);
}

bool TNonreplicatedPartitionMigrationCommonActor::StartDeferredMigration(
    const NActors::TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(IsMigrationAllowed() && !IsIoDepthLimitReached());

    for (auto [key, range] : DeferredMigrations) {
        if (IsOverlapsWithInflightWriteAndZero(range)) {
            continue;
        }
        MigrateRange(ctx, range);
        DeferredMigrations.erase(key);
        return true;
    }

    return false;
}

void TNonreplicatedPartitionMigrationCommonActor::MigrateRange(
    const NActors::TActorContext& ctx,
    TBlockRange64 range)
{
    Y_DEBUG_ABORT_UNLESS(IsMigrationAllowed() && !IsIoDepthLimitReached());

    LastRangeMigrationStartTs = ctx.Now();

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Migrating range: %s",
        DiskId.c_str(),
        DescribeRange(range).c_str());

    auto [_, inserted] = MigrationsInProgress.emplace(range.Start, range);
    Y_DEBUG_ABORT_UNLESS(inserted);

    NCloud::Register<TCopyRangeActor>(
        ctx,
        CreateRequestInfo(
            SelfId(),
            0,   // cookie
            MakeIntrusive<TCallContext>()),
        BlockSize,
        range,
        SrcActorId,
        DstActorId,
        RWClientId,
        BlockDigestGenerator);
}

////////////////////////////////////////////////////////////////////////////////

bool TNonreplicatedPartitionMigrationCommonActor::IsMigrationAllowed() const
{
    return SrcActorId && DstActorId && MigrationEnabled;
}

bool TNonreplicatedPartitionMigrationCommonActor::IsIoDepthLimitReached() const
{
    Y_DEBUG_ABORT_UNLESS(MigrationsInProgress.size() <= MaxIoDepth);
    return MigrationsInProgress.size() >= MaxIoDepth;
}

bool TNonreplicatedPartitionMigrationCommonActor::
    IsOverlapsWithInflightWriteAndZero(TBlockRange64 range) const
{
    for (const auto& [key, requestInfo]:
         WriteAndZeroRequestsInProgress.AllRequests())
    {
        if (range.Overlaps(requestInfo.Value)) {
            return true;
        }
    }
    return false;
}

void TNonreplicatedPartitionMigrationCommonActor::HandleRangeMigrated(
    const TEvNonreplPartitionPrivate::TEvRangeMigrated::TPtr& ev,
    const TActorContext& ctx)
{
    if (!IsMigrationAllowed()) {
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

    size_t erasedCount = MigrationsInProgress.erase(msg->Range.Start);
    Y_DEBUG_ABORT_UNLESS(erasedCount == 1);

    if (HasError(msg->GetError())) {
        LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
            "[%s] Range migration failed: %s, error: %s",
            DiskId.c_str(),
            DescribeRange(msg->Range).c_str(),
            FormatError(msg->GetError()).c_str());

        if (GetErrorKind(msg->GetError()) != EErrorKind::ErrorRetriable) {
            ReportMigrationFailed();
            MigrationOwner->OnMigrationError(ctx);
            MigrationEnabled = false;
            return;
        }

        // Schedule to retry the migration of the failed range.
        DeferredMigrations.emplace(msg->Range.Start, msg->Range);
        ScheduleRangeMigration(ctx);
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Range %s migrated",
        DiskId.c_str(),
        DescribeRange(msg->Range).c_str());

    NotifyMigrationProgressIfNeeded(ctx, msg->Range);
    NotifyMigrationFinishedIfNeeded(ctx);
    ScheduleRangeMigration(ctx);
}

void TNonreplicatedPartitionMigrationCommonActor::
    NotifyMigrationProgressIfNeeded(
        const TActorContext& ctx,
        TBlockRange64 migratedRange)
{
    const ui64 nextCachedProgress =
        ProcessingBlocks.GetLastReportedProcessingIndex() +
        Config->GetMigrationIndexCachingInterval();

    if (migratedRange.Contains(nextCachedProgress - 1)) {
        // Perhaps we have just migrated a range that has been deferred for a
        // long time, and the current cached range has already overtaken it.
        CachedMigrationProgressAchieved = std::max(
            migratedRange.End + 1,
            CachedMigrationProgressAchieved.value_or(0));
    }

    if (!CachedMigrationProgressAchieved) {
        return;
    }

    Cout << "CachedMigrationProgressAchieved=" << *CachedMigrationProgressAchieved << Endl;
    for (const auto& [id, range]: MigrationsInProgress) {
        if (range.End < *CachedMigrationProgressAchieved) {
            return;
        }
    }
    for (const auto& [id, range]: DeferredMigrations) {
        if (range.End < *CachedMigrationProgressAchieved) {
            return;
        }
    }

    ProcessingBlocks.SetLastReportedProcessingIndex(
        *CachedMigrationProgressAchieved);
    MigrationOwner->OnMigrationProgress(ctx, *CachedMigrationProgressAchieved);
    CachedMigrationProgressAchieved = std::nullopt;
}

void TNonreplicatedPartitionMigrationCommonActor::
    NotifyMigrationFinishedIfNeeded(const TActorContext& ctx)
{
    if (ProcessingBlocks.IsProcessingStarted() ||
        !MigrationsInProgress.empty() || !DeferredMigrations.empty())
    {
        return;
    }

    MigrationOwner->OnMigrationFinished(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::ScheduleRangeMigration(
    const TActorContext& ctx)
{
    if (MigrateRangeScheduled || !IsMigrationAllowed() ||
        IsIoDepthLimitReached())
    {
        return;
    }

    auto delayBetweenMigrations =
        ProcessingBlocks.IsProcessingStarted()
            ? MigrationOwner->CalculateMigrationTimeout(
                  ProcessingBlocks.BuildProcessingRange())
            : LastUsedDelay;
    LastUsedDelay = delayBetweenMigrations;

    const auto deadline =
        LastRangeMigrationStartTs + delayBetweenMigrations;

    if (ctx.Now() >= deadline) {
        StartRangeMigration(ctx);
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Schedule migrating next range after %s",
        DiskId.c_str(),
        delayBetweenMigrations.ToString().Quote().c_str());

    MigrateRangeScheduled = true;
    ctx.Schedule(
        delayBetweenMigrations,
        new TEvNonreplPartitionPrivate::TEvMigrateNextRange());
}

void TNonreplicatedPartitionMigrationCommonActor::HandleMigrateNextRange(
    const TEvNonreplPartitionPrivate::TEvMigrateNextRange::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    MigrateRangeScheduled = false;

    if (!IsMigrationAllowed() || IsIoDepthLimitReached()) {
        return;
    }

    StartRangeMigration(ctx);
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
