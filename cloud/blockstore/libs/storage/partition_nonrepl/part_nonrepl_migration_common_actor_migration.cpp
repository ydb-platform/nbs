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
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::InitWork(
    const NActors::TActorContext& ctx,
    NActors::TActorId srcActorId,
    NActors::TActorId dstActorId,
    std::unique_ptr<TMigrationTimeoutCalculator> timeoutCalculator)
{
    SrcActorId = srcActorId;
    DstActorId = dstActorId;
    TimeoutCalculator = std::move(timeoutCalculator);
    Y_DEBUG_ABORT_UNLESS(TimeoutCalculator);

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
    MigrationEnabled = true;
    DoRegisterTrafficSource(ctx);
    ScheduleRangeMigration(ctx);
}

void TNonreplicatedPartitionMigrationCommonActor::StartRangeMigration(
    const NActors::TActorContext& ctx)
{
    STORAGE_CHECK_PRECONDITION(
        IsMigrationAllowed() && !IsIoDepthLimitReached());

    auto migrationRange = TakeNextMigrationRange(ctx);
    if (!migrationRange) {
        // Migration of the last range has already started.
        return;
    }

    MigrateRange(ctx, *migrationRange);
    ScheduleRangeMigration(ctx);
}

void TNonreplicatedPartitionMigrationCommonActor::MigrateRange(
    const NActors::TActorContext& ctx,
    TBlockRange64 range)
{
    STORAGE_CHECK_PRECONDITION(
        IsMigrationAllowed() && !IsIoDepthLimitReached());

    LastRangeMigrationStartTs = ctx.Now();

    LOG_DEBUG(
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

bool TNonreplicatedPartitionMigrationCommonActor::IsMigrationFinished() const
{
    return !ProcessingBlocks.IsProcessing() && MigrationsInProgress.empty() &&
           DeferredMigrations.empty();
}

bool TNonreplicatedPartitionMigrationCommonActor::IsIoDepthLimitReached() const
{
    Y_DEBUG_ABORT_UNLESS(MigrationsInProgress.size() <= MaxIoDepth);
    return MigrationsInProgress.size() >= MaxIoDepth;
}

bool TNonreplicatedPartitionMigrationCommonActor::
    OverlapsWithInflightWriteAndZero(TBlockRange64 range) const
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

std::optional<TBlockRange64>
TNonreplicatedPartitionMigrationCommonActor::GetNextMigrationRange() const
{
    // Attention! It is necessary to keep the logic consistent with
    // TakeNextMigrationRange().

    if (!IsMigrationAllowed()) {
        return std::nullopt;
    }

    // First of all, we are trying to continue the migration range from
    // DeferredMigrations.
    if (!DeferredMigrations.empty()) {
        return DeferredMigrations.begin()->second;
    }

    // Secondly, we are trying to migrate a new range.
    if (ProcessingBlocks.IsProcessing()) {
        return ProcessingBlocks.BuildProcessingRange();
    }

    return std::nullopt;
}

std::optional<TBlockRange64>
TNonreplicatedPartitionMigrationCommonActor::TakeNextMigrationRange(
    const TActorContext& ctx)
{
    // Attention! It is necessary to keep the logic consistent with
    // GetNextMigrationRange().

    if (!IsMigrationAllowed()) {
        return std::nullopt;
    }

    // First of all, we are trying to continue the migration range from
    // DeferredMigrations.
    for (auto [key, range]: DeferredMigrations) {
        // We can only perform migration that does not overlap with user
        // requests.
        if (!OverlapsWithInflightWriteAndZero(range)) {
            DeferredMigrations.erase(key);
            return range;
        }
    }

    // Secondly, we are trying to migrate a new range.
    if (ProcessingBlocks.IsProcessing()) {
        auto range = ProcessingBlocks.BuildProcessingRange();

        // Check whether migration intersects with inflight user requests.
        if (OverlapsWithInflightWriteAndZero(range)) {
            LOG_DEBUG(
                ctx,
                TBlockStoreComponents::PARTITION,
                "[%s] Range migration rejected, range: %s",
                DiskId.c_str(),
                DescribeRange(range).c_str());
            return std::nullopt;
        }
        ProcessingBlocks.AdvanceProcessingIndex();
        return range;
    }

    return std::nullopt;
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
        LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
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

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Range %s migrated",
        DiskId.c_str(),
        DescribeRange(msg->Range).c_str());

    if (msg->AllZeroes) {
        ChangedRangesMap.MarkNotChanged(msg->Range);
    }
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

    if (migratedRange.Contains(nextCachedProgress - 1) ||
        migratedRange.Start > nextCachedProgress)
    {
        CachedMigrationProgressAchieved = nextCachedProgress;
    }

    if (!CachedMigrationProgressAchieved) {
        return;
    }

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
    if (!IsMigrationFinished())
    {
        return;
    }

    MigrationOwner->OnMigrationFinished(ctx);
}

TString TNonreplicatedPartitionMigrationCommonActor::GetChangedBlocks(
    TBlockRange64 range) const
{
    return ChangedRangesMap.GetChangedBlocks(range);
}

TDuration
TNonreplicatedPartitionMigrationCommonActor::CalculateMigrationTimeout(
    TBlockRange64 range) const
{
    Y_DEBUG_ABORT_UNLESS(TimeoutCalculator);
    return TimeoutCalculator->CalculateTimeout(range);
}

void TNonreplicatedPartitionMigrationCommonActor::DoRegisterTrafficSource(
    const TActorContext& ctx)
{
    if (!IsMigrationAllowed() || IsMigrationFinished()) {
        return;
    }

    Y_DEBUG_ABORT_UNLESS(TimeoutCalculator);
    TimeoutCalculator->RegisterTrafficSource(ctx);
    ctx.Schedule(
        RegisterBackgroundTrafficDuration,
        new TEvents::TEvWakeup(REGISTER_TRAFFIC_SOURCE));
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::ScheduleRangeMigration(
    const TActorContext& ctx)
{
    if (RangeMigrationScheduled || IsIoDepthLimitReached()) {
        return;
    }

    auto nextRange = GetNextMigrationRange();
    if (!nextRange) {
        return;
    }

    const auto delayBetweenMigrations = CalculateMigrationTimeout(*nextRange);
    const auto deadline = LastRangeMigrationStartTs + delayBetweenMigrations;

    if (ctx.Now() >= deadline) {
        StartRangeMigration(ctx);
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Schedule migrating next range after %s",
        DiskId.c_str(),
        delayBetweenMigrations.ToString().Quote().c_str());

    RangeMigrationScheduled = true;
    ctx.Schedule(
        delayBetweenMigrations,
        new TEvNonreplPartitionPrivate::TEvMigrateNextRange());
}

void TNonreplicatedPartitionMigrationCommonActor::HandleMigrateNextRange(
    const TEvNonreplPartitionPrivate::TEvMigrateNextRange::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    RangeMigrationScheduled = false;

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
