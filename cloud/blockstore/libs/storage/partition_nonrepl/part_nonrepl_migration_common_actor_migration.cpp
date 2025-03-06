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
#include <cloud/blockstore/libs/storage/partition_nonrepl/direct_copy_range.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::InitWork(
    const NActors::TActorContext& ctx,
    NActors::TActorId migrationSrcActorId,
    NActors::TActorId srcActorId,
    NActors::TActorId dstActorId,
    bool takeOwnershipOverActors,
    std::unique_ptr<TMigrationTimeoutCalculator> timeoutCalculator)
{
    MigrationSrcActorId = migrationSrcActorId;
    SrcActorId = srcActorId;
    DstActorId = dstActorId;
    TimeoutCalculator = std::move(timeoutCalculator);
    STORAGE_CHECK_PRECONDITION(TimeoutCalculator);

    ActorOwner = takeOwnershipOverActors;
    if (ActorOwner) {
        PoisonPillHelper.TakeOwnership(ctx, SrcActorId);
        PoisonPillHelper.TakeOwnership(ctx, DstActorId);
    }

    GetDeviceForRangeCompanion.SetDelegate(SrcActorId);

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
    NotifyMigrationFinishedIfNeeded(ctx);
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

    const bool inserted = MigrationsInProgress.TryInsert(range);
    if (!inserted) {
        ReportOverlappingRangesDuringMigrationDetected(
            TStringBuilder() << "An error occurred while inserting a range to "
                                "the container. Range: "
                             << range << ", diskId: " << DiskId);
    }

    if (Config->GetUseDirectCopyRange()) {
        NCloud::Register<TDirectCopyRangeActor>(
            ctx,
            CreateRequestInfo(
                SelfId(),
                0,   // cookie
                MakeIntrusive<TCallContext>()),
            BlockSize,
            range,
            MigrationSrcActorId,
            DstActorId,
            RWClientId,
            BlockDigestGenerator);
    } else {
        NCloud::Register<TCopyRangeActor>(
            ctx,
            CreateRequestInfo(
                SelfId(),
                0,   // cookie
                MakeIntrusive<TCallContext>()),
            BlockSize,
            range,
            MigrationSrcActorId,
            DstActorId,
            RWClientId,
            BlockDigestGenerator);
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TNonreplicatedPartitionMigrationCommonActor::IsMigrationAllowed() const
{
    return MigrationSrcActorId && DstActorId && MigrationEnabled;
}

bool TNonreplicatedPartitionMigrationCommonActor::IsMigrationFinished() const
{
    return !ProcessingBlocks.IsProcessing() && MigrationsInProgress.Empty() &&
           DeferredMigrations.Empty();
}

bool TNonreplicatedPartitionMigrationCommonActor::IsIoDepthLimitReached() const
{
    Y_DEBUG_ABORT_UNLESS(MigrationsInProgress.Size() <= MaxIoDepth);
    return MigrationsInProgress.Size() >= MaxIoDepth;
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
    if (!DeferredMigrations.Empty()) {
        return DeferredMigrations.LeftmostRange();
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

    TDisjointRangeSetIterator deferredIterator{DeferredMigrations};
    // First of all, we are trying to continue the migration range from
    // DeferredMigrations.
    while (deferredIterator.HasNext()) {
        auto range = deferredIterator.Next();
        // We can only perform migration that does not overlap with user
        // requests.
        if (!OverlapsWithInflightWriteAndZero(range)) {
            const bool removed = DeferredMigrations.Remove(range);
            Y_DEBUG_ABORT_UNLESS(removed);
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
    STORAGE_CHECK_PRECONDITION(TimeoutCalculator);

    auto* msg = ev->Get();

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

    if (msg->ExecutionSide == EExecutionSide::Remote) {
        const auto networkBytes = msg->Range.Size() * BlockSize;
        const auto execTime = msg->ReadDuration + msg->WriteDuration;

        MigrationCounters->RequestCounters.CopyBlocks.AddRequest(
            execTime.MicroSeconds(),
            networkBytes);
    } else {
        NetworkBytes += 2 * msg->Range.Size() * BlockSize;
        CpuUsage += CyclesToDurationSafe(msg->ExecCycles);
    }

    const bool erased = MigrationsInProgress.Remove(msg->Range);
    Y_DEBUG_ABORT_UNLESS(erased);

    if (HasError(msg->GetError())) {
        LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
            "[%s] Range migration failed: %s, error: %s",
            DiskId.c_str(),
            DescribeRange(msg->Range).c_str(),
            FormatError(msg->GetError()).c_str());

        if (GetErrorKind(msg->GetError()) != EErrorKind::ErrorRetriable) {
            OnMigrationNonRetriableError(ctx);
            return;
        }

        // Schedule to retry the migration of the failed range.
        const bool inserted = DeferredMigrations.TryInsert(msg->Range);
        if (!inserted) {
            ReportOverlappingRangesDuringMigrationDetected(
                TStringBuilder()
                << "Can't defer a range to migrate later. Range: " << msg->Range
                << ", diskId: " << DiskId);
        }
        ScheduleRangeMigration(ctx);
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Range %s migrated. Recommended bandwidth: %.2f MiB",
        DiskId.c_str(),
        DescribeRange(msg->Range).c_str(),
        static_cast<double>(msg->RecommendedBandwidth) / 1_MB);

    if (msg->AllZeroes) {
        NonZeroRangesMap.MarkNotChanged(msg->Range);
    }
    TimeoutCalculator->SetRecommendedBandwidth(msg->RecommendedBandwidth);
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
        ProcessingBlocks.GetLastReportedProcessingIndex().value_or(0) +
        Config->GetMigrationIndexCachingInterval();

    if (nextCachedProgress <= migratedRange.End + 1) {
        MigrationThresholdAchieved = true;
    }

    if (!MigrationThresholdAchieved) {
        return;
    }

    if (!MigrationsInProgress.Empty() &&
        MigrationsInProgress.LeftmostRange().End < nextCachedProgress)
    {
        return;
    }
    if (!DeferredMigrations.Empty() &&
        DeferredMigrations.LeftmostRange().End < nextCachedProgress)
    {
        return;
    }

    ProcessingBlocks.SetLastReportedProcessingIndex(nextCachedProgress);
    MigrationOwner->OnMigrationProgress(ctx, nextCachedProgress);
    MigrationThresholdAchieved = false;
}

void TNonreplicatedPartitionMigrationCommonActor::
    NotifyMigrationFinishedIfNeeded(const TActorContext& ctx)
{
    if (!IsMigrationFinished()) {
        return;
    }

    MigrationOwner->OnMigrationFinished(ctx);
}

TString TNonreplicatedPartitionMigrationCommonActor::GetNonZeroBlocks(
    TBlockRange64 range) const
{
    return NonZeroRangesMap.GetChangedBlocks(range);
}

const TStorageConfigPtr&
TNonreplicatedPartitionMigrationCommonActor::GetConfig() const
{
    return Config;
}

const TDiagnosticsConfigPtr&
TNonreplicatedPartitionMigrationCommonActor::GetDiagnosticsConfig() const
{
    return DiagnosticsConfig;
}

TDuration
TNonreplicatedPartitionMigrationCommonActor::CalculateMigrationTimeout(
    TBlockRange64 range) const
{
    STORAGE_CHECK_PRECONDITION(TimeoutCalculator);
    return TimeoutCalculator->CalculateTimeout(range);
}

void TNonreplicatedPartitionMigrationCommonActor::DoRegisterTrafficSource(
    const TActorContext& ctx)
{
    if (!IsMigrationAllowed() || IsMigrationFinished()) {
        return;
    }

    STORAGE_CHECK_PRECONDITION(TimeoutCalculator);
    TimeoutCalculator->RegisterTrafficSource(ctx);
    ctx.Schedule(
        RegisterBackgroundTrafficDuration,
        new TEvents::TEvWakeup(WR_REGISTER_TRAFFIC_SOURCE));
}

void TNonreplicatedPartitionMigrationCommonActor::OnMigrationNonRetriableError(
    const NActors::TActorContext& ctx)
{
    ReportMigrationFailed();
    MigrationOwner->OnMigrationError(ctx);
    MigrationEnabled = false;
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::ScheduleRangeMigration(
    const TActorContext& ctx)
{
    if (!MigrationEnabled || RangeMigrationScheduled || IsIoDepthLimitReached())
    {
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
