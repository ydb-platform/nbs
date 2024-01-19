#include "part_nonrepl_migration_actor.h"

#include "copy_range.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationActor::ContinueMigrationIfNeeded(
    const NActors::TActorContext& ctx)
{
    if (MigrationInProgress || !State.IsMigrationStarted()) {
        return;
    }

    MigrationInProgress = true;
    ScheduleMigrateNextRange(ctx);
}

void TNonreplicatedPartitionMigrationActor::MigrateNextRange(
    const NActors::TActorContext& ctx)
{
    auto requestInfo = CreateRequestInfo(
        SelfId(),
        0,  // cookie
        MakeIntrusive<TCallContext>()
    );

    const auto migrationRange = State.BuildMigrationRange();

    for (const auto& [key, requestInfo] :
         WriteAndZeroRequestsInProgress.AllRequests())
    {
        const auto& requestRange = requestInfo.Value;
        if (migrationRange.Overlaps(requestRange)) {
            LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
                "[%s] Range migration rejected, range: %s, inflight request: %s",
                SrcConfig->GetName().c_str(),
                DescribeRange(migrationRange).c_str(),
                DescribeRange(requestRange).c_str());
            MigrationInProgress = false;
            return;
        }
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Migrating range: %lu, count=%u",
        SrcConfig->GetName().c_str(),
        migrationRange.Start,
        migrationRange.Size());

    LastRangeMigrationStartTs = ctx.Now();

    NCloud::Register<TCopyRangeActor>(
        ctx,
        std::move(requestInfo),
        SrcConfig->GetBlockSize(),
        migrationRange,
        SrcActorId,
        DstActorId,
        State.GetRWClientId(),
        BlockDigestGenerator
    );
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationActor::FinishMigration(
    const NActors::TActorContext& ctx,
    bool isRetry)
{
    auto request = std::make_unique<TEvDiskRegistry::TEvFinishMigrationRequest>();
    request->Record.SetDiskId(SrcConfig->GetName());

    for (const auto& migration: Migrations) {
        auto* m = request->Record.AddMigrations();
        m->SetSourceDeviceId(migration.GetSourceDeviceId());
        m->SetTargetDeviceId(migration.GetTargetDevice().GetDeviceUUID());

        LOG_INFO(ctx, TBlockStoreComponents::PARTITION,
            "[%s] Migration finished: %s -> %s",
            SrcConfig->GetName().c_str(),
            m->GetSourceDeviceId().c_str(),
            m->GetTargetDeviceId().c_str());
    }

    if (isRetry) {
        const TDuration timeout = TDuration::Seconds(5);
        TActivationContext::Schedule(timeout, new IEventHandle(
            MakeDiskRegistryProxyServiceId(),
            ctx.SelfID,
            request.release())
        );
    } else {
        NCloud::Send(
            ctx,
            MakeDiskRegistryProxyServiceId(),
            std::move(request));
    }
}

void TNonreplicatedPartitionMigrationActor::HandleFinishMigrationResponse(
    const TEvDiskRegistry::TEvFinishMigrationResponse::TPtr& ev,
    const TActorContext& ctx)
{
    // TODO: backoff? FinishMigrationRequests should always succeed so, maybe,
    // no backoff needed here

    const auto& error = ev->Get()->Record.GetError();

    if (HasError(error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
            "[%s] Finish migration failed, error: %s",
            SrcConfig->GetName().c_str(),
            FormatError(error).c_str());

        if (GetErrorKind(error) != EErrorKind::ErrorRetriable) {
            ReportMigrationFailed();
            return;
        }
    }

    if (GetErrorKind(error) == EErrorKind::ErrorRetriable) {
        FinishMigration(ctx, true);
    } else {
        MigrationInProgress = false;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationActor::HandleRangeMigrated(
    const TEvNonreplPartitionPrivate::TEvRangeMigrated::TPtr& ev,
    const TActorContext& ctx)
{
    Y_ABORT_UNLESS(MigrationInProgress);

    if (!State.IsMigrationStarted()) {
        // migration cancelled
        MigrationInProgress = false;
        return;
    }

    auto* msg = ev->Get();

    NetworkBytes += 2 * msg->Range.Size() * SrcConfig->GetBlockSize();
    CpuUsage += CyclesToDurationSafe(msg->ExecCycles);

    ProfileLog->Write({
        .DiskId = SrcConfig->GetName(),
        .Ts = msg->ReadStartTs,
        .Request = IProfileLog::TSysReadWriteRequest{
            .RequestType = ESysRequestType::Migration,
            .Duration = msg->ReadDuration,
            .Ranges = {msg->Range},
        },
    });

    ProfileLog->Write({
        .DiskId = SrcConfig->GetName(),
        .Ts = msg->WriteStartTs,
        .Request = IProfileLog::TSysReadWriteRequest{
            .RequestType = ESysRequestType::Migration,
            .Duration = msg->WriteDuration,
            .Ranges = {msg->Range},
        },
    });

    if (msg->AffectedBlockInfos) {
        ProfileLog->Write({
            .DiskId = SrcConfig->GetName(),
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
            SrcConfig->GetName().c_str(),
            msg->Range.Start,
            msg->Range.Size(),
            FormatError(msg->GetError()).c_str());

        if (GetErrorKind(msg->GetError()) != EErrorKind::ErrorRetriable) {
            ReportMigrationFailed();
            MigrationInProgress = false;
            return;
        }
    } else {
        LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
            "[%s] Range migrated: %lu, count=%u",
            SrcConfig->GetName().c_str(),
            msg->Range.Start,
            msg->Range.Size());
        if (!State.AdvanceMigrationIndex()) {
            FinishMigration(ctx);
            return;
        }

        indexUpdated = true;
    }

    if (indexUpdated) {
        auto migrationRange = State.BuildMigrationRange();
        const auto currentIndex = State.GetLastReportedMigrationIndex();
        const auto step = Config->GetMigrationIndexCachingInterval();

        if (currentIndex + step < migrationRange.Start) {
            State.SetLastReportedMigrationIndex(migrationRange.Start);

            NCloud::Send(
                ctx,
                SrcConfig->GetParentActorId(),
                std::make_unique<TEvVolume::TEvUpdateMigrationState>(
                    migrationRange.Start
                )
            );

            return;
        }
    }

    ScheduleMigrateNextRange(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationActor::ScheduleMigrateNextRange(
    const TActorContext& ctx)
{
    const auto timeout = State.CalculateMigrationTimeout(
        Config->GetMaxMigrationBandwidth(),
        Config->GetExpectedDiskAgentSize()
    );

    const auto deadline = LastRangeMigrationStartTs + timeout;

    if (ctx.Now() >= deadline) {
        MigrateNextRange(ctx);
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%s] Postponing range migration till %s",
        SrcConfig->GetName().c_str(),
        deadline.ToString().Quote().c_str());

    ctx.Schedule(
        deadline,
        new TEvNonreplPartitionPrivate::TEvMigrateNextRange());
}

void TNonreplicatedPartitionMigrationActor::HandleMigrateNextRange(
    const TEvNonreplPartitionPrivate::TEvMigrateNextRange::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    if (!State.IsMigrationStarted()) {
        // migration cancelled
        MigrationInProgress = false;
        return;
    }

    MigrateNextRange(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationActor::HandleRWClientIdChanged(
    const TEvVolume::TEvRWClientIdChanged::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    State.SetRWClientId(std::move(ev->Get()->RWClientId));
}

void TNonreplicatedPartitionMigrationActor::HandleMigrationStateUpdated(
    const TEvVolume::TEvMigrationStateUpdated::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    if (!State.IsMigrationStarted()) {
        // migration cancelled
        MigrationInProgress = false;
        return;
    }

    ScheduleMigrateNextRange(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
