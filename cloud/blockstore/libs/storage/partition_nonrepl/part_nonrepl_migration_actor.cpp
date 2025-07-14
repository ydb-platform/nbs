#include "part_nonrepl_migration_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration PrepareMigrationInterval = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

TNonreplicatedPartitionMigrationActor::TNonreplicatedPartitionMigrationActor(
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
        ui64 initialMigrationIndex,
        TString rwClientId,
        TNonreplicatedPartitionConfigPtr srcConfig,
        google::protobuf::RepeatedPtrField<NProto::TDeviceMigration> migrations,
        NRdma::IClientPtr rdmaClient,
        NActors::TActorId statActorId,
        NActors::TActorId migrationSrcActorId)
    : TNonreplicatedPartitionMigrationCommonActor(
          static_cast<IMigrationOwner*>(this),
          config,
          std::move(diagnosticsConfig),
          srcConfig->GetName(),
          srcConfig->GetBlockCount(),
          srcConfig->GetBlockSize(),
          std::move(profileLog),
          std::move(digestGenerator),
          initialMigrationIndex,
          std::move(rwClientId),
          statActorId,
          config->GetMaxMigrationIoDepth(),
          srcConfig->GetParentActorId())
    , SrcConfig(std::move(srcConfig))
    , Migrations(std::move(migrations))
    , RdmaClient(std::move(rdmaClient))
    , MigrationSrcActorId(migrationSrcActorId)
{}

void TNonreplicatedPartitionMigrationActor::OnBootstrap(
    const NActors::TActorContext& ctx)
{
    auto srcActorId = CreateSrcActor(ctx);
    InitWork(
        ctx,
        MigrationSrcActorId ? MigrationSrcActorId : srcActorId,
        srcActorId,
        CreateDstActor(ctx),
        true,   // takeOwnershipOverActors
        std::make_unique<TMigrationTimeoutCalculator>(
            GetConfig()->GetMaxMigrationBandwidth(),
            GetConfig()->GetExpectedDiskAgentSize(),
            SrcConfig));

    PrepareForMigration(ctx);
}

bool TNonreplicatedPartitionMigrationActor::OnMessage(
    const NActors::TActorContext& ctx,
    TAutoPtr<NActors::IEventHandle>& ev)
{
    Y_UNUSED(ctx);

    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvVolume::TEvPreparePartitionMigrationRequest,
            HandlePreparePartitionMigrationRequest);
        HFunc(
            TEvVolume::TEvPreparePartitionMigrationResponse,
            HandlePreparePartitionMigrationResponse);
        HFunc(TEvVolume::TEvMigrationStateUpdated, HandleMigrationStateUpdated);
        HFunc(
            TEvDiskRegistry::TEvFinishMigrationResponse,
            HandleFinishMigrationResponse);
        HFunc(
            TEvNonreplPartitionPrivate::TEvAgentIsUnavailable,
            HandleAgentIsUnavailable);
        HFunc(
            TEvNonreplPartitionPrivate::TEvAgentIsBackOnline,
            HandleAgentIsBackOnline);
        default:
            return false;
            break;
    }
    return true;
}

void TNonreplicatedPartitionMigrationActor::OnMigrationFinished(
    const NActors::TActorContext& ctx)
{
    MigrationFinished = true;
    FinishMigration(ctx, false);
}

void TNonreplicatedPartitionMigrationActor::OnMigrationError(
    const NActors::TActorContext& ctx)
{
    LOG_ERROR(
        ctx,
        TBlockStoreComponents::PARTITION,
        "[%s] Migration failed",
        SrcConfig->GetName().c_str());
}

void TNonreplicatedPartitionMigrationActor::OnMigrationProgress(
    const NActors::TActorContext& ctx,
    ui64 migrationIndex)
{
    if (UpdatingMigrationState || MigrationFinished) {
        return;
    }

    NCloud::Send(
        ctx,
        SrcConfig->GetParentActorId(),
        std::make_unique<TEvVolume::TEvUpdateMigrationState>(
            migrationIndex,
            GetBlockCountNeedToBeProcessed()));

    UpdatingMigrationState = true;
}

NActors::TActorId
TNonreplicatedPartitionMigrationActor::GetActorToLockAndDrainRange() const
{
    return MigrationSrcActorId;
}

void TNonreplicatedPartitionMigrationActor::FinishMigration(
    const NActors::TActorContext& ctx,
    bool isRetry)
{
    if (UpdatingMigrationState) {
        return;
    }

    auto request =
        std::make_unique<TEvDiskRegistry::TEvFinishMigrationRequest>();
    request->Record.SetDiskId(SrcConfig->GetName());

    for (const auto& migration: Migrations) {
        auto* m = request->Record.AddMigrations();
        m->SetSourceDeviceId(migration.GetSourceDeviceId());
        m->SetTargetDeviceId(migration.GetTargetDevice().GetDeviceUUID());

        LOG_INFO(
            ctx,
            TBlockStoreComponents::PARTITION,
            "[%s] Migration finished: %s -> %s",
            SrcConfig->GetName().c_str(),
            m->GetSourceDeviceId().c_str(),
            m->GetTargetDeviceId().c_str());
    }

    if (isRetry) {
        const TDuration timeout = TDuration::Seconds(5);
        TActivationContext::Schedule(
            timeout,
            new IEventHandle(
                MakeDiskRegistryProxyServiceId(),
                ctx.SelfID,
                request.release()));
    } else {
        NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));
    }
}

NActors::TActorId TNonreplicatedPartitionMigrationActor::CreateSrcActor(
    const NActors::TActorContext& ctx)
{
    auto devices = SrcConfig->GetDevices();
    for (auto& device: devices) {
        if (IsMigrationTarget(device)) {
            device.ClearDeviceUUID();
        }
    }

    return NCloud::Register(
        ctx,
        CreateNonreplicatedPartition(
            GetConfig(),
            GetDiagnosticsConfig(),
            SrcConfig->Fork(std::move(devices)),
            SelfId(),
            RdmaClient));
}

NActors::TActorId TNonreplicatedPartitionMigrationActor::CreateDstActor(
    const NActors::TActorContext& ctx)
{
    Y_ABORT_UNLESS(!Migrations.empty());

    if (GetConfig()->GetNonReplicatedVolumeMigrationDisabled()) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::PARTITION,
            "[%s] migration disabled => aborted",
            SrcConfig->GetName().c_str());
        return {};
    }

    ui64 blockIndex = 0;
    auto devices = SrcConfig->GetDevices();
    for (auto& device: devices) {
        auto* migration = FindIfPtr(
            Migrations,
            [&](const NProto::TDeviceMigration& m)
            {
                return m.GetSourceDeviceId() &&
                       m.GetSourceDeviceId() == device.GetDeviceUUID();
            });

        if (migration) {
            const auto& target = migration->GetTargetDevice();

            if (device.GetBlocksCount() != target.GetBlocksCount()) {
                ReportBadMigrationConfig(
                    TStringBuilder()
                    << "[" << SrcConfig->GetName() << "] "
                    << "source (" << device.GetDeviceUUID() << ") block count ("
                    << device.GetBlocksCount() << ") "
                    << "!= target (" << target.GetDeviceUUID()
                    << ") block count (" << target.GetBlocksCount() << ")");
            }

            device.CopyFrom(migration->GetTargetDevice());
        } else if (!IsMigrationTarget(device)) {
            // Skip this device for migration
            MarkMigratedBlocks(
                TBlockRange64::WithLength(blockIndex, device.GetBlocksCount()));
            device.ClearDeviceUUID();
        }

        blockIndex += device.GetBlocksCount();
    }

    return NCloud::Register(
        ctx,
        CreateNonreplicatedPartition(
            GetConfig(),
            GetDiagnosticsConfig(),
            SrcConfig->Fork(std::move(devices)),
            SelfId(),
            RdmaClient));
}

void TNonreplicatedPartitionMigrationActor::HandleMigrationStateUpdated(
    const TEvVolume::TEvMigrationStateUpdated::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    UpdatingMigrationState = false;
    if (MigrationFinished) {
        FinishMigration(ctx, false);
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
            ReportMigrationFailed(
                TStringBuilder() << "Finish migration failed, diskId: "
                                 << SrcConfig->GetName().Quote());
            return;
        }
    }

    if (GetErrorKind(error) == EErrorKind::ErrorRetriable) {
        FinishMigration(ctx, true);
    }
}

void TNonreplicatedPartitionMigrationActor::PrepareForMigration(
    const NActors::TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvVolume::TEvPreparePartitionMigrationRequest>();

    NCloud::Send(
        ctx,
        SrcConfig->GetParentActorId(),
        std::move(request));
}

void TNonreplicatedPartitionMigrationActor::HandlePreparePartitionMigrationRequest(
    const TEvVolume::TEvPreparePartitionMigrationRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    PrepareForMigration(ctx);
}

void TNonreplicatedPartitionMigrationActor::HandlePreparePartitionMigrationResponse(
    const TEvVolume::TEvPreparePartitionMigrationResponse::TPtr& ev,
    const TActorContext& ctx)
{
    bool isMigrationAllowed = ev->Get()->IsMigrationAllowed;
    if (!isMigrationAllowed) {
        ctx.Schedule(
            PrepareMigrationInterval,
            new TEvVolume::TEvPreparePartitionMigrationRequest());
        return;
    }

    StartWork(ctx);
}

void TNonreplicatedPartitionMigrationActor::HandleAgentIsUnavailable(
    const TEvNonreplPartitionPrivate::TEvAgentIsUnavailable::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    NCloud::Send(
        ctx,
        GetSrcActorId(),
        std::make_unique<TEvNonreplPartitionPrivate::TEvAgentIsUnavailable>(
            msg->LaggingAgent));
    NCloud::Send(
        ctx,
        GetDstActorId(),
        std::make_unique<TEvNonreplPartitionPrivate::TEvAgentIsUnavailable>(
            msg->LaggingAgent));

    const bool migrationTargetIsLagging = AllOf(
        Migrations,
        [&laggingAgentId = msg->LaggingAgent.GetAgentId()](
            const NProto::TDeviceMigration& migration)
        { return migration.GetTargetDevice().GetAgentId() == laggingAgentId; });

    if (migrationTargetIsLagging) {
        SetTargetMigrationIsLagging(true);
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<
                TEvNonreplPartitionPrivate::TEvLaggingMigrationDisabled>(
                msg->LaggingAgent.GetAgentId()));
    }
}

void TNonreplicatedPartitionMigrationActor::HandleAgentIsBackOnline(
    const TEvNonreplPartitionPrivate::TEvAgentIsBackOnline::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto* msg = ev->Get();
    NCloud::Send(
        ctx,
        GetSrcActorId(),
        std::make_unique<TEvNonreplPartitionPrivate::TEvAgentIsBackOnline>(
            msg->AgentId));
    NCloud::Send(
        ctx,
        GetDstActorId(),
        std::make_unique<TEvNonreplPartitionPrivate::TEvAgentIsBackOnline>(
            msg->AgentId));

    const bool migrationTargetIsBackOnline = AllOf(
        Migrations,
        [&laggingAgentId =
             msg->AgentId](const NProto::TDeviceMigration& migration)
        { return migration.GetTargetDevice().GetAgentId() == laggingAgentId; });
    if (migrationTargetIsBackOnline && GetTargetMigrationIsLagging()) {
        SetTargetMigrationIsLagging(false);
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<
                TEvNonreplPartitionPrivate::TEvLaggingMigrationEnabled>(
                msg->AgentId));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
