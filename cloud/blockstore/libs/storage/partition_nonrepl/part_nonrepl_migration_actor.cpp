#include "part_nonrepl_migration_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
<<<<<<< HEAD
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/unimplemented.h>

#include <ydb/core/base/appdata.h>
=======
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl.h>
>>>>>>> 024c54e6b8 (NBS-4827 extract TNonreplicatedPartitionMigrationCommonActor (#194))

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TNonreplicatedPartitionMigrationActor::TNonreplicatedPartitionMigrationActor(
        TStorageConfigPtr config,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
        ui64 initialMigrationIndex,
        TString rwClientId,
        TNonreplicatedPartitionConfigPtr srcConfig,
        google::protobuf::RepeatedPtrField<NProto::TDeviceMigration> migrations,
        NRdma::IClientPtr rdmaClient,
        NActors::TActorId statActorId)
    : TNonreplicatedPartitionMigrationCommonActor(
          static_cast<IMigrationOwner*>(this),
          config,
          srcConfig->GetName(),
          srcConfig->GetBlockCount(),
          srcConfig->GetBlockSize(),
          std::move(profileLog),
          std::move(digestGenerator),
          initialMigrationIndex,
          std::move(rwClientId),
          statActorId)
    , Config(std::move(config))
    , SrcConfig(std::move(srcConfig))
    , Migrations(std::move(migrations))
    , RdmaClient(std::move(rdmaClient))
    , TimeoutCalculator(Config, SrcConfig)
{}

void TNonreplicatedPartitionMigrationActor::Bootstrap(
    const NActors::TActorContext& ctx)
{
    TNonreplicatedPartitionMigrationCommonActor::Bootstrap(ctx);

    StartWork(ctx, CreateSrcActor(ctx), CreateDstActor(ctx));
}

void TNonreplicatedPartitionMigrationActor::OnMessage(
    TAutoPtr<NActors::IEventHandle>& ev)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvMigrationStateUpdated, HandleMigrationStateUpdated);
        HFunc(
            TEvDiskRegistry::TEvFinishMigrationResponse,
            HandleFinishMigrationResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION);
            break;
    }
}

TDuration TNonreplicatedPartitionMigrationActor::CalculateMigrationTimeout()
{
    return TimeoutCalculator.CalculateTimeout(GetNextProcessingRange());
}

void TNonreplicatedPartitionMigrationActor::OnMigrationFinished(
    const NActors::TActorContext& ctx)
{
    MigrationFinished = true;
    FinishMigration(ctx, false);
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
        std::make_unique<TEvVolume::TEvUpdateMigrationState>(migrationIndex));

    UpdatingMigrationState = true;
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
    return NCloud::Register(
        ctx,
        CreateNonreplicatedPartition(Config, SrcConfig, SelfId(), RdmaClient));
}

NActors::TActorId TNonreplicatedPartitionMigrationActor::CreateDstActor(
    const NActors::TActorContext& ctx)
{
    Y_ABORT_UNLESS(!Migrations.empty());

    if (Config->GetNonReplicatedVolumeMigrationDisabled()) {
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
            { return m.GetSourceDeviceId() == device.GetDeviceUUID(); });

        if (migration) {
            const auto& target = migration->GetTargetDevice();

            if (device.GetBlocksCount() != target.GetBlocksCount()) {
                LOG_ERROR(
                    ctx,
                    TBlockStoreComponents::PARTITION,
                    "[%s] source (%s) block count (%lu)"
                    " != target (%s) block count (%lu)",
                    SrcConfig->GetName().c_str(),
                    device.GetDeviceUUID().c_str(),
                    device.GetBlocksCount(),
                    target.GetDeviceUUID().c_str(),
                    target.GetBlocksCount());

                ReportBadMigrationConfig();
                return {};
            }

            device.CopyFrom(migration->GetTargetDevice());
        } else {
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
            Config,
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
            ReportMigrationFailed();
            return;
        }
    }

    if (GetErrorKind(error) == EErrorKind::ErrorRetriable) {
        FinishMigration(ctx, true);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
