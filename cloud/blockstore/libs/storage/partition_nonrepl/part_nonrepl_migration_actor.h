#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/partition_nonrepl/migration_timeout_calculator.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_migration_common_actor.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TNonreplicatedPartitionMigrationActor final
    : public TNonreplicatedPartitionMigrationCommonActor
    , public IMigrationOwner
{
private:
    const TStorageConfigPtr Config;
    TNonreplicatedPartitionConfigPtr SrcConfig;
    google::protobuf::RepeatedPtrField<NProto::TDeviceMigration> Migrations;
    NRdma::IClientPtr RdmaClient;
    TMigrationTimeoutCalculator TimeoutCalculator;

    bool UpdatingMigrationState = false;
    bool MigrationFinished = false;

public:
    TNonreplicatedPartitionMigrationActor(
        TStorageConfigPtr config,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr digestGenerator,
        ui64 initialMigrationIndex,
        TString rwClientId,
        TNonreplicatedPartitionConfigPtr srcConfig,
        google::protobuf::RepeatedPtrField<NProto::TDeviceMigration> migrations,
        NRdma::IClientPtr rdmaClient,
        NActors::TActorId statActorId);

    // IMigrationOwner implementation
    void OnBootstrap(const NActors::TActorContext& ctx) override;
    bool OnMessage(const NActors::TActorContext& ctx,
        TAutoPtr<NActors::IEventHandle>& ev) override;
    TDuration CalculateMigrationTimeout(TBlockRange64 range) override;
    void OnMigrationProgress(
        const NActors::TActorContext& ctx,
        ui64 migrationIndex) override;
    void OnMigrationFinished(const NActors::TActorContext& ctx) override;
    void OnMigrationError(const NActors::TActorContext& ctx) override;

private:
    void PrepareForMigration(const NActors::TActorContext& ctx);
    void FinishMigration(const NActors::TActorContext& ctx, bool isRetry);
    NActors::TActorId CreateSrcActor(const NActors::TActorContext& ctx);
    NActors::TActorId CreateDstActor(const NActors::TActorContext& ctx);
    void DoRegisterTrafficSource(const NActors::TActorContext& ctx);

    void HandleMigrationStateUpdated(
        const TEvVolume::TEvMigrationStateUpdated::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleFinishMigrationResponse(
        const TEvDiskRegistry::TEvFinishMigrationResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePreparePartitionMigrationRequest(
        const TEvVolume::TEvPreparePartitionMigrationRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePreparePartitionMigrationResponse(
        const TEvVolume::TEvPreparePartitionMigrationResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
