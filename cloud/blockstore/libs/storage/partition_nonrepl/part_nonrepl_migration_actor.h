#pragma once

#include "public.h"

<<<<<<< HEAD
#include "config.h"
#include "part_nonrepl_events_private.h"
#include "part_nonrepl_migration_state.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/requests_in_progress.h>
#include <cloud/blockstore/libs/storage/partition_common/drain_actor_companion.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/mon.h>
=======
#include <cloud/blockstore/libs/storage/partition_nonrepl/migration_timeout_calculator.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_migration_common_actor.h>
>>>>>>> 024c54e6b8 (NBS-4827 extract TNonreplicatedPartitionMigrationCommonActor (#194))

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

    void Bootstrap(const NActors::TActorContext& ctx) override;

    // IMigrationOwner implementation
    void OnMessage(TAutoPtr<NActors::IEventHandle>& ev) override;
    TDuration CalculateMigrationTimeout() override;
    void OnMigrationProgress(
        const NActors::TActorContext& ctx,
        ui64 migrationIndex) override;
    void OnMigrationFinished(const NActors::TActorContext& ctx) override;

private:
    void FinishMigration(const NActors::TActorContext& ctx, bool isRetry);
    NActors::TActorId CreateSrcActor(const NActors::TActorContext& ctx);
    NActors::TActorId CreateDstActor(const NActors::TActorContext& ctx);

    void HandleMigrationStateUpdated(
        const TEvVolume::TEvMigrationStateUpdated::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleFinishMigrationResponse(
        const TEvDiskRegistry::TEvFinishMigrationResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
