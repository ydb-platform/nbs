#pragma once

#include "public.h"

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
    TDuration CalculateMigrationTimeout() override;
    void FinishMigration(const NActors::TActorContext& ctx, bool isRetry)
        override;

private:
    NActors::TActorId CreateSrcActor(const NActors::TActorContext& ctx);
    NActors::TActorId CreateDestActor(const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
