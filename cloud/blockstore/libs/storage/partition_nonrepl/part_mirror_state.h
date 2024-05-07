#pragma once

#include "public.h"

#include "config.h"
#include "replica_info.h"

#include <cloud/blockstore/libs/storage/core/public.h>

#include <library/cpp/actors/core/actorid.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TMirrorPartitionState
{
private:
    const TStorageConfigPtr Config;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    TString RWClientId;

    TMigrations Migrations;
    TVector<TReplicaInfo> ReplicaInfos;
    TVector<NActors::TActorId> ReplicaActors;

    ui32 ReadReplicaIndex = 0;

    bool MigrationConfigPrepared = false;

public:
    TMirrorPartitionState(
        TStorageConfigPtr config,
        TString rwClientId,
        TNonreplicatedPartitionConfigPtr partConfig,
        TMigrations migrations,
        TVector<TDevices> replicas);

public:
    const TVector<TReplicaInfo>& GetReplicaInfos() const
    {
        return ReplicaInfos;
    }

    void AddReplicaActor(const NActors::TActorId& actorId)
    {
        ReplicaActors.push_back(actorId);
    }

    const TVector<NActors::TActorId>& GetReplicaActors() const
    {
        return ReplicaActors;
    }

    void SetRWClientId(TString rwClientId)
    {
        RWClientId = std::move(rwClientId);
    }

    const TString& GetRWClientId() const
    {
        return RWClientId;
    }

    [[nodiscard]] NProto::TError Validate();
    void PrepareMigrationConfig();

    [[nodiscard]] NProto::TError NextReadReplica(
        const TBlockRange64 readRange,
        NActors::TActorId* actorId);

    ui32 GetBlockSize() const;

    ui64 GetBlockCount() const;
};

}   // namespace NCloud::NBlockStore::NStorage
