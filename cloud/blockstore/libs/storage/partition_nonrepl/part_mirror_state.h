#pragma once

#include "public.h"

#include "config.h"
#include "replica_info.h"

#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/model/replica_actors.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TMirrorPartitionState
{
    enum class EMigrationConfigState
    {
        NotPrepared,
        PreparedForFresh,
        PreparedForWarning,
    };

private:
    const TStorageConfigPtr Config;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    TString RWClientId;

    TMigrations Migrations;
    TVector<TReplicaInfo> ReplicaInfos;
    TReplicaActors ReplicaActors;

    ui32 ReadReplicaIndex = 0;

    EMigrationConfigState MigrationConfigPrepared =
        EMigrationConfigState::NotPrepared;

public:
    TMirrorPartitionState(
        TStorageConfigPtr config,
        TString rwClientId,
        TNonreplicatedPartitionConfigPtr partConfig,
        TMigrations migrations,
        TVector<TDevices> replicas);

public:
    [[nodiscard]] const TVector<TReplicaInfo>& GetReplicaInfos() const
    {
        return ReplicaInfos;
    }

    void AddReplicaActor(const NActors::TActorId& actorId)
    {
        ReplicaActors.AddReplicaActor(actorId);
    }

    [[nodiscard]] auto GetReplicaActors() const
    {
        return ReplicaActors.GetReplicaActors();
    }

    [[nodiscard]] const NActors::TActorId& GetReplicaActor(ui32 index) const
    {
        return ReplicaActors.GetReplicaActor(index);
    }

    [[nodiscard]] auto GetReplicaActorsBypassingProxies() const
    {
        return ReplicaActors.GetReplicaActorsBypassingProxies();
    }

    [[nodiscard]] TVector<NActors::TActorId> GetAllActors() const
    {
        return ReplicaActors.GetAllActors();
    }

    [[nodiscard]] ui32 GetReplicaIndex(NActors::TActorId actorId) const;
    [[nodiscard]] bool IsReplicaActor(NActors::TActorId actorId) const;

    void SetRWClientId(TString rwClientId)
    {
        RWClientId = std::move(rwClientId);
    }

    [[nodiscard]] const TString& GetRWClientId() const
    {
        return RWClientId;
    }

    void SetReadReplicaIndex(ui32 readReplicaIndex)
    {
        if (readReplicaIndex < 0 || readReplicaIndex >= ReplicaActors.Size()) {
            return;
        }
        ReadReplicaIndex = readReplicaIndex;
    }

    [[nodiscard]] bool DevicesReadyForReading(
        ui32 replicaIndex,
        const TBlockRange64 blockRange) const;

    void AddLaggingAgent(NProto::TLaggingAgent laggingAgent);
    void RemoveLaggingAgent(const NProto::TLaggingAgent& laggingAgent);
    [[nodiscard]] bool HasLaggingAgents(ui32 replicaIndex) const;

    void SetLaggingReplicaProxy(
        ui32 replicaIndex,
        const NActors::TActorId& actorId);
    void ResetLaggingReplicaProxy(ui32 replicaIndex);
    [[nodiscard]] bool IsLaggingProxySet(ui32 replicaIndex) const;
    [[nodiscard]] size_t LaggingReplicaCount() const;

    [[nodiscard]] NProto::TError Validate();
    void PrepareMigrationConfig();
    [[nodiscard]] bool PrepareMigrationConfigForWarningDevices();
    [[nodiscard]] bool PrepareMigrationConfigForFreshDevices();

    [[nodiscard]] bool IsMigrationConfigPreparedForFresh() const
    {
        return MigrationConfigPrepared ==
               EMigrationConfigState::PreparedForFresh;
    }

    [[nodiscard]] NProto::TError NextReadReplica(
        const TBlockRange64 readRange,
        ui32& replicaIndex);

    ui32 GetBlockSize() const;

    ui64 GetBlockCount() const;

    TVector<TBlockRange64> SplitRangeByDeviceBorders(
        const TBlockRange64 readRange) const;

    [[nodiscard]] bool IsEncrypted() const;
};

}   // namespace NCloud::NBlockStore::NStorage
