#pragma once

#include "public.h"

#include "config.h"
#include "replica_info.h"

#include <cloud/blockstore/libs/storage/core/public.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/vector.h>

#include <ranges>

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

    struct TReplicaActors
    {
        NActors::TActorId LaggingProxyActorId;
        NActors::TActorId PartActorId;
    };
    TVector<TReplicaActors> ReplicaActors;

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
        ReplicaActors.push_back({actorId, actorId});
    }

    [[nodiscard]] auto GetReplicaActors() const
    {
        return ReplicaActors |
               std::views::transform(&TReplicaActors::LaggingProxyActorId);
    }

    [[nodiscard]] auto GetReplicaActorsVector() const
    {
        const auto replicaActors = GetReplicaActors();
        return TVector<NActors::TActorId>{
            replicaActors.begin(),
            replicaActors.end()};
    }

    [[nodiscard]] const NActors::TActorId& GetReplicaActor(ui32 index) const
    {
        Y_DEBUG_ABORT_UNLESS(index < ReplicaActors.size());
        return ReplicaActors[index].LaggingProxyActorId;
    }

    [[nodiscard]] auto GetReplicaActorsBypassingProxies() const
    {
        return ReplicaActors |
               std::views::transform(&TReplicaActors::PartActorId);
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
        if (readReplicaIndex < 0 || readReplicaIndex >= ReplicaActors.size()) {
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
};

}   // namespace NCloud::NBlockStore::NStorage
