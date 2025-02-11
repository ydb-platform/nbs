#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/requests_in_progress.h>
#include <cloud/blockstore/libs/storage/partition_common/drain_actor_companion.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_events_private.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/smart_migration_actor.h>
#include <cloud/storage/core/libs/common/compressed_bitmap.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/mon.h>

namespace NCloud::NBlockStore::NStorage {

struct TSplitRequest
{
    std::unique_ptr<NActors::IEventBase> Request;
    TCallContextPtr CallContext;
    NActors::TActorId RecipientActorId;
};

class TReplicaLaggingAgentsIOControllerActor final
    : public NActors::TActorBootstrapped<TReplicaLaggingAgentsIOControllerActor>
    , public IPoisonPillHelperOwner
{
private:
    using TBase =
        NActors::TActorBootstrapped<TReplicaLaggingAgentsIOControllerActor>;

    const TStorageConfigPtr Config;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const IProfileLogPtr ProfileLog;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    TString RwClientId;
    const NActors::TActorId PartNonreplActorId;
    const NActors::TActorId StatActorId;
    const NActors::TActorId MirrorPartitionActor;

    enum class EAgentState : ui8
    {
        Unavailable,
        Resyncing
    };
    struct TAgentState
    {
        EAgentState State;
        NProto::TLaggingAgent LaggingAgent;
        NActors::TActorId AgentAvailabilityMonitoring;
        NActors::TActorId SmartMigrationActor;
        std::shared_ptr<TCompressedBitmap> CleanBlocksMap;
    };
    THashMap<TString, TAgentState> AgentState;

    TPoisonPillHelper PoisonPillHelper;

public:
    TReplicaLaggingAgentsIOControllerActor(
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        TNonreplicatedPartitionConfigPtr partConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        TString rwClientId,
        NActors::TActorId partNonreplActorId,
        NActors::TActorId statActorId,
        NActors::TActorId mirrorPartitionActor);

    ~TReplicaLaggingAgentsIOControllerActor() override;

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    [[nodiscard]] bool AgentIsUnavailable(const TString& agentId) const;

    template <typename TMethod>
    TVector<TSplitRequest> SplitRequest(
        const TMethod::TRequest::TPtr& ev,
        const TVector<TDeviceRequest>& deviceRequests);

    [[nodiscard]] TVector<TSplitRequest> DoSplitRequest(
        const TEvService::TEvWriteBlocksRequest::TPtr& ev,
        const TVector<TDeviceRequest>& deviceRequests);
    [[nodiscard]] TVector<TSplitRequest> DoSplitRequest(
        const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
        const TVector<TDeviceRequest>& deviceRequests);
    [[nodiscard]] TVector<TSplitRequest> DoSplitRequest(
        const TEvService::TEvZeroBlocksRequest::TPtr& ev,
        const TVector<TDeviceRequest>& deviceRequests);

    [[nodiscard]] bool ShouldSplitWriteRequest(
        const TVector<TDeviceRequest>& requests) const;
    [[nodiscard]] NActors::TActorId GetRecipientActorId(
        const TString& agentId) const;

    void MarkBlocksAsDirty(
        const TString& unavailableAgentId,
        TBlockRange64 range);

    // IPoisonPillHelperOwner implementation:
    void Die(const NActors::TActorContext& ctx) override;

private:
    STFUNC(StateWork);
    STFUNC(StateZombie);

    void HandleAgentIsUnavailable(
        const TEvNonreplPartitionPrivate::TEvAgentIsUnavailable::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAgentIsBackOnline(
        const TEvNonreplPartitionPrivate::TEvAgentIsBackOnline::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteOrZeroCompleted(
        const TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAddLaggingAgent(
        const TEvNonreplPartitionPrivate::TEvAddLaggingAgentRequest::
            TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRWClientIdChanged(
        const TEvVolume::TEvRWClientIdChanged::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    void WriteRequest(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void ForwardUnhandledEvent(
        TAutoPtr<NActors::IEventHandle>& ev,
        const NActors::TActorContext& ctx);

    BLOCKSTORE_IMPLEMENT_REQUEST(WriteBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(WriteBlocksLocal, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(ZeroBlocks, TEvService);
};

}   // namespace NCloud::NBlockStore::NStorage
