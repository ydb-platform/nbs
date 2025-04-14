#pragma once

#include "public.h"

#include "config.h"
#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <cloud/storage/core/libs/actors/poison_pill_helper.h>
#include <cloud/storage/core/libs/common/compressed_bitmap.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// The main purpose of this class is to handle lagging agents for a single
// replica of a mirrored disk. When an agent becomes unavailable, it creates
// TAgentAvailabilityMonitoringActor and maintains the dirty blocks bitmap by
// proxying all write requests from the mirror partition. Once the agent becomes
// responsive again, it starts migration from the mirror to the non-replicated
// partition using the dirty blocks bitmap.
class TLaggingAgentsReplicaProxyActor final
    : public NActors::TActorBootstrapped<TLaggingAgentsReplicaProxyActor>
    , public IPoisonPillHelperOwner
{
    enum class ERequestKind
    {
        Read,
        Write,
    };

private:
    using TBase = NActors::TActorBootstrapped<TLaggingAgentsReplicaProxyActor>;

    const TStorageConfigPtr Config;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    const TNonreplicatedPartitionConfigPtr PartConfig;
    const google::protobuf::RepeatedPtrField<NProto::TDeviceMigration>
        Migrations;
    const IProfileLogPtr ProfileLog;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    TString RwClientId;
    const NActors::TActorId NonreplPartitionActorId;
    const NActors::TActorId MirrorPartitionActorId;

    enum class EAgentState : ui8
    {
        Unavailable,
        WaitingForDrain,
        Resyncing
    };
    struct TAgentState
    {
        EAgentState State;
        NProto::TLaggingAgent LaggingAgent;
        NActors::TActorId AvailabilityMonitoringActorId;
        NActors::TActorId MigrationActorId;
        std::unique_ptr<TCompressedBitmap> CleanBlocksMap;
        bool MigrationDisabled = false;
        bool DrainFinished = false;
    };
    THashMap<TString, TAgentState> AgentState;

    struct TBlockRangeData {
        TString LaggingAgentId;
        bool IsTargetMigration = false;
    };
    TMap<ui64, TBlockRangeData> BlockRangeDataByBlockRangeEnd;

    ui64 DrainRequestCounter = 0;
    // To determine which agent has drained in-flight writes by cookie in the
    // response.
    THashMap<ui64, TString> CurrentDrainingAgents;

    TPoisonPillHelper PoisonPillHelper;

public:
    struct TSplitRequest
    {
        std::unique_ptr<NActors::IEventBase> Request;
        TCallContextPtr CallContext;
        NActors::TActorId RecipientActorId;
        TString DeviceUUID;
    };

    TLaggingAgentsReplicaProxyActor(
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        TNonreplicatedPartitionConfigPtr partConfig,
        google::protobuf::RepeatedPtrField<NProto::TDeviceMigration> migrations,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        TString rwClientId,
        NActors::TActorId nonreplPartitionActorId,
        NActors::TActorId mirrorPartitionActorId);

    ~TLaggingAgentsReplicaProxyActor() override;

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
        const TVector<TDeviceRequest>& deviceRequests) const;
    [[nodiscard]] NActors::TActorId GetRecipientActorId(
        const TBlockRange64& requestBlockRange,
        ERequestKind kind) const;

    [[nodiscard]] const TBlockRangeData& GetBlockRangeData(
        const TBlockRange64& requestBlockRange) const;

    void RecalculateBlockRangeDataByBlockRangeEnd();

    void MarkBlocksAsDirty(
        const NActors::TActorContext& ctx,
        const TString& unavailableAgentId,
        TBlockRange64 range);

    void StartLaggingResync(
        const NActors::TActorContext& ctx,
        const TString& agentId,
        TAgentState* state);

    ui64 TakeDrainRequestId(const TString& agentId);

    void DestroyChildActor(
        const NActors::TActorContext& ctx,
        NActors::TActorId* actorId);

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

    void HandleLaggingAgentMigrationFinished(
        const TEvVolumePrivate::TEvLaggingAgentMigrationFinished::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWaitForInFlightWritesResponse(
        const NPartition::TEvPartition::TEvWaitForInFlightWritesResponse::TPtr&
            ev,
        const NActors::TActorContext& ctx);

    void HandleWriteOrZeroCompleted(
        const TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAddLaggingAgent(
        const TEvNonreplPartitionPrivate::TEvAddLaggingAgentRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleLaggingMigrationDisabled(
        const TEvNonreplPartitionPrivate::TEvLaggingMigrationDisabled::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleLaggingMigrationEnabled(
        const TEvNonreplPartitionPrivate::TEvLaggingMigrationEnabled::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRWClientIdChanged(
        const TEvVolume::TEvRWClientIdChanged::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    void WriteBlocks(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    void ReadBlocks(
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void ForwardUnhandledEvent(
        TAutoPtr<NActors::IEventHandle>& ev,
        const NActors::TActorContext& ctx);

    BLOCKSTORE_IMPLEMENT_REQUEST(WriteBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(WriteBlocksLocal, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(ZeroBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(ReadBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(ReadBlocksLocal, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(
        WaitForInFlightWrites,
        NPartition::TEvPartition);
};

}   // namespace NCloud::NBlockStore::NStorage
