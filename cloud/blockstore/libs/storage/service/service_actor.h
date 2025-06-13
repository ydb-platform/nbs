#pragma once

#include "public.h"

#include "service_counters.h"
#include "service_events_private.h"
#include "service_state.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/diagnostics/stats_aggregator.h>
#include <cloud/blockstore/libs/discovery/discovery.h>
#include <cloud/blockstore/libs/encryption/public.h>
#include <cloud/blockstore/libs/endpoints/public.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/manually_preempted_volumes.h>
#include <cloud/blockstore/libs/storage/core/metrics.h>
#include <cloud/blockstore/libs/storage/core/monitoring_utils.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/service/model/ping_metrics.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <contrib/ydb/library/actors/core/mon.h>

#include <util/stream/output.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TServiceActor final
    : public NActors::TActorBootstrapped<TServiceActor>
{
private:
    const TStorageConfigPtr Config;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    const IProfileLogPtr ProfileLog;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const NDiscovery::IDiscoveryServicePtr DiscoveryService;
    const ITraceSerializerPtr TraceSerializer;
    const NServer::IEndpointEventHandlerPtr EndpointEventHandler;
    const NRdma::IClientPtr RdmaClient;
    const IVolumeStatsPtr VolumeStats;
    const IRootKmsKeyProviderPtr RootKmsKeyProvider;
    const bool TemporaryServer;

    TSharedServiceCountersPtr SharedCounters;

    TServiceState State;
    std::shared_ptr<NKikimr::TTabletCountersBase> Counters;

    TPingMetrics ReadWriteCounters;

    bool IsSyncManuallyPreemptedVolumesRunning = false;
    bool HasPendingManuallyPreemptedVolumesUpdate = false;
    bool IsVolumeLivenessCheckRunning = false;

    NMonitoring::TDynamicCounters::TCounterPtr SelfPingMaxUsCounter;
    ui64 SelfPingMaxUs = 0;

public:
    TServiceActor(
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        NDiscovery::IDiscoveryServicePtr discoveryService,
        ITraceSerializerPtr traceSerializer,
        NServer::IEndpointEventHandlerPtr endpointEventHandler,
        NRdma::IClientPtr rdmaClient,
        IVolumeStatsPtr volumeStats,
        TManuallyPreemptedVolumesPtr preemptedVolumes,
        IRootKmsKeyProviderPtr rootKmsKeyProvider,
        bool temporaryServer);
    ~TServiceActor() override;

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void RegisterPages(const NActors::TActorContext& ctx);
    void RegisterCounters(const NActors::TActorContext& ctx);

    void ScheduleCountersUpdate(const NActors::TActorContext& ctx);

    void UpdateCounters(const NActors::TActorContext& ctx);

    void UpdateActorStats(const NActors::TActorContext& ctx);
    void UpdateActorStatsSampled(const NActors::TActorContext& ctx)
    {
        static constexpr int SampleRate = 128;
        if (Y_UNLIKELY(GetHandledEvents() % SampleRate == 0)) {
            UpdateActorStats(ctx);
        }
    }

    void ScheduleStatsUpload(const NActors::TActorContext& ctx);

    void RenderHtmlInfo(IOutputStream& out) const;
    void RenderDownDisks(IOutputStream& out) const;
    void RenderVolumeList(IOutputStream& out) const;

    template <typename TMethod>
    void ForwardRequest(
        const NActors::TActorContext& ctx,
        const typename TMethod::TRequest::TPtr& ev);

    template <typename TMethod>
    void ForwardRequestToDiskRegistry(
        const NActors::TActorContext& ctx,
        const typename TMethod::TRequest::TPtr& ev);

    template <typename TResponse, typename TRequestPtr>
    void HandleNotSupportedRequest(
        const TRequestPtr& ev,
        const NActors::TActorContext& ctx);

    void PushYdbStats(const NActors::TActorContext& ctx);

    void ScheduleLivenessCheck(
        const NActors::TActorContext& ctx,
        TDuration timeout);
    void RunManuallyPreemptedVolumesSync(const NActors::TActorContext& ctx);
    void RunVolumesLivenessCheck(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo);

    void ScheduleSelfPing(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleSelfPing(
        const TEvServicePrivate::TEvSelfPing::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleHttpInfo(
        const NActors::NMon::TEvHttpInfo::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleHttpInfo_Search(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_Clients(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_Unmount(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_VolumeBinding(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void SendHttpResponse(
        const NActors::TActorContext& ctx,
        TRequestInfo& requestInfo,
        TStringStream message);

    void RejectHttpRequest(
        const NActors::TActorContext& ctx,
        TRequestInfo& requestInfo,
        TString message);

    void HandleVolumeMountStateChanged(
        const TEvService::TEvVolumeMountStateChanged::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRegisterVolume(
        const TEvService::TEvRegisterVolume::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUnregisterVolume(
        const TEvService::TEvUnregisterVolume::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleVolumeConfigUpdated(
        const TEvService::TEvVolumeConfigUpdated::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleSessionActorDied(
        const TEvServicePrivate::TEvSessionActorDied::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateManuallyPreemptedVolume(
        const TEvServicePrivate::TEvUpdateManuallyPreemptedVolume::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleSyncManuallyPreemptedVolumesComplete(
        const TEvServicePrivate::TEvSyncManuallyPreemptedVolumesComplete::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRunVolumesLivenessCheckResponse(
        const TEvService::TEvRunVolumesLivenessCheckResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool HandleRequests(STFUNC_SIG);

    BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_IMPLEMENT_REQUEST, TEvService)
    BLOCKSTORE_SERVICE_REQUESTS(BLOCKSTORE_IMPLEMENT_REQUEST, TEvService)

////////////////////////////////////////////////////////////////////////////////

    TResultOrError<NActors::IActorPtr> CreateDescribeVolumeActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateGetPartitionInfoActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateDescribeBlocksActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateCheckBlobActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateResetTabletActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateDrainNodeActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateCompactRangeActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateGetCompactionStatusActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateModifyTagsActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateReallocateDiskActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateDiskRegistryChangeStateActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateReplaceDeviceActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateRebaseVolumeActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateWritableStateActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateRebindVolumesActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateConfigureVolumeBalancerActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateDeleteCheckpointDataActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateKillTabletActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateBackupDiskRegistryStateActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateSetUserIdActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateMarkReplacementDeviceActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateUpdateDiskBlockSizeActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateUpdateDiskReplicaCountActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateUpdatePlacementGroupSettingsActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateUpdateUsedBlocksActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateRebuildMetadataActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateRebuildMetadataStatusActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateReassignDiskRegistryActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateSuspendDeviceActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateRestoreDiskRegistryStateActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateGetDiskRegistryTabletInfo(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateScanDiskActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateScanDiskStatusActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateMigrationDiskRegistryDeviceActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateCreateDiskFromDevicesActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateChangeDiskDeviceActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateSetupChannelsActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateUpdateDiskRegistryAgentListParamsActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateFinishFillDiskActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateUpdateVolumeParamsActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateGetDependentDisksActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateChangeStorageConfigActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateGetNameserverNodesActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateCmsActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateCheckRangeActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateFlushProfileLogActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateGetDiskAgentNodeIdActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateWaitDependentDisksToSwitchNodeActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreatePartiallySuspendDiskAgentActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateGetStorageConfigActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateBackupPathDescriptionsActor(
        TRequestInfoPtr requestInfo,
        TString input);

    TResultOrError<NActors::IActorPtr> CreateBackupTabletBootInfosActor(
        TRequestInfoPtr requestInfo,
        TString input);
};

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateReadBlocksRemoteActor(
    TEvService::TEvReadBlocksLocalRequest::TPtr request,
    ui64 blockSize,
    NActors::TActorId volumeClient);

NActors::IActorPtr CreateWriteBlocksRemoteActor(
    TEvService::TEvWriteBlocksLocalRequest::TPtr request,
    ui64 blockSize,
    NActors::TActorId volumeClient);

NActors::IActorPtr CreateVolumeSessionActor(
    TVolumeInfoPtr volumeInfo,
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    ITraceSerializerPtr traceSerializer,
    NServer::IEndpointEventHandlerPtr endpointEventHandler,
    NRdma::IClientPtr rdmaClient,
    std::shared_ptr<NKikimr::TTabletCountersBase> counters,
    TSharedServiceCountersPtr sharedCounters,
    bool temporaryServer);

void RegisterAlterVolumeActor(
    const NActors::TActorId& sender,
    ui64 cookie,
    TStorageConfigPtr config,
    const NPrivateProto::TSetupChannelsRequest& request,
    const NActors::TActorContext& ctx);
}   // namespace NCloud::NBlockStore::NStorage
