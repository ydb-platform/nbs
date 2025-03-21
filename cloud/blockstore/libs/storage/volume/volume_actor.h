#pragma once

#include "public.h"

#include "volume.h"
#include "volume_counters.h"
#include "volume_events_private.h"
#include "volume_state.h"
#include "volume_tx.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/storage/api/bootstrapper.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/metrics.h>
#include <cloud/blockstore/libs/storage/core/monitoring_utils.h>
#include <cloud/blockstore/libs/storage/core/pending_request.h>
#include <cloud/blockstore/libs/storage/core/tablet.h>
#include <cloud/blockstore/libs/storage/model/composite_id.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/partition_common/long_running_operation_companion.h>
#include <cloud/blockstore/libs/storage/volume/model/requests_inflight.h>
#include <cloud/blockstore/libs/storage/volume/model/volume_throttler_logger.h>
#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/protos/trace.pb.h>

#include <contrib/ydb/core/base/tablet_pipe.h>
#include <contrib/ydb/core/blockstore/core/blockstore.h>
#include <contrib/ydb/core/mind/local.h>
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <contrib/ydb/library/actors/core/mon.h>

#include <util/generic/deque.h>
#include <util/generic/list.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TVolumeActor final
    : public NActors::TActor<TVolumeActor>
    , public TTabletBase<TVolumeActor>
{
    enum EState
    {
        STATE_BOOT,
        STATE_INIT,
        STATE_WORK,
        STATE_ZOMBIE,
        STATE_MAX,
    };

    struct TStateInfo
    {
        TString Name;
        NActors::IActor::TReceiveFunc Func;
    };

    enum EStatus
    {
        STATUS_OFFLINE,
        STATUS_INACTIVE,
        STATUS_ONLINE,
        STATUS_MOUNTED
    };

    enum class EPartitionsStartedReason
    {
        STARTED_FOR_GC,
        STARTED_FOR_USE,
        NOT_STARTED
    };

    using TEvUpdateVolumeConfigPtr =
        std::unique_ptr<NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::THandle>;

    struct TClientRequest
    {
        const TRequestInfoPtr RequestInfo;
        const TString DiskId;
        const NActors::TActorId PipeServerActorId;

        const NProto::TVolumeClientInfo AddedClientInfo;
        const TString RemovedClientId;
        const bool IsMonRequest = false;

        TClientRequest(
                TRequestInfoPtr requestInfo,
                TString diskId,
                const NActors::TActorId& pipeServerActorId,
                NProto::TVolumeClientInfo addedClientInfo)
            : RequestInfo(std::move(requestInfo))
            , DiskId(std::move(diskId))
            , PipeServerActorId(pipeServerActorId)
            , AddedClientInfo(std::move(addedClientInfo))
        {}

        TClientRequest(
                TRequestInfoPtr requestInfo,
                TString diskId,
                const NActors::TActorId& pipeServerActorId,
                TString removedClientId,
                bool isMonRequest)
            : RequestInfo(std::move(requestInfo))
            , DiskId(std::move(diskId))
            , PipeServerActorId(pipeServerActorId)
            , RemovedClientId(std::move(removedClientId))
            , IsMonRequest(isMonRequest)
        {}

        TString GetClientId() const
        {
            return RemovedClientId
                ? RemovedClientId
                : AddedClientInfo.GetClientId();
        }
    };

    using TClientRequestPtr = std::shared_ptr<TClientRequest>;

    struct TVolumeRequest
    {
        using TCancelRoutine = void(
            const NActors::TActorContext& ctx,
            NActors::TActorId caller,
            ui64 callerCookie,
            TCallContext& callContext,
            NProto::TError error);

        const NActors::TActorId Caller;
        const ui64 CallerCookie;
        const TCallContextPtr CallContext;
        const TCallContextPtr ForkedContext;
        const ui64 ReceiveTime;
        TCancelRoutine* const CancelRoutine;
        const bool IsMultipartitionWriteOrZero;

        TVolumeRequest(
                const NActors::TActorId& caller,
                ui64 callerCookie,
                TCallContextPtr callContext,
                TCallContextPtr forkedContext,
                ui64 receiveTime,
                TCancelRoutine cancelRoutine,
                bool isMultipartitionWriteOrZero)
            : Caller(caller)
            , CallerCookie(callerCookie)
            , CallContext(std::move(callContext))
            , ForkedContext(std::move(forkedContext))
            , ReceiveTime(receiveTime)
            , CancelRoutine(cancelRoutine)
            , IsMultipartitionWriteOrZero(isMultipartitionWriteOrZero)
        {}

        void CancelRequest(
            const NActors::TActorContext& ctx,
            NProto::TError error)
        {
            CancelRoutine(
                ctx,
                Caller,
                CallerCookie,
                *CallContext,
                std::move(error));
        }
    };

    using TVolumeRequestMap = THashMap<ui64, TVolumeRequest>;

public:
    struct TDuplicateRequest
    {
        TCallContextPtr CallContext;
        TEvService::EEvents EventType = TEvService::EvEnd;
        NActors::IEventHandlePtr Event;
        ui64 ReceiveTime = 0;
    };

private:
    TStorageConfigPtr GlobalStorageConfig;
    TStorageConfigPtr Config;
    bool HasStorageConfigPatch = false;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    const IProfileLogPtr ProfileLog;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;
    const ITraceSerializerPtr TraceSerializer;
    const NRdma::IClientPtr RdmaClient;
    NServer::IEndpointEventHandlerPtr EndpointEventHandler;
    const EVolumeStartMode StartMode;
    TVolumeThrottlerLogger ThrottlerLogger;

    std::unique_ptr<TVolumeState> State;
    bool StateLoadFinished = false;
    TInstant ExecutorActivationTimestamp;
    TInstant StateLoadTimestamp;
    TInstant StartInitializationTimestamp;
    TInstant StartCompletionTimestamp;

    static const TStateInfo States[];
    EState CurrentState = STATE_BOOT;

    NKikimr::TTabletCountersWithTxTypes* Counters = nullptr;

    bool UpdateCountersScheduled = false;
    bool UpdateLeakyBucketCountersScheduled = false;
    bool UpdateReadWriteClientInfoScheduled = false;
    bool RemoveExpiredVolumeParamsScheduled = false;
    TInstant LastThrottlerStateWrite;

    // Next version that is being applied
    ui32 NextVolumeConfigVersion = 0;

    // Pending UpdateVolumeConfig operations
    TDeque<TEvUpdateVolumeConfigPtr> PendingVolumeConfigUpdates;
    bool ProcessUpdateVolumeConfigScheduled = false;
    bool UpdateVolumeConfigInProgress = false;

    struct TUnfinishedUpdateVolumeConfig
    {
        NKikimrBlockStore::TUpdateVolumeConfig Record;
        ui32 ConfigVersion = 0;
        TRequestInfoPtr RequestInfo;
        TDevices Devices;
        TMigrations Migrations;
        TVector<TDevices> Replicas;
        TVector<TString> FreshDeviceIds;
        TVector<TString> RemovedLaggingDeviceIds;

        void Clear()
        {
            Record.Clear();
            ConfigVersion = 0;
            RequestInfo = nullptr;
        }
    } UnfinishedUpdateVolumeConfig;

    // Pending requests while partitions are not ready
    TDeque<TPendingRequest> PendingRequests;

    TDeque<TClientRequestPtr> PendingClientRequests;

    ITabletThrottlerPtr Throttler;

    bool ShuttingDown = false;

    TVolumeSelfCountersPtr VolumeSelfCounters;

    struct TAcquireReleaseDiskRequest
    {
        bool IsAcquire;
        TString ClientId;
        NProto::EVolumeAccessMode AccessMode;
        ui64 MountSeqNumber;
        TClientRequestPtr ClientRequest;
        TVector<NProto::TDeviceConfig> DevicesToRelease;

        TAcquireReleaseDiskRequest(
                TString clientId,
                NProto::EVolumeAccessMode accessMode,
                ui64 mountSeqNumber,
                TClientRequestPtr clientRequest)
            : IsAcquire(true)
            , ClientId(std::move(clientId))
            , AccessMode(accessMode)
            , MountSeqNumber(mountSeqNumber)
            , ClientRequest(std::move(clientRequest))
        {
        }

        TAcquireReleaseDiskRequest(
                TString clientId,
                TClientRequestPtr clientRequest,
                TVector<NProto::TDeviceConfig> devicesToRelease)
            : IsAcquire(false)
            , ClientId(std::move(clientId))
            , AccessMode(NProto::EVolumeAccessMode::
                             VOLUME_ACCESS_READ_WRITE)   // doesn't matter
            , MountSeqNumber(0)                          // doesn't matter
            , ClientRequest(std::move(clientRequest))
            , DevicesToRelease(std::move(devicesToRelease))
        {}
    };
    TList<TAcquireReleaseDiskRequest> AcquireReleaseDiskRequests;
    bool AcquireDiskScheduled = false;

    NProto::TError StorageAllocationResult;
    bool DiskAllocationScheduled = false;

    TVolumeRequestMap VolumeRequests;
    TRequestsInFlight WriteAndZeroRequestsInFlight;

    // inflight VolumeRequestId -> duplicate request queue
    // we respond to duplicate requests as soon as our original request is completed
    THashMap<ui64, TDeque<TDuplicateRequest>> DuplicateWriteAndZeroRequests;
    ui32 DuplicateRequestCount = 0;

    std::optional<TCompositeId> VolumeRequestIdGenerator;

    TIntrusiveList<TRequestInfo> ActiveReadHistoryRequests;

    EPartitionsStartedReason PartitionsStartedReason = EPartitionsStartedReason::NOT_STARTED;
    ui32 FailedBoots = 0;

    struct TCheckpointRequestInfo
    {
        TRequestInfoPtr RequestInfo;
        bool IsTraced;
        ui64 TraceTs;

        TCheckpointRequestInfo(
                TRequestInfoPtr requestInfo,
                bool isTraced,
                ui64 traceTs)
            : RequestInfo(std::move(requestInfo))
            , IsTraced(isTraced)
            , TraceTs(traceTs)
        {}

        explicit TCheckpointRequestInfo(const NActors::TActorId& actorId)
            : RequestInfo(CreateRequestInfo(
                  actorId,
                  0,   // cookie
                  MakeIntrusive<TCallContext>()))
            , IsTraced(false)
            , TraceTs(GetCycleCount())
        {}
    };
    TMap<ui64, TCheckpointRequestInfo> CheckpointRequests;

    ui32 MultipartitionWriteAndZeroRequestsInProgress = 0;

    ui64 DiskRegistryTabletId = 0;

    // requests in progress
    THashSet<NActors::TActorId> Actors;
    TIntrusiveList<TRequestInfo> ActiveTransactions;
    TRunningActors LongRunningActors;

    NBlobMetrics::TBlobLoadMetrics PrevMetrics;

    THashSet<NActors::TActorId> StoppedPartitions;

    using TPoisonCallback = std::function<
        void (const NActors::TActorContext&, NProto::TError)
    >;

    TDeque<std::pair<NActors::TActorId, TPoisonCallback>> WaitForPartitions;

    using TDiskRegistryBasedPartitionStoppedCallback =
        std::function<void(const NActors::TActorContext&)>;
    THashMap<ui64, TDiskRegistryBasedPartitionStoppedCallback> OnPartitionStopped;

    TVector<ui64> GCCompletedPartitions;

public:
    TVolumeActor(
        const NActors::TActorId& owner,
        NKikimr::TTabletStorageInfoPtr storage,
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ITraceSerializerPtr traceSerializer,
        NRdma::IClientPtr rdmaClient,
        NServer::IEndpointEventHandlerPtr endpointEventHandler,
        EVolumeStartMode startMode);
    ~TVolumeActor() override;

    static constexpr ui32 LogComponent = TBlockStoreComponents::VOLUME;
    using TCounters = TVolumeCounters;

    static TString GetStateName(ui32 state);

private:
    void Enqueue(STFUNC_SIG) override;
    void DefaultSignalTabletActive(const NActors::TActorContext& ctx) override;

    void BecomeAux(const NActors::TActorContext& ctx, EState state);
    void ReportTabletState(const NActors::TActorContext& ctx);

    void RegisterCounters(const NActors::TActorContext& ctx);
    void ScheduleRegularUpdates(const NActors::TActorContext& ctx);
    void UpdateCounters(const NActors::TActorContext& ctx);
    void UpdateLeakyBucketCounters(const NActors::TActorContext& ctx);

    void UpdateActorStats(const NActors::TActorContext& ctx);

    void UpdateActorStatsSampled(const NActors::TActorContext& ctx)
    {
        static constexpr int SampleRate = 128;
        if (Y_UNLIKELY(GetHandledEvents() % SampleRate == 0)) {
            UpdateActorStats(ctx);
        }
    }

    void SendPartStatsToService(const NActors::TActorContext& ctx);
    void DoSendPartStatsToService(
        const NActors::TActorContext& ctx,
        const TString& diskId);
    void SendSelfStatsToService(const NActors::TActorContext& ctx);

    void OnActivateExecutor(const NActors::TActorContext& ctx) override;

    bool ReassignChannelsEnabled() const override;

    bool OnRenderAppHtmlPage(
        NActors::NMon::TEvRemoteHttpInfo::TPtr ev,
        const NActors::TActorContext& ctx) override;

    void RenderHtmlInfo(IOutputStream& out, TInstant now) const;

    void RenderTabletList(IOutputStream& out) const;

    void RenderClientList(IOutputStream& out, TInstant now) const;

    void RenderConfig(IOutputStream& out) const;
    void RenderStatus(IOutputStream& out) const;
    void RenderMigrationStatus(IOutputStream& out) const;
    void RenderResyncStatus(IOutputStream& out) const;
    void RenderLaggingStatus(IOutputStream& out) const;
    void RenderMountSeqNumber(IOutputStream& out) const;
    void RenderHistory(
        const TVolumeMountHistorySlice& history,
        const TVector<TVolumeMetaHistoryItem>& metaHistory,
        IOutputStream& out) const;
    void RenderCheckpoints(IOutputStream& out) const;
    void RenderLinks(IOutputStream& out) const;
    void RenderTraces(IOutputStream& out) const;
    void RenderStorageConfig(IOutputStream& out) const;
    void RenderRawVolumeConfig(IOutputStream& out) const;
    void RenderCommonButtons(IOutputStream& out) const;

    void OnDetach(const NActors::TActorContext& ctx) override;

    void OnTabletDead(
        NKikimr::TEvTablet::TEvTabletDead::TPtr& ev,
        const NActors::TActorContext& ctx) override;

    void BeforeDie(const NActors::TActorContext& ctx);

    void KillActors(const NActors::TActorContext& ctx);
    void AddTransaction(TRequestInfo& transaction);
    void RemoveTransaction(TRequestInfo& transaction);
    void TerminateTransactions(const NActors::TActorContext& ctx);
    void ReleaseTransactions();

    ui32 GetCurrentConfigVersion() const;

    bool SendBootExternalRequest(
        const NActors::TActorContext& ctx,
        TPartitionInfo& partition);

    void ScheduleRetryStartPartition(
        const NActors::TActorContext& ctx,
        TPartitionInfo& partition);

    void OnServicePipeDisconnect(
        const NActors::TActorContext& ctx,
        const NActors::TActorId& serverId,
        TInstant timestamp);

    void StartPartitionsIfNeeded(const NActors::TActorContext& ctx);
    void StartPartitionsForUse(const NActors::TActorContext& ctx);
    void StartPartitionsForGc(const NActors::TActorContext& ctx);
    void StopPartitions(
        const NActors::TActorContext& ctx,
        TDiskRegistryBasedPartitionStoppedCallback onPartitionStopped);

    void SetupDiskRegistryBasedPartitions(const NActors::TActorContext& ctx);

    bool LaggingDevicesAreAllowed() const;
    void ReportLaggingDevicesToDR(const NActors::TActorContext& ctx);

    void DumpUsageStats(
        const NActors::TActorContext& ctx,
        TVolumeActor::EStatus status);

    TString GetVolumeStatusString(EStatus status) const;
    EStatus GetVolumeStatus() const;

    NRdma::IClientPtr GetRdmaClient() const;
    ui64 GetBlocksCount() const;

    void ProcessNextPendingClientRequest(const NActors::TActorContext& ctx);
    void ProcessNextAcquireReleaseDiskRequest(const NActors::TActorContext& ctx);
    void OnClientListUpdate(const NActors::TActorContext& ctx);

    void UpdateDelayCounter(
        TVolumeThrottlingPolicy::EOpType opType,
        TDuration time);

    void ResetServicePipes(const NActors::TActorContext& ctx);

    void RegisterVolume(const NActors::TActorContext& ctx);
    void DoRegisterVolume(
        const NActors::TActorContext& ctx,
        const TString& diskId);
    void UnregisterVolume(const NActors::TActorContext& ctx);
    void DoUnregisterVolume(
        const NActors::TActorContext& ctx,
        const TString& diskId);
    void SendVolumeConfigUpdated(const NActors::TActorContext& ctx);
    void SendVolumeSelfCounters(const NActors::TActorContext& ctx);

    TDuration GetLoadTime() const
    {
        return StateLoadTimestamp - ExecutorActivationTimestamp;
    }

    TDuration GetStartTime() const
    {
        return StartCompletionTimestamp - StartInitializationTimestamp;
    }

    void OnStarted(const NActors::TActorContext& ctx);

    void ProcessReadHistory(
        const NActors::TActorContext& ctx,
        TRequestInfoPtr requestInfo,
        THistoryLogKey startTs,
        TInstant endTs,
        size_t recordCount,
        bool monRequest);

    bool CheckAllocationResult(
        const NActors::TActorContext& ctx,
        const TDevices& devices,
        const TVector<TDevices>& replicas);

    void CopyCachedStatsToPartCounters(
        const NProto::TCachedPartStats& src,
        TPartitionStatInfo& dst);

    void CopyPartCountersToCachedStats(
        const TPartitionDiskCounters& src,
        NProto::TCachedPartStats& dst);

    void UpdateCachedStats(
        const TPartitionDiskCounters& src,
        TPartitionDiskCounters& dst);

    void SetupIOErrorDetails(const NActors::TActorContext& ctx);

    void ResetThrottlingPolicy();

    void CancelRequests(const NActors::TActorContext& ctx);

    bool CanExecuteWriteRequest() const;

    bool CanChangeThrottlingPolicy() const;

    NKikimr::NMetrics::TResourceMetrics* GetResourceMetrics();

    bool CheckReadWriteBlockRange(const TBlockRange64& range) const;

private:
    STFUNC(StateBoot);
    STFUNC(StateInit);
    STFUNC(StateWork);
    STFUNC(StateZombie);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonTaken(
        const NActors::TEvents::TEvPoisonTaken::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleHttpInfo(
        const NActors::NMon::TEvRemoteHttpInfo::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleHttpInfo_RemoveClient(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_ResetMountSeqNumber(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_CreateCheckpoint(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_DeleteCheckpoint(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_StartPartitions(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_ChangeThrottlingPolicy(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_RenderNonreplPartitionInfo(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_Default(
        const NActors::TActorContext& ctx,
        const TVolumeMountHistorySlice& history,
        const TVector<TVolumeMetaHistoryItem>& metaHistory,
        const TStringBuf tabName,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void SendHttpResponse(
        const NActors::TActorContext& ctx,
        TRequestInfo& requestInfo,
        TString message);

    void SendHttpResponse(
        const NActors::TActorContext& ctx,
        TRequestInfo& requestInfo,
        TString message,
        NMonitoringUtils::EAlertLevel alertLevel);

    void RejectHttpRequest(
        const NActors::TActorContext& ctx,
        TRequestInfo& requestInfo,
        TString message);

    void HandleUpdateThrottlerState(
        const TEvVolumePrivate::TEvUpdateThrottlerState::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateCounters(
        const TEvVolumePrivate::TEvUpdateCounters::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDiskRegistryBasedPartCounters(
        const TEvVolume::TEvDiskRegistryBasedPartitionCounters::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePartCounters(
        const TEvStatsService::TEvVolumePartCounters::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePartStatsSaved(
        const TEvVolumePrivate::TEvPartStatsSaved::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteOrZeroCompleted(
        const TEvVolumePrivate::TEvWriteOrZeroCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReportLaggingDevicesToDR(
        const TEvVolumePrivate::TEvReportLaggingDevicesToDR::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    bool ReplyToOriginalRequest(
        const NActors::TActorContext& ctx,
        NActors::TActorId sender,
        NActors::IEventHandle::TEventFlags flags,
        ui64 volumeRequestId,
        std::unique_ptr<typename TMethod::TResponse> response);

    void ReplyToDuplicateRequests(
        const NActors::TActorContext& ctx,
        ui64 key,
        ui32 resultCode);

    void HandleUpdateReadWriteClientInfo(
        const TEvVolumePrivate::TEvUpdateReadWriteClientInfo::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRemoveExpiredVolumeParams(
        const TEvVolumePrivate::TEvRemoveExpiredVolumeParams::TPtr& ev,
        const NActors::TActorContext& ctx);

    void ProcessCheckpointRequests(const NActors::TActorContext& ctx);

    bool ProcessCheckpointRequest(const NActors::TActorContext& ctx, ui64 requestId);

    void ReplyToCheckpointRequestWithoutSaving(
        const NActors::TActorContext& ctx,
        ECheckpointRequestType requestType,
        const TCheckpointRequestInfo* requestInfo,
        const NProto::TError& error);

    void ExecuteCheckpointRequest(
        const NActors::TActorContext& ctx,
        ui64 requestId);

    void HandleUpdateShadowDiskStateRequest(
        const TEvVolumePrivate::TEvUpdateShadowDiskStateRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetDrTabletInfoResponse(
        const TEvDiskRegistryProxy::TEvGetDrTabletInfoResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleTabletStatus(
        const TEvBootstrapper::TEvStatus::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateVolumeConfig(
        const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool UpdateVolumeConfig(
        const NKikimr::TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAcquireDiskResponse(
        const TEvDiskRegistry::TEvAcquireDiskResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDevicesAcquireFinishedImpl(
        const NProto::TError& error,
        const NActors::TActorContext& ctx);

    void AcquireDisk(
        const NActors::TActorContext& ctx,
        TString clientId,
        NProto::EVolumeAccessMode accessMode,
        ui64 mountSeqNumber);

    void SendAcquireDevicesToAgents(
        TString clientId,
        NProto::EVolumeAccessMode accessMode,
        ui64 mountSeqNumber,
        const NActors::TActorContext& ctx);

    void HandleDevicesAcquireFinished(
        const TEvVolumePrivate::TEvDevicesAcquireFinished::TPtr& ev,
        const NActors::TActorContext& ctx);

    void AcquireDiskIfNeeded(const NActors::TActorContext& ctx);
    void ReleaseReplacedDevices(
        const NActors::TActorContext& ctx,
        const TVector<NProto::TDeviceConfig>& replacedDevices);

    void ScheduleAcquireDiskIfNeeded(const NActors::TActorContext& ctx);

    void HandleAcquireDiskIfNeeded(
        const TEvVolumePrivate::TEvAcquireDiskIfNeeded::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReacquireDisk(
        const TEvVolume::TEvReacquireDisk::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRdmaUnavailable(
        const TEvVolume::TEvRdmaUnavailable::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReleaseDiskResponse(
        const TEvDiskRegistry::TEvReleaseDiskResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDevicesReleasedFinishedImpl(
        const NProto::TError& error,
        const NActors::TActorContext& ctx);

    void ReleaseDisk(
        const NActors::TActorContext& ctx,
        const TString& clientId,
        const TVector<NProto::TDeviceConfig>& devicesToRelease);

    void SendReleaseDevicesToAgents(
        const TString& clientId,
        const NActors::TActorContext& ctx,
        TVector<NProto::TDeviceConfig> devicesToRelease);

    void HandleDevicesReleasedFinished(
        const TEvVolumePrivate::TEvDevicesReleaseFinished::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAllocateDiskResponse(
        const TEvDiskRegistry::TEvAllocateDiskResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAllocateDiskError(
        const NActors::TActorContext& ctx,
        NProto::TError error);

    void HandleAddLaggingDevicesResponse(
        const TEvDiskRegistry::TEvAddLaggingDevicesResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void ScheduleAllocateDiskIfNeeded(const NActors::TActorContext& ctx);

    NProto::TAllocateDiskRequest MakeAllocateDiskRequest() const;

    void AllocateDisk(const NActors::TActorContext& ctx);

    void HandleAllocateDiskIfNeeded(
        const TEvVolumePrivate::TEvAllocateDiskIfNeeded::TPtr& ev,
        const NActors::TActorContext& ctx);

    void ScheduleProcessUpdateVolumeConfig(const NActors::TActorContext& ctx);

    void HandleProcessUpdateVolumeConfig(
        const TEvVolumePrivate::TEvProcessUpdateVolumeConfig::TPtr& ev,
        const NActors::TActorContext& ctx);

    void FinishUpdateVolumeConfig(const NActors::TActorContext& ctx);

    const NKikimrBlockStore::TVolumeConfig& GetNewestConfig() const;

    void HandleRetryStartPartition(
        const TEvVolumePrivate::TEvRetryStartPartition::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleBootExternalResponse(
        const NCloud::NStorage::TEvHiveProxy::TEvBootExternalResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWaitReadyResponse(
        const NPartition::TEvPartition::TEvWaitReadyResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleBackpressureReport(
        const NPartition::TEvPartition::TEvBackpressureReport::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGarbageCollectorCompleted(
        const NPartition::TEvPartition::TEvGarbageCollectorCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleServerConnected(
        const NKikimr::TEvTabletPipe::TEvServerConnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleServerDisconnected(
        const NKikimr::TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleServerDestroyed(
        const NKikimr::TEvTabletPipe::TEvServerDestroyed::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleTabletMetrics(
        NKikimr::TEvLocal::TEvTabletMetrics::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateMigrationState(
        const TEvVolume::TEvUpdateMigrationState::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePreparePartitionMigration(
        const TEvVolume::TEvPreparePartitionMigrationRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateResyncState(
        const TEvVolume::TEvUpdateResyncState::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleResyncFinished(
        const TEvVolume::TEvResyncFinished::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    void FillResponse(
        typename TMethod::TResponse& response,
        TCallContext& callContext,
        ui64 startTime);

    template <typename TMethod>
    typename TMethod::TRequest::TPtr WrapRequest(
        const typename TMethod::TRequest::TPtr& ev,
        NActors::TActorId newRecipient,
        ui64 volumeRequestId,
        ui64 traceTime,
        bool forkTraces,
        bool isMultipartition);

    template <typename TMethod>
    void SendRequestToPartition(
        const NActors::TActorContext& ctx,
        const typename TMethod::TRequest::TPtr& ev,
        ui64 volumeRequestId,
        ui32 partitionId,
        ui64 traceTs);

    template <typename TMethod>
    NProto::TError ProcessAndValidateReadFromCheckpoint(
        typename TMethod::TRequest::ProtoRecordType& record) const;

    template <typename TMethod>
    bool SendRequestToPartitionWithUsedBlockTracking(
        const NActors::TActorContext& ctx,
        const typename TMethod::TRequest::TPtr& ev,
        const NActors::TActorId& partActorId,
        const ui64 volumeRequestId);

    template <typename TMethod>
    bool HandleMultipartitionVolumeRequest(
        const NActors::TActorContext& ctx,
        const typename TMethod::TRequest::TPtr& ev,
        ui64 volumeRequestId,
        bool isTraced,
        ui64 traceTs);

    template <typename TMethod>
    void HandleCheckpointRequest(
        const NActors::TActorContext& ctx,
        const typename TMethod::TRequest::TPtr& ev,
        bool isTraced,
        ui64 traceTs);

    template <typename TMethod>
    void ForwardRequest(
        const NActors::TActorContext& ctx,
        const typename TMethod::TRequest::TPtr& ev);

    template <typename TMethod>
    NProto::TError Throttle(
        const NActors::TActorContext& ctx,
        const typename TMethod::TRequest::TPtr& ev,
        bool throttlingDisabled);

    template <typename TMethod>
    void ForwardResponse(
        const NActors::TActorContext& ctx,
        const typename TMethod::TResponse::TPtr& ev);

    bool HandleRequests(STFUNC_SIG);
    bool RejectRequests(STFUNC_SIG);

    void HandleDescribeBlocksResponse(
        const TEvVolume::TEvDescribeBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetUsedBlocksResponse(
        const TEvVolume::TEvGetUsedBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetPartitionInfoResponse(
        const TEvVolume::TEvGetPartitionInfoResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleCompactRangeResponse(
        const TEvVolume::TEvCompactRangeResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetCompactionStatusResponse(
        const TEvVolume::TEvGetCompactionStatusResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRebuildMetadataResponse(
        const TEvVolume::TEvRebuildMetadataResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetRebuildMetadataStatusResponse(
        const TEvVolume::TEvGetRebuildMetadataStatusResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleScanDiskResponse(
        const TEvVolume::TEvScanDiskResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetScanDiskStatusResponse(
        const TEvVolume::TEvGetScanDiskStatusResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadBlocksResponse(
        const TEvService::TEvReadBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteBlocksResponse(
        const TEvService::TEvWriteBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleZeroBlocksResponse(
        const TEvService::TEvZeroBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleCreateCheckpointResponse(
        const TEvService::TEvCreateCheckpointResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDeleteCheckpointResponse(
        const TEvService::TEvDeleteCheckpointResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDeleteCheckpointDataResponse(
        const TEvVolume::TEvDeleteCheckpointDataResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetCheckpointStatusResponse(
        const TEvService::TEvGetCheckpointStatusResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteBlocksLocalResponse(
        const TEvService::TEvWriteBlocksLocalResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetChangedBlocksResponse(
        const TEvService::TEvGetChangedBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadBlocksLocalResponse(
        const TEvService::TEvReadBlocksLocalResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleLaggingAgentMigrationFinished(
        const TEvVolumePrivate::TEvLaggingAgentMigrationFinished::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateLaggingAgentMigrationState(
        const TEvVolumePrivate::TEvUpdateLaggingAgentMigrationState::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleCheckRangeResponse(
        const TEvVolume::TEvCheckRangeResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void CreateCheckpointLightRequest(
        const NActors::TActorContext& ctx,
        ui64 requestId,
        const TString& checkpointId);

    void DeleteCheckpointLightRequest(
        const NActors::TActorContext& ctx,
        ui64 requestId,
        const TString& checkpointId);

    void GetChangedBlocksForLightCheckpoints(
        const TEvService::TGetChangedBlocksMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void ReplyErrorOnNormalGetChangedBlocksRequestForDiskRegistryBasedDisk(
        const TEvService::TGetChangedBlocksMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleLongRunningBlobOperation(
        const TEvPartitionCommonPrivate::TEvLongRunningOperation::TPtr& ev,
        const NActors::TActorContext& ctx);

    NActors::TActorId WrapNonreplActorIfNeeded(
        const NActors::TActorContext& ctx,
        NActors::TActorId nonreplicatedActorId,
        std::shared_ptr<TNonreplicatedPartitionConfig> srcConfig);

    void RestartDiskRegistryBasedPartition(
        const NActors::TActorContext& ctx,
        TDiskRegistryBasedPartitionStoppedCallback onPartitionStopped);
    void StartPartitionsImpl(const NActors::TActorContext& ctx);

    BLOCKSTORE_VOLUME_REQUESTS(BLOCKSTORE_IMPLEMENT_REQUEST, TEvVolume)
    BLOCKSTORE_VOLUME_REQUESTS_PRIVATE(BLOCKSTORE_IMPLEMENT_REQUEST, TEvVolumePrivate)
    BLOCKSTORE_VOLUME_REQUESTS_FWD_SERVICE(BLOCKSTORE_IMPLEMENT_REQUEST, TEvService)

    BLOCKSTORE_VOLUME_TRANSACTIONS(BLOCKSTORE_IMPLEMENT_TRANSACTION, TTxVolume)
};

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_VOLUME_COUNTER(name)                                        \
    if (Counters) {                                                            \
        auto& counter = Counters->Cumulative()                                 \
            [TVolumeCounters::CUMULATIVE_COUNTER_Request_##name];              \
        counter.Increment(1);                                                  \
    }                                                                          \
// BLOCKSTORE_VOLUME_COUNTER

TString DescribeAllocation(const NProto::TAllocateDiskResponse& record);

}   // namespace NCloud::NBlockStore::NStorage
