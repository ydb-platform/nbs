#pragma once

#include "public.h"

#include "disk_registry_counters.h"
#include "disk_registry_database.h"
#include "disk_registry_private.h"
#include "disk_registry_state.h"
#include "disk_registry_tx.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/logbroker/iface/public.h>
#include <cloud/blockstore/libs/notify/iface/public.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/monitoring_utils.h>
#include <cloud/blockstore/libs/storage/core/pending_request.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/core/tablet.h>
#include <cloud/blockstore/libs/storage/model/composite_task_waiter.h>

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>

#include <contrib/ydb/core/base/tablet_pipe.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <contrib/ydb/library/actors/core/mon.h>

#include <util/generic/deque.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TDiskRegistryActor final
    : public NActors::TActor<TDiskRegistryActor>
    , public TTabletBase<TDiskRegistryActor>
{
    using TLogbrokerServicePtr = NLogbroker::IServicePtr;
    using TDiskId = TString;
    using TDeviceId = TString;

    enum EState
    {
        STATE_BOOT,
        STATE_INIT,
        STATE_WORK,
        STATE_RESTORE,
        STATE_READ_ONLY,
        STATE_ZOMBIE,
        STATE_MAX,
    };

    struct TStateInfo
    {
        TString Name;
        NActors::IActor::TReceiveFunc Func;
    };

    struct TAdditionalColumn
    {
        std::function<void(IOutputStream& out)> TitleInserter;
        std::function<void(size_t index, IOutputStream& out)> DataInserter;
    };

private:
    const TStorageConfigPtr Config;
    const TDiagnosticsConfigPtr DiagnosticsConfig;

    static const TStateInfo States[];
    EState CurrentState = STATE_BOOT;

    std::unique_ptr<TDiskRegistryState> State;
    NKikimr::TTabletCountersWithTxTypes* Counters = nullptr;
    NMonitoring::TDynamicCountersPtr ComponentGroup;

    // Pending requests
    TDeque<TPendingRequest> PendingRequests;

    THashMap<TDiskId, TVector<TRequestInfoPtr>> PendingDiskDeallocationRequests;

    bool BrokenDisksDestructionInProgress = false;
    bool DisksNotificationInProgress = false;
    bool UsersNotificationInProgress = false;
    bool DiskStatesPublicationInProgress = false;
    bool AutomaticallyReplacedDevicesDeletionInProgress = false;
    THashSet<TString> SecureEraseInProgressPerPool;
    bool StartMigrationInProgress = false;

    TVector<TString> DisksBeingDestroyed;
    TVector<TDiskNotification> DisksBeingNotified;
    TVector<NProto::TUserNotification> UserNotificationsBeingProcessed;

    TInstant BrokenDisksDestructionStartTs;
    TInstant DisksNotificationStartTs;
    TInstant UsersNotificationStartTs;
    TInstant DiskStatesPublicationStartTs;
    TInstant SecureEraseStartTs;
    TInstant StartMigrationStartTs;

    THashMap<NActors::TActorId, TString> ServerToAgentId;

    struct TAgentRegInfo
    {
        ui64 SeqNo = 0;
        bool Connected = false;
    };

    THashMap<TString, TAgentRegInfo> AgentRegInfo;

    // Requests in-progress
    THashSet<NActors::TActorId> Actors;

    TLogbrokerServicePtr LogbrokerService;
    NNotify::IServicePtr NotifyService;

    TCompositeTaskList UpdateVolumeConfigsWaiters;
    TBackoffDelayProvider BackoffDelayProvider{
        TDuration::Seconds(1),
        Config->GetDiskRegistryVolumeConfigUpdatePeriod()};

    ILoggingServicePtr Logging;

    TTransactionTimeTracker TransactionTimeTracker;

public:
    TDiskRegistryActor(
        const NActors::TActorId& owner,
        NKikimr::TTabletStorageInfoPtr storage,
        TStorageConfigPtr config,
        TDiagnosticsConfigPtr diagnosticsConfig,
        TLogbrokerServicePtr logbrokerService,
        NNotify::IServicePtr notifyService,
        ILoggingServicePtr logging);

    ~TDiskRegistryActor();

    static constexpr ui32 LogComponent = TBlockStoreComponents::DISK_REGISTRY;

    static TString GetStateName(ui32 state);

private:
    void BecomeAux(const NActors::TActorContext& ctx, EState state);
    void ReportTabletState(const NActors::TActorContext& ctx);

    void DefaultSignalTabletActive(const NActors::TActorContext& ctx) override;
    void OnActivateExecutor(const NActors::TActorContext& ctx) override;

    bool OnRenderAppHtmlPage(
        NKikimr::NMon::TEvRemoteHttpInfo::TPtr ev,
        const NActors::TActorContext& ctx) override;

    void OnDetach(const NActors::TActorContext& ctx) override;

    void OnTabletDead(
        NKikimr::TEvTablet::TEvTabletDead::TPtr& ev,
        const NActors::TActorContext& ctx) override;

    void KillActors(const NActors::TActorContext& ctx);
    void UnregisterCounters(const NActors::TActorContext& ctx);

    void BeforeDie(const NActors::TActorContext& ctx);

    void RegisterCounters(const NActors::TActorContext& ctx);
    void ScheduleWakeup(const NActors::TActorContext& ctx);
    void UpdateCounters(const NActors::TActorContext& ctx);

    void UpdateActorStats(const NActors::TActorContext& ctx);
    void UpdateActorStatsSampled(const NActors::TActorContext& ctx)
    {
        static constexpr int SampleRate = 128;
        if (Y_UNLIKELY(GetHandledEvents() % SampleRate == 0)) {
            UpdateActorStats(ctx);
        }
    }

    void ScheduleMakeBackup(
        const NActors::TActorContext& ctx,
        TInstant lastBackupTs);
    void ScheduleCleanup(const NActors::TActorContext& ctx);
    void SecureErase(const NActors::TActorContext& ctx);

    void DestroyBrokenDisks(const NActors::TActorContext& ctx);

    void ReallocateDisks(const NActors::TActorContext& ctx);
    void NotifyUsers(const NActors::TActorContext& ctx);

    void SendEnableDevice(
        const NActors::TActorContext& ctx,
        const TString& deviceId);

    void UpdateVolumeConfigs(
        const NActors::TActorContext& ctx,
        const TVector<TString>& diskIds,
        TPrincipalTaskId principalTaskId);
    void UpdateVolumeConfigs(const NActors::TActorContext& ctx);

    void FinishVolumeConfigUpdate(
        const NActors::TActorContext& ctx,
        const TString& diskId);

    void UpdateAgentState(
        const NActors::TActorContext& ctx,
        TString agentId,
        NProto::EAgentState state,
        TInstant timestamp,
        bool force);

    void PublishDiskStates(const NActors::TActorContext& ctx);

    void ScheduleRejectAgent(
        const NActors::TActorContext& ctx,
        TString agentId,
        ui64 seqNo);

    void ScheduleSwitchAgentDisksToReadOnly(
        const NActors::TActorContext& ctx,
        TString agentId);

    void StartMigration(const NActors::TActorContext& ctx);

    bool LoadState(
        TDiskRegistryDatabase& db,
        TDiskRegistryStateSnapshot& args);

    void AddPendingDeallocation(
        const NActors::TActorContext& ctx,
        const TString& diskId,
        TRequestInfoPtr requestInfoPtr);

    void ReplyToPendingDeallocations(
        const NActors::TActorContext& ctx,
        const TString& diskId);

    void ReplyToPendingDeallocations(
        const NActors::TActorContext& ctx,
        TVector<TRequestInfoPtr>& requestInfos,
        NProto::TError error);

    void ProcessAutomaticallyReplacedDevices(const NActors::TActorContext& ctx);

    void OnDiskAcquired(
        TVector<TAgentAcquireDevicesCachedRequest> sentAcquireRequests);
    void OnDiskReleased(
        const TVector<TAgentReleaseDevicesCachedRequest>& sentReleaseRequests);
    void OnDiskDeallocated(const TDiskId& diskId);
    void SendCachedAcquireRequestsToAgent(
        const NActors::TActorContext& ctx,
        const NProto::TAgentConfig& config);

    void RenderHtmlInfo(IOutputStream& out) const;
    void RenderState(IOutputStream& out) const;
    void RenderDisks(IOutputStream& out) const;
    void RenderDisksDetailed(IOutputStream& out) const;
    void RenderDiskList(IOutputStream& out) const;
    void RenderMirroredDiskList(IOutputStream& out) const;
    void RenderMigrationList(IOutputStream& out) const;
    void RenderBrokenDiskList(IOutputStream& out) const;
    void RenderDisksToNotify(IOutputStream& out) const;
    void RenderUserNotifications(IOutputStream& out) const;
    void RenderPlacementGroupList(IOutputStream& out) const;
    void RenderPlacementGroupListDetailed(IOutputStream& out) const;
    void RenderPlacementGroupTable(IOutputStream& out, bool showRecent) const;
    void RenderRacks(IOutputStream& out) const;
    void RenderRacksDetailed(IOutputStream& out) const;
    void RenderPoolRacks(IOutputStream& out, const TString& poolName) const;
    void RenderAgentList(IOutputStream& out) const;
    void RenderAgentListDetailed(TInstant now, IOutputStream& out) const;
    void RenderConfig(IOutputStream& out) const;
    void RenderConfigDetailed(IOutputStream& out) const;
    void RenderDirtyDeviceList(IOutputStream& out) const;
    void RenderDirtyDeviceListDetailed(IOutputStream& out) const;
    void RenderSuspendedDeviceList(IOutputStream& out) const;
    void RenderSuspendedDeviceListDetailed(IOutputStream& out) const;
    void RenderAutomaticallyReplacedDeviceList(IOutputStream& out) const;
    void RenderTransactionsLatency(IOutputStream& out) const;
    template <typename TDevices>
    void RenderDevicesWithDetails(
        IOutputStream& out,
        const TDevices& devices,
        const TString& title,
        const TVector<TAdditionalColumn>& additionalColumns = {}) const;
    void RenderBrokenDeviceList(IOutputStream& out) const;
    void RenderBrokenDeviceListDetailed(IOutputStream& out) const;
    void RenderDeviceHtmlInfo(IOutputStream& out, const TString& id) const;
    void RenderAgentHtmlInfo(IOutputStream& out, const TString& id) const;
    void RenderDiskHtmlInfo(IOutputStream& out, const TString& id) const;

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

    void HandleHttpInfo_GetTransactionsLatency(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void ScheduleDiskRegistryAgentListExpiredParamsCleanup(
        const NActors::TActorContext& ctx);

    void InitializeState(TDiskRegistryStateSnapshot snapshot);

    void ProcessInitialAgentRejectionPhase(const NActors::TActorContext& ctx);

private:
    STFUNC(StateBoot);
    STFUNC(StateInit);
    STFUNC(StateWork);
    STFUNC(StateRestore);
    STFUNC(StateReadOnly);
    STFUNC(StateZombie);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeupReadOnly(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleHttpInfo(
        const NActors::NMon::TEvRemoteHttpInfo::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleHttpInfo_VolumeRealloc(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_ReplaceDevice(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_ChangeDeviseState(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_ChangeAgentState(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_RenderDisks(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_RenderBrokenDeviceList(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_RenderPlacementGroupList(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_RenderRacks(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_RenderAgentList(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_RenderDirtyDeviceList(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_RenderSuspendedDeviceList(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_RenderTransactionsLatency(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_RenderConfig(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_RenderDeviceHtmlInfo(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_RenderAgentHtmlInfo(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleHttpInfo_RenderDiskHtmlInfo(
        const NActors::TActorContext& ctx,
        const TCgiParameters& params,
        TRequestInfoPtr requestInfo);

    void HandleCleanupDisksResponse(
        const TEvDiskRegistryPrivate::TEvCleanupDisksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDestroyBrokenDisksResponse(
        const TEvDiskRegistryPrivate::TEvDestroyBrokenDisksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleNotifyDisksResponse(
        const TEvDiskRegistryPrivate::TEvNotifyDisksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleNotifyUsersResponse(
        const TEvDiskRegistryPrivate::TEvNotifyUsersResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePublishDiskStatesResponse(
        const TEvDiskRegistryPrivate::TEvPublishDiskStatesResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleSecureEraseResponse(
        const TEvDiskRegistryPrivate::TEvSecureEraseResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleStartMigrationResponse(
        const TEvDiskRegistryPrivate::TEvStartMigrationResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleServerConnected(
        const NKikimr::TEvTabletPipe::TEvServerConnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleServerDisconnected(
        const NKikimr::TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAgentConnectionLost(
        const TEvDiskRegistryPrivate::TEvAgentConnectionLost::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleAgentConnectionLostReadOnly(
        const TEvDiskRegistryPrivate::TEvAgentConnectionLost::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleOperationCompleted(
        const TEvDiskRegistryPrivate::TEvOperationCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateVolumeConfigResponse(
        const TEvDiskRegistryPrivate::TEvUpdateVolumeConfigResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRestoreDiskRegistryPartResponse(
        const TEvDiskRegistryPrivate::TEvRestoreDiskRegistryPartResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleBackupDiskRegistryStateResponse(
        const TEvDiskRegistry::TEvBackupDiskRegistryStateResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool HandleRequests(STFUNC_SIG);
    bool RejectRequests(STFUNC_SIG);

    void HandleRestoreDiskRegistryValidationResponse(
        const TEvDiskRegistryPrivate::TEvRestoreDiskRegistryValidationResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDiskRegistryAgentListExpiredParamsCleanup(
        const TEvDiskRegistryPrivate::TEvDiskRegistryAgentListExpiredParamsCleanup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDiskRegistryAgentListExpiredParamsCleanupReadOnly(
        const TEvDiskRegistryPrivate::TEvDiskRegistryAgentListExpiredParamsCleanup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleEnableDeviceResponse(
        const TEvDiskAgent::TEvEnableAgentDeviceResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleSwitchAgentDisksToReadOnlyReshedule(
        const TEvDiskRegistryPrivate::TEvSwitchAgentDisksToReadOnlyRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    BLOCKSTORE_DISK_REGISTRY_REQUESTS(BLOCKSTORE_IMPLEMENT_REQUEST, TEvDiskRegistry)
    BLOCKSTORE_DISK_REGISTRY_REQUESTS_FWD_SERVICE(BLOCKSTORE_IMPLEMENT_REQUEST, TEvService)
    BLOCKSTORE_DISK_REGISTRY_REQUESTS_PRIVATE(BLOCKSTORE_IMPLEMENT_REQUEST, TEvDiskRegistryPrivate)

    using TCounters = TDiskRegistryCounters;
    BLOCKSTORE_DISK_REGISTRY_TRANSACTIONS(BLOCKSTORE_IMPLEMENT_TRANSACTION, TTxDiskRegistry)
};

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DISK_REGISTRY_COUNTER(name)                                 \
    if (Counters) {                                                            \
        auto& counter = Counters->Cumulative()                                 \
            [TDiskRegistryCounters::CUMULATIVE_COUNTER_Request_##name];        \
        counter.Increment(1);                                                  \
    }                                                                          \
// BLOCKSTORE_DISK_REGISTRY_COUNTER

TDiskRegistryStateSnapshot MakeNewLoadState(
    NProto::TDiskRegistryStateBackup&& backup);
bool ToLogicalBlocks(NProto::TDeviceConfig& device, ui32 logicalBlockSize);
}   // namespace NCloud::NBlockStore::NStorage
