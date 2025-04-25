#pragma once

#include "public.h"

#include "disk_agent_counters.h"
#include "disk_agent_private.h"
#include "disk_agent_state.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/rdma/iface/config.h>
#include <cloud/blockstore/libs/spdk/iface/env.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/pending_request.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/bandwidth_calculator.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/recent_blocks_tracker.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <contrib/ydb/library/actors/core/mon.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TDiskAgentActor final
    : public NActors::TActorBootstrapped<TDiskAgentActor>
{
    struct TPostponedRequest
    {
        ui64 VolumeRequestId = 0;
        TBlockRange64 Range;
        NActors::IEventHandlePtr Event;
    };

    enum class ERegistrationState
    {
        NotStarted,
        InProgress,
        Registered,
    };

private:
    const TStorageConfigPtr Config;
    const TDiskAgentConfigPtr AgentConfig;
    const NRdma::TRdmaConfigPtr RdmaConfig;
    const NSpdk::ISpdkEnvPtr Spdk;
    const ICachingAllocatorPtr Allocator;
    const IStorageProviderPtr StorageProvider;
    const IProfileLogPtr ProfileLog;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;

    ILoggingServicePtr Logging;
    NRdma::IServerPtr RdmaServer;

    NNvme::INvmeManagerPtr NvmeManager;

    std::unique_ptr<TDiskAgentState> State;
    std::unique_ptr<NKikimr::TTabletCountersBase> Counters;

    // Pending WaitReady requests
    TDeque<TPendingRequest> PendingRequests;

    TBandwidthCalculator BandwidthCalculator {*AgentConfig};

    ERegistrationState RegistrationState = ERegistrationState::NotStarted;

    NActors::TActorId StatsActor;
    TOldRequestCounters OldRequestCounters;

    THashMap<TString, TDeque<TRequestInfoPtr>> SecureErasePendingRequests;
    THashSet<TString> SecureEraseDevicesNames;

    const bool RejectLateRequestsAtDiskAgentEnabled =
        Config->GetRejectLateRequestsAtDiskAgentEnabled();
    THashMap<TString, TRecentBlocksTracker> RecentBlocksTrackers;
    TList<TPostponedRequest> PostponedRequests;

    NActors::TActorId SessionCacheActor;

    TRequestInfoPtr PartiallySuspendAgentRequestInfo;

    TVector<NActors::TActorId> IOParserActors;
    ui32 ParserActorIdx = 0;

    NActors::TActorId HealthCheckActor;

public:
    TDiskAgentActor(
        TStorageConfigPtr config,
        TDiskAgentConfigPtr agentConfig,
        NRdma::TRdmaConfigPtr rdmaConfig,
        NSpdk::ISpdkEnvPtr spdk,
        ICachingAllocatorPtr allocator,
        IStorageProviderPtr storageProvider,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ILoggingServicePtr logging,
        NRdma::IServerPtr rdmaServer,
        NNvme::INvmeManagerPtr nvmeManager);

    ~TDiskAgentActor() override;

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void RegisterPages(const NActors::TActorContext& ctx);
    void RegisterCounters(const NActors::TActorContext& ctx);

    void ScheduleCountersUpdate(const NActors::TActorContext& ctx);
    void UpdateCounters(const NActors::TActorContext& ctx);

    void UpdateActorStats();
    void UpdateActorStatsSampled()
    {
        static constexpr int SampleRate = 128;
        if (Y_UNLIKELY(GetHandledEvents() % SampleRate == 0)) {
            UpdateActorStats();
        }
    }

    void InitAgent(const NActors::TActorContext& ctx);
    void InitLocalStorageProvider(const NActors::TActorContext& ctx);

    void ScheduleUpdateStats(const NActors::TActorContext& ctx);
    void RestartDeviceHealthChecking(const NActors::TActorContext& ctx);

    void SendRegisterRequest(const NActors::TActorContext& ctx);

    template <typename TMethod, typename TEv, typename TOp>
    void PerformIO(
        const NActors::TActorContext& ctx,
        const TEv& ev,
        TOp operation);

    template <typename TMethod, typename TRequestPtr>
    bool CheckIntersection(
        const NActors::TActorContext& ctx,
        const TRequestPtr& ev);

    void RenderDevices(IOutputStream& out) const;

    bool CanStartSecureErase(const TString& uuid);

    void SecureErase(
        const NActors::TActorContext& ctx,
        const TString& deviceId);

    TRecentBlocksTracker& GetRecentBlocksTracker(const TString& deviceUUID);

    TString GetCachedSessionsPath() const;

    void UpdateSessionCache(const NActors::TActorContext& ctx);
    void RunSessionCacheActor(const NActors::TActorContext& ctx);

    bool ShouldOffloadRequest(ui32 eventType) const;

    void ProcessDevicesToDisableIO(
        const NActors::TActorContext& ctx,
        TVector<TString> devicesToDisableIO);

    TDuration GetMaxRequestTimeout() const;

private:
    STFUNC(StateInit);
    STFUNC(StateWork);
    STFUNC(StateIdle);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleHttpInfo(
        const NActors::NMon::TEvHttpInfo::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleInitAgentCompleted(
        const TEvDiskAgentPrivate::TEvInitAgentCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleSecureEraseCompleted(
        const TEvDiskAgentPrivate::TEvSecureEraseCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleRegisterAgentResponse(
        const TEvDiskAgentPrivate::TEvRegisterAgentResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleSubscribeResponse(
        const TEvDiskRegistryProxy::TEvSubscribeResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleConnectionEstablished(
        const TEvDiskRegistryProxy::TEvConnectionEstablished::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleConnectionLost(
        const TEvDiskRegistryProxy::TEvConnectionLost::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteOrZeroCompleted(
        const TEvDiskAgentPrivate::TEvWriteOrZeroCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReportDelayedDiskAgentConfigMismatch(
        const TEvDiskAgentPrivate::TEvReportDelayedDiskAgentConfigMismatch::
            TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdateSessionCacheResponse(
        const TEvDiskAgentPrivate::TEvUpdateSessionCacheResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleCancelSuspension(
        const TEvDiskAgentPrivate::TEvCancelSuspensionRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleParsedWriteDeviceBlocks(
        const TEvDiskAgentPrivate::TEvParsedWriteDeviceBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool HandleRequests(STFUNC_SIG);
    bool RejectRequests(STFUNC_SIG);

    BLOCKSTORE_DISK_AGENT_REQUESTS(BLOCKSTORE_IMPLEMENT_REQUEST, TEvDiskAgent)
    BLOCKSTORE_DISK_AGENT_REQUESTS_PRIVATE(BLOCKSTORE_IMPLEMENT_REQUEST, TEvDiskAgentPrivate)
};

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DISK_AGENT_COUNTER(name)                                    \
    if (Counters) {                                                            \
        auto& counter = Counters->Cumulative()                                 \
            [TDiskAgentCounters::CUMULATIVE_COUNTER_Request_##name];           \
        counter.Increment(1);                                                  \
    }                                                                          \
// BLOCKSTORE_DISK_AGENT_COUNTER

}   // namespace NCloud::NBlockStore::NStorage
