#pragma once

#include "public.h"

#include "volume_balancer_events_private.h"
#include "volume_balancer_state.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume_balancer.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <cloud/storage/core/libs/diagnostics/public.h>

#include <contrib/ydb/core/tablet/tablet_metrics.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/mon.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TVolumeBalancerActor final
    : public NActors::TActorBootstrapped<TVolumeBalancerActor>
{
private:
    const TStorageConfigPtr StorageConfig;
    const IVolumeStatsPtr VolumeStats;
    const NCloud::NStorage::IStatsFetcherPtr StatsFetcher;
    const IVolumeBalancerSwitchPtr VolumeBalancerSwitch;
    const NActors::TActorId ServiceActorId;

    NMonitoring::TDynamicCounters::TCounterPtr PushCount;
    NMonitoring::TDynamicCounters::TCounterPtr PullCount;

    NMonitoring::TDynamicCounters::TCounterPtr ManuallyPreempted;
    NMonitoring::TDynamicCounters::TCounterPtr BalancerPreempted;
    NMonitoring::TDynamicCounters::TCounterPtr InitiallyPreempted;

    NMonitoring::TDynamicCounters::TCounterPtr CpuWaitCounter;

    std::unique_ptr<TVolumeBalancerState> State;

    TMonotonic LastCpuWaitTs;

public:
    TVolumeBalancerActor(
        TStorageConfigPtr storageConfig,
        IVolumeStatsPtr volumeStats,
        NCloud::NStorage::IStatsFetcherPtr statsFetcher,
        IVolumeBalancerSwitchPtr volumeBalancerSwitch,
        NActors::TActorId serviceActorId);

    TVolumeBalancerActor() = default;

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void RegisterPages(const NActors::TActorContext& ctx);
    void RegisterCounters(const NActors::TActorContext& ctx);

    bool IsBalancerEnabled() const;

    void PullVolumeFromHive(
        const NActors::TActorContext& ctx,
        TString volume);

    void SendVolumeToHive(
        const NActors::TActorContext& ctx,
        TString volume);

    STFUNC(StateWork);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleBindingResponse(
        const TEvService::TEvChangeVolumeBindingResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleGetVolumeStatsResponse(
        const TEvService::TEvGetVolumeStatsResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleHttpInfo(
        const NActors::NMon::TEvHttpInfo::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleConfigureVolumeBalancerRequest(
        const TEvVolumeBalancer::TEvConfigureVolumeBalancerRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
