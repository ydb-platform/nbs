#include "volume_balancer_actor.h"

#include "volume_balancer_events_private.h"

#include <cloud/blockstore/libs/diagnostics/volume_balancer_switch.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_balancer.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/stats_fetcher.h>

#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/mon/mon.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NMetrics;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration Timeout = TDuration::Seconds(15);
constexpr ui32 CpuLackPercentsMultiplier = 100;

////////////////////////////////////////////////////////////////////////////////

class TRemoteVolumeStatActor final
    : public TActorBootstrapped<TRemoteVolumeStatActor>
{
private:
    const TActorId VolumeBalancerActorId;
    const TVector<TString> RemoteVolumes;
    const TDuration Timeout;

    ui32 Responses = 0;

    TVector<NProto::TVolumeBalancerDiskStats> Stats;

public:
    TRemoteVolumeStatActor(
            TActorId volumeBalancerActorId,
            TVector<TString> remoteVolumes,
            TDuration timeout)
        : VolumeBalancerActorId(volumeBalancerActorId)
        , RemoteVolumes(std::move(remoteVolumes))
        , Timeout(timeout)
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        Y_UNUSED(ctx);

        for (const auto& v: RemoteVolumes) {
            auto request = std::make_unique<TEvVolume::TEvGetVolumeLoadInfoRequest>();
            request->Record.SetDiskId(v);
        }
        Become(&TThis::StateWork);

        ctx.Schedule(
            Timeout,
            new TEvents::TEvWakeup()
        );

    }

private:
    STFUNC(StateWork);

    void HandleGetVolumeLoadInfoResponse(
        const TEvVolume::TEvGetVolumeLoadInfoResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();

        if (FAILED(msg->GetStatus())) {
            LOG_ERROR(ctx, TBlockStoreComponents::VOLUME_BALANCER,
                "Failed to get stats for remote volume %s",
                msg->Record.GetStats().GetDiskId().c_str());
        } else {
            LOG_INFO(ctx, TBlockStoreComponents::VOLUME_BALANCER,
                "Got stats for remote volume %s",
                msg->Record.GetStats().GetDiskId().c_str());
            Stats.push_back(msg->Record.GetStats());
        }
        if (++Responses == RemoteVolumes.size()) {
            ReplyAndDie(ctx);
        }
    }

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        ReplyAndDie(ctx);
    }

    void ReplyAndDie(const TActorContext& ctx)
    {
        auto response = std::make_unique<TEvVolumeBalancerPrivate::TEvRemoteVolumeStats>();
        response->Stats = std::move(Stats);
        Die(ctx);
    }
};

////////////////////////////////////////////////////////////////////////////////


STFUNC(TRemoteVolumeStatActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvGetVolumeLoadInfoResponse, HandleGetVolumeLoadInfoResponse);
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME_BALANCER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}  // namespace

////////////////////////////////////////////////////////////////////////////////

TVolumeBalancerActor::TVolumeBalancerActor(
        TStorageConfigPtr storageConfig,
        IVolumeStatsPtr volumeStats,
        NCloud::NStorage::IStatsFetcherPtr statsFetcher,
        IVolumeBalancerSwitchPtr volumeBalancerSwitch,
        TActorId serviceActorId)
    : StorageConfig(std::move(storageConfig))
    , VolumeStats(std::move(volumeStats))
    , StatsFetcher(std::move(statsFetcher))
    , VolumeBalancerSwitch(std::move(volumeBalancerSwitch))
    , ServiceActorId(serviceActorId)
    , State(std::make_unique<TVolumeBalancerState>(StorageConfig))
{
}

void TVolumeBalancerActor::Bootstrap(const TActorContext& ctx)
{
    LastCpuWaitTs = ctx.Monotonic();

    SendConfigSubscriptionRequest(ctx);
    RegisterPages(ctx);
    RegisterCounters(ctx);
    Become(&TThis::StateWork);
}

void TVolumeBalancerActor::RegisterPages(const TActorContext& ctx)
{
    auto mon = AppData(ctx)->Mon;
    if (mon) {
        auto* rootPage = mon->RegisterIndexPage("blockstore", "BlockStore");

        mon->RegisterActorPage(rootPage, "balancer", "Balancer",
            false, ctx.ActorSystem(), SelfId());
    }
}

void TVolumeBalancerActor::RegisterCounters(const TActorContext& ctx)
{
    auto counters = AppData(ctx)->Counters;
    auto rootGroup = counters->GetSubgroup("counters", "blockstore");
    auto serviceCounters = rootGroup->GetSubgroup("component", "service");
    auto serverCounters = rootGroup->GetSubgroup("component", "server");
    PushCount = serviceCounters->GetCounter("PushCount", true);
    PullCount = serviceCounters->GetCounter("PullCount", true);

    ManuallyPreempted = serviceCounters->GetCounter("ManuallyPreempted", false);
    BalancerPreempted = serviceCounters->GetCounter("BalancerPreempted", false);
    InitiallyPreempted = serviceCounters->GetCounter("InitiallyPreempted", false);

    CpuWaitCounter = serverCounters->GetCounter("CpuWait", false);

    ctx.Schedule(Timeout, new TEvents::TEvWakeup);
}

bool TVolumeBalancerActor::IsBalancerEnabled() const
{
    return State->GetEnabled() && VolumeBalancerSwitch->IsBalancerEnabled();
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeBalancerActor::PullVolumeFromHive(
    const TActorContext& ctx,
    TString volume)
{
    LOG_INFO(ctx, TBlockStoreComponents::VOLUME_BALANCER,
        "Pull volume %s from hive",
        volume.data());

    auto pullRequest = std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
        volume,
        TEvService::TEvChangeVolumeBindingRequest::EChangeBindingOp::ACQUIRE_FROM_HIVE,
        NProto::EPreemptionSource::SOURCE_BALANCER);
    NCloud::Send(ctx, ServiceActorId, std::move(pullRequest));

    PullCount->Add(1);

    State->SetVolumeInProgress(volume);
}

void TVolumeBalancerActor::SendVolumeToHive(
    const TActorContext& ctx,
    TString volume)
{
    LOG_INFO(ctx, TBlockStoreComponents::VOLUME_BALANCER,
        "Push volume %s to hive",
        volume.data());

    auto pushRequest = std::make_unique<TEvService::TEvChangeVolumeBindingRequest>(
        volume,
        TEvService::TEvChangeVolumeBindingRequest::EChangeBindingOp::RELEASE_TO_HIVE,
        NProto::EPreemptionSource::SOURCE_BALANCER);
    NCloud::Send(ctx, ServiceActorId, std::move(pushRequest));

    PushCount->Add(1);

    State->SetVolumeInProgress(volume);
}

void TVolumeBalancerActor::SendConfigSubscriptionRequest(
    const TActorContext& ctx)
{
    auto req = std::make_unique<
        TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest>();
    auto blockstoreConfig =
        static_cast<ui32>(NKikimrConsole::TConfigItem::BlockstoreConfigItem);
    req->ConfigItemKinds = {
        blockstoreConfig,
    };
    NCloud::Send(
        ctx,
        NConsole::MakeConfigsDispatcherID(ctx.SelfID.NodeId()),
        std::move(req));
}

void TVolumeBalancerActor::HandleGetVolumeStatsResponse(
    const TEvService::TEvGetVolumeStatsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    if (State) {
        const auto *msg = ev->Get();

        auto [cpuWait, error] = StatsFetcher->GetCpuWait();
        if (HasError(error)) {
            auto errorMessage =
                ReportCpuWaitCounterReadError(error.GetMessage());
                LOG_WARN_S(
                    ctx,
                    TBlockStoreComponents::VOLUME_BALANCER,
                    "Failed to get CpuWait stats: " << errorMessage);
        }

        auto now = ctx.Monotonic();
        if (LastCpuWaitTs < now) {
            auto intervalUs = (now - LastCpuWaitTs).MicroSeconds();
            auto cpuLack = CpuLackPercentsMultiplier * cpuWait.MicroSeconds();
            cpuLack /= intervalUs;
            *CpuWaitCounter = cpuLack;

            LastCpuWaitTs = now;

            if (cpuLack >= StorageConfig->GetCpuLackThreshold()) {
                LOG_WARN_S(ctx, TBlockStoreComponents::VOLUME_BALANCER,
                    "Cpu wait is " << cpuLack);
            }
        }

        ui64 numManuallyPreempted = 0;
        ui64 numBalancerPreempted = 0;
        ui64 numInitiallyPreempted = 0;
        for (const auto& v: msg->VolumeStats) {
            LOG_DEBUG_S(
                ctx,
                TBlockStoreComponents::VOLUME_BALANCER,
                TStringBuilder() << "Disk:" << v.GetDiskId()
                << " Local:" << v.GetIsLocal()
                << " Preemption: " << static_cast<ui32>(v.GetPreemptionSource()));
            switch (v.GetPreemptionSource()) {
                case NProto::EPreemptionSource::SOURCE_MANUAL: {
                    ++numManuallyPreempted;
                    break;
                }
                case NProto::EPreemptionSource::SOURCE_BALANCER: {
                    ++numBalancerPreempted;
                    break;
                }
                case NProto::EPreemptionSource::SOURCE_INITIAL_MOUNT: {
                    ++numInitiallyPreempted;
                    break;
                }
                default: {
                }
            }
        }

        *ManuallyPreempted = numManuallyPreempted;
        *BalancerPreempted = numBalancerPreempted;
        *InitiallyPreempted = numInitiallyPreempted;

        auto status = VolumeStats->GatherVolumePerfStatuses();
        THashMap<TString, ui32> statusMap(status.begin(), status.end());

        State->UpdateVolumeStats(
            std::move(msg->VolumeStats),
            std::move(statusMap),
            *CpuWaitCounter,
            ctx.Now());

        if (IsBalancerEnabled()) {
            if (auto vol = State->GetVolumeToPush()) {
                SendVolumeToHive(ctx, std::move(vol));
            } else if (auto vol = State->GetVolumeToPull()) {
                PullVolumeFromHive(ctx, std::move(vol));
            }
        }

        ctx.Schedule(Timeout, new TEvents::TEvWakeup());
    }
}

void TVolumeBalancerActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto msg = std::make_unique<TEvService::TEvGetVolumeStatsRequest>();
    NCloud::Send(ctx, ServiceActorId, std::move(msg));
}

void TVolumeBalancerActor::HandleBindingResponse(
    const TEvService::TEvChangeVolumeBindingResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(ctx, TBlockStoreComponents::VOLUME_BALANCER,
            "Failed to change volume binding %s",
            msg->DiskId.Quote().data());
    }

    State->SetVolumeInProgressCompleted(msg->DiskId);
}

void TVolumeBalancerActor::HandleConfigureVolumeBalancerRequest(
    const TEvVolumeBalancer::TEvConfigureVolumeBalancerRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response = std::make_unique<TEvVolumeBalancer::TEvConfigureVolumeBalancerResponse>();
    if (State->GetEnabled()) {
        response->Record.SetOpStatus(NPrivateProto::EBalancerOpStatus::ENABLE);
    } else {
        response->Record.SetOpStatus(NPrivateProto::EBalancerOpStatus::DISABLE);
    }

    NCloud::Send(ctx, ev->Sender, std::move(response));

    const auto* msg = ev->Get();

    switch (msg->Record.GetOpStatus()) {
        case NPrivateProto::EBalancerOpStatus::ENABLE: {
            State->SetEnabled(true);
            break;
        }
        case NPrivateProto::EBalancerOpStatus::DISABLE: {
            State->SetEnabled(false);
            break;
        }
        default: {
            // remain the same
        }
    }
}

void TVolumeBalancerActor::HandleConfigSubscriptionResponse(
    const TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    LOG_INFO(
        ctx,
        TBlockStoreComponents::VOLUME_BALANCER,
        "Subscribed to config changes");
}

void TVolumeBalancerActor::HandleConfigNotificationRequest(
    const TEvConsole::TEvConfigNotificationRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;
    auto response =
        std::make_unique<TEvConsole::TEvConfigNotificationResponse>(record);
    NCloud::Reply(ctx, *ev, std::move(response));

    const auto& config = record.GetConfig();

    if (!config.HasBlockstoreConfig() ||
        !config.GetBlockstoreConfig().HasVolumePreemptionType())
    {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::VOLUME_BALANCER,
            "VolumePreemptionType is not present in ConfigDispatcher "
            "notification");
        return;
    }

    auto volumePreemptionType = static_cast<NProto::EVolumePreemptionType>(
        config.GetBlockstoreConfig().GetVolumePreemptionType());

    State->SetVolumePreemptionType(volumePreemptionType);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TVolumeBalancerActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        HFunc(TEvService::TEvChangeVolumeBindingResponse, HandleBindingResponse);
        HFunc(TEvService::TEvGetVolumeStatsResponse, HandleGetVolumeStatsResponse);
        HFunc(TEvVolumeBalancer::TEvConfigureVolumeBalancerRequest, HandleConfigureVolumeBalancerRequest);

        HFunc(NMon::TEvHttpInfo, HandleHttpInfo);

        HFunc(
            TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse,
            HandleConfigSubscriptionResponse);
        HFunc(
            TEvConsole::TEvConfigNotificationRequest,
            HandleConfigNotificationRequest);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME_BALANCER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace NCloud::NBlockStore::NStorage
