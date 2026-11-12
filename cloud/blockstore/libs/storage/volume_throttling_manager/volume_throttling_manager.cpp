#include "volume_throttling_manager.h"

#include <cloud/blockstore/libs/configs_manager/events.h>
#include <cloud/blockstore/libs/configs_manager/public.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume_throttling_manager.h>
#include <cloud/blockstore/libs/storage/service/service_events_private.h>
#include <cloud/blockstore/libs/storage/volume_throttling_manager/model/helpers.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/generic/ylimits.h>

#include <google/protobuf/util/message_differencer.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Copy service rules into the delivery payload without assigning a version.
NProto::TVolumeThrottlingConfig MakeVolumeThrottlingConfig(
    const NProto::TVolumeThrottlingServiceConfig& config)
{
    NProto::TVolumeThrottlingConfig result;
    result.MutableRules()->CopyFrom(config.GetRules());
    return result;
}

class TVolumeThrottlingManagerActor final
    : public NActors::TActorBootstrapped<TVolumeThrottlingManagerActor>
{
private:
    const TDuration CycleTime;
    NProto::TVolumeThrottlingConfig ThrottlingConfig;

    // Last service rules accepted by this actor, retained while CLI rules apply.
    // Always non-null and retained independently of the notification.
    TVolumeThrottlingConfigConstPtr BaseConfig;

    // Last accepted CLI version; independent of service updates and delivery.
    ui32 LastManualVersion = 0;

    // Presence of nonempty CLI rules that mask the service rules.
    bool ManualOverride = false;

    bool WakeupScheduled = false;

public:
    TVolumeThrottlingManagerActor(
        TDuration cycleTime,
        TVolumeThrottlingConfigConstPtr initialConfig);

    void Bootstrap(const TActorContext& ctx);

private:
    // Assign a new delivery version without ordering independent source
    // versions.
    NProto::TError SetEffectiveConfig(NProto::TVolumeThrottlingConfig config);

    NProto::TError UpdateConfig(NProto::TVolumeThrottlingConfig newConfig);

    // Retain service rules from the supplied snapshot while preserving an active
    // CLI override.
    void ApplyBaseConfig(
        const IBlockstoreConfigConstPtr& blockstoreConfig,
        const TActorContext& ctx);

    void NotifyVolumes(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleConfigSubscriptionResponse(
        const TEvConfigsManager::TEvSetConfigSubscriptionResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleConfigChanged(
        const TEvConfigsManager::TEvConfigChanged::TPtr& ev,
        const TActorContext& ctx);

    void HandleUpdateConfig(
        TEvVolumeThrottlingManager::TEvUpdateVolumeThrottlingConfigRequest::
            TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleListMountedVolumesResponse(
        TEvServicePrivate::TEvListMountedVolumesResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeup(
        TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);
};

TVolumeThrottlingManagerActor::TVolumeThrottlingManagerActor(
    TDuration cycleTime,
    TVolumeThrottlingConfigConstPtr initialConfig)
    : CycleTime(cycleTime)
    , BaseConfig(std::move(initialConfig))
{
    if (BaseConfig->RulesSize()) {
        ThrottlingConfig = MakeVolumeThrottlingConfig(*BaseConfig);
        ThrottlingConfig.SetVersion(1);
    }
}

void TVolumeThrottlingManagerActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    ctx.Send(
        MakeConfigsManagerServiceId(),
        new TEvConfigsManager::TEvSetConfigSubscriptionRequest());

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Volume Throttling Manager started. Delivery version: %u",
        ThrottlingConfig.GetVersion());

    NotifyVolumes(ctx);
}

NProto::TError TVolumeThrottlingManagerActor::SetEffectiveConfig(
    NProto::TVolumeThrottlingConfig config)
{
    if (ThrottlingConfig.GetVersion() == Max<ui32>()) {
        return MakeError(
            E_INVALID_STATE,
            "Volume Throttling delivery version exhausted");
    }

    config.SetVersion(
        Max(ThrottlingConfig.GetVersion() + 1, config.GetVersion()));
    ThrottlingConfig = std::move(config);

    return {};
}

NProto::TError TVolumeThrottlingManagerActor::UpdateConfig(
    NProto::TVolumeThrottlingConfig newConfig)
{
    if (LastManualVersion >= newConfig.GetVersion()) {
        return MakeError(
            E_FAIL,
            TStringBuilder()
                << "Received throttler config with version "
                   "older then (or equal to) the known one's: ("
                << newConfig.GetVersion() << ") <= (" << LastManualVersion
                << ")");
    }

    auto error = ValidateThrottlingConfigRules(newConfig.GetRules());
    if (HasError(error)) {
        return error;
    }

    // Release the complete manual override when its replacement has no
    // rules.
    const auto version = newConfig.GetVersion();
    const bool manualOverride = newConfig.RulesSize() != 0;

    if (!manualOverride) {
        newConfig = MakeVolumeThrottlingConfig(*BaseConfig);
        newConfig.SetVersion(version);
    }

    error = SetEffectiveConfig(std::move(newConfig));
    if (HasError(error)) {
        return error;
    }

    LastManualVersion = version;
    ManualOverride = manualOverride;

    return {};
}

void TVolumeThrottlingManagerActor::ApplyBaseConfig(
    const IBlockstoreConfigConstPtr& blockstoreConfig,
    const TActorContext& ctx)
{
    auto config = blockstoreConfig->GetVolumeThrottlingConfig();
    if (google::protobuf::util::MessageDifferencer::Equals(
            *config,
            *BaseConfig))
    {
        return;
    }

    // Publish only when the service source is currently selected.
    if (!ManualOverride) {
        const auto error =
            SetEffectiveConfig(MakeVolumeThrottlingConfig(*config));
        if (HasError(error)) {
            LOG_ERROR(
                ctx,
                TBlockStoreComponents::SERVICE,
                "%s",
                FormatError(error).c_str());
            return;
        }
    }

    BaseConfig = std::move(config);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Updated service throttling, active source %s, CLI version %u, "
        "delivery version %u",
        ManualOverride ? "CLI" : "service",
        LastManualVersion,
        ThrottlingConfig.GetVersion());

    if (!ManualOverride) {
        NotifyVolumes(ctx);
    }
}

void TVolumeThrottlingManagerActor::NotifyVolumes(const TActorContext& ctx)
{
    if (ThrottlingConfig.GetVersion() == 0) {
        return;
    }

    NCloud::Send(
        ctx,
        MakeStorageServiceId(),
        std::make_unique<
            TEvServicePrivate::TEvListMountedVolumesRequest>());

    if (!WakeupScheduled) {
        WakeupScheduled = true;
        NCloud::Schedule<TEvents::TEvWakeup>(ctx, CycleTime);
    }
}

STFUNC(TVolumeThrottlingManagerActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvConfigsManager::TEvSetConfigSubscriptionResponse,
            HandleConfigSubscriptionResponse);
        HFunc(TEvConfigsManager::TEvConfigChanged, HandleConfigChanged);
        IgnoreFunc(TEvConfigsManager::TEvRemoveConfigSubscriptionResponse);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(
            TEvVolumeThrottlingManager::
                TEvUpdateVolumeThrottlingConfigRequest,
            HandleUpdateConfig);
        HFunc(
            TEvServicePrivate::TEvListMountedVolumesResponse,
            HandleListMountedVolumesResponse);
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TVolumeThrottlingManagerActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    // Remove the local publication subscription.
    ctx.Send(
        MakeConfigsManagerServiceId(),
        new TEvConfigsManager::TEvRemoveConfigSubscriptionRequest());

    Die(ctx);
}

void TVolumeThrottlingManagerActor::HandleConfigSubscriptionResponse(
    const TEvConfigsManager::TEvSetConfigSubscriptionResponse::TPtr& ev,
    const TActorContext& ctx)
{
    // Apply the configuration snapshot received in the subscription response.
    ApplyBaseConfig(ev->Get()->Config, ctx);
}

void TVolumeThrottlingManagerActor::HandleConfigChanged(
    const TEvConfigsManager::TEvConfigChanged::TPtr& ev,
    const TActorContext& ctx)
{
    // Refresh the service base independently of the selected manual source.
    ApplyBaseConfig(ev->Get()->Config, ctx);
}

void TVolumeThrottlingManagerActor::HandleUpdateConfig(
    TEvVolumeThrottlingManager::TEvUpdateVolumeThrottlingConfigRequest::
        TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* event = ev.Get();

    auto response =
        std::make_unique<TEvVolumeThrottlingManager::
                             TEvUpdateVolumeThrottlingConfigResponse>();
    response->Error = UpdateConfig(event->Get()->ThrottlingConfig);

    if (!HasError(response->Error)) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Applied throttling source %s, CLI version %u, delivery "
            "version %u",
            ManualOverride ? "CLI" : "service",
            LastManualVersion,
            ThrottlingConfig.GetVersion());

        NotifyVolumes(ctx);
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TVolumeThrottlingManagerActor::HandleListMountedVolumesResponse(
    TEvServicePrivate::TEvListMountedVolumesResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* event = ev->Get();

    for (const auto& [diskId, actorId]: event->MountedVolumes) {
        Y_UNUSED(diskId);

        if (actorId.NodeId() != SelfId().NodeId()) {
            // Volume actor is not running on current node
            continue;
        }

        auto request =
            std::make_unique<TEvVolumeThrottlingManager::
                                 TEvVolumeThrottlingConfigNotification>();
        request->Config = ThrottlingConfig;

        // VolumeProxy is unnecessary here:
        // 1. We've already filtered volumes running on the current node
        // 2. We don't want this message forwarded to other nodes
        NCloud::Send(ctx, actorId, std::move(request));
    }
}

void TVolumeThrottlingManagerActor::HandleWakeup(
    TEvents::TEvWakeup::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_UNUSED(ev);
    WakeupScheduled = false;
    NotifyVolumes(ctx);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

// Create the local rules owner without waiting for ConfigsManager startup.
NActors::IActorPtr CreateVolumeThrottlingManager(
    TDuration cycleTime,
    TVolumeThrottlingConfigConstPtr initialConfig)
{
    if (!initialConfig) {
        initialConfig =
            std::make_shared<const NProto::TVolumeThrottlingServiceConfig>();
    }
    return std::make_unique<TVolumeThrottlingManagerActor>(
        cycleTime,
        std::move(initialConfig));
}

}   // namespace NCloud::NBlockStore::NStorage
