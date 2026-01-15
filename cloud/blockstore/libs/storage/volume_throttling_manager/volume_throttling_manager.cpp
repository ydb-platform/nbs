#include "volume_throttling_manager.h"

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

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TVolumeThrottlingManagerActor final
    : public NActors::TActorBootstrapped<TVolumeThrottlingManagerActor>
{
private:
    const TDuration CycleTime;
    NProto::TVolumeThrottlingConfig ThrottlingConfig;

public:
    explicit TVolumeThrottlingManagerActor(TDuration cycleTime)
        : CycleTime(cycleTime)
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        Y_UNUSED(ctx);
        Become(&TThis::StateWork);
    }

private:
    NProto::TError UpdateConfig(NProto::TVolumeThrottlingConfig newConfig)
    {
        if (ThrottlingConfig.GetVersion() >= newConfig.GetVersion()) {
            return MakeError(
                E_FAIL,
                TStringBuilder()
                    << "Received throttler config with version "
                       "older then (or equal to) the known one's: ("
                    << newConfig.GetVersion() << ") <= ("
                    << ThrottlingConfig.GetVersion() << ")");
        }

        auto error = ValidateThrottlingConfig(newConfig);
        if (HasError(error)) {
            return error;
        }

        ThrottlingConfig = std::move(newConfig);
        return {};
    }

    void NotifyVolumes(const TActorContext& ctx)
    {
        if (ThrottlingConfig.GetVersion() == 0) {
            return;
        }

        NCloud::Send(
            ctx,
            MakeStorageServiceId(),
            std::make_unique<
                TEvServicePrivate::TEvListMountedVolumesRequest>());

        NCloud::Schedule<TEvents::TEvWakeup>(ctx, CycleTime);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
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

    void HandleUpdateConfig(
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
            NotifyVolumes(ctx);
        }

        NCloud::Reply(ctx, *ev, std::move(response));
    }

    void HandleListMountedVolumesResponse(
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

    void HandleWakeup(
        TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        Y_UNUSED(ev);
        NotifyVolumes(ctx);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateVolumeThrottlingManager(TDuration cycleTime)
{
    return std::make_unique<TVolumeThrottlingManagerActor>(cycleTime);
}

}   // namespace NCloud::NBlockStore::NStorage
