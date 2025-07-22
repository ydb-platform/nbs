#include "throttling_manager.h"

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/throttling_manager.h>
#include <cloud/blockstore/libs/storage/service/service_events_private.h>
#include <cloud/blockstore/libs/storage/throttling_manager/model/helpers.h>

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

class TThrottlingManagerActor final
    : public NActors::TActorBootstrapped<TThrottlingManagerActor>
{
private:
    const TDuration CycleTime;
    NProto::TThrottlingConfig ThrottlingConfig;

public:
    explicit TThrottlingManagerActor(TDuration cycleTime)
        : CycleTime(cycleTime)
    {}

    void Bootstrap(const TActorContext& /*unused*/)
    {
        Become(&TThis::StateWork);
    }

private:
    NProto::TError UpdateConfig(NProto::TThrottlingConfig newConfig)
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
                TEvThrottlingManager::TEvUpdateConfigRequest,
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
        TEvThrottlingManager::TEvUpdateConfigRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        auto* event = ev.Get();

        auto response =
            std::make_unique<TEvThrottlingManager::TEvUpdateConfigResponse>();
        response->Error = UpdateConfig(event->Get()->ThrottlingConfig);

        if (!HasError(response->Error)) {
            NotifyVolumes(ctx);
        }

        NCloud::Send(ctx, event->Sender, std::move(response));
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
                std::make_unique<TEvThrottlingManager::TEvNotifyVolume>();
            request->Config = ThrottlingConfig;

            // No need to use VolumeProxy here
            NCloud::Send(ctx, actorId, std::move(request));
        }
    }

    void HandleWakeup(
        TEvents::TEvWakeup::TPtr& /*unused*/,
        const NActors::TActorContext& ctx)
    {
        NotifyVolumes(ctx);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateThrottlingManager(TDuration cycleTime)
{
    // TODO: form TThrottlerManagerConfig from storage config
    return std::make_unique<TThrottlingManagerActor>(cycleTime);
}

}   // namespace NCloud::NBlockStore::NStorage
