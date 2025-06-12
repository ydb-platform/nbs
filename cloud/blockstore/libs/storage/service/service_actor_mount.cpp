#include "service_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <contrib/ydb/core/tablet/tablet_setup.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMountVolumeActor final
    : public TActorBootstrapped<TMountVolumeActor>
{
private:
    const TActorId SessionActor;
    const TRequestInfoPtr RequestInfo;
    NProto::TMountVolumeRequest Request;

public:
    TMountVolumeActor(
            TActorId sessionActor,
            TRequestInfoPtr requestInfo,
            NProto::TMountVolumeRequest&& request)
        : SessionActor(sessionActor)
        , RequestInfo(std::move(requestInfo))
        , Request(std::move(request))
    {
    }

    void Bootstrap(const TActorContext& ctx)
    {
        auto request =
            std::make_unique<TEvServicePrivate::TEvInternalMountVolumeRequest>(
                std::move(RequestInfo->CallContext),
                std::move(Request));

        NCloud::SendWithUndeliveryTracking(
            ctx,
            SessionActor,
            std::move(request),
            RequestInfo->Cookie);

        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork);

    void HandleInternalMountVolumeResponse(
        const TEvServicePrivate::TEvInternalMountVolumeResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        *msg->Record.MutableError() = msg->GetError();
        auto response =
            std::make_unique<TEvService::TEvMountVolumeResponse>(
                std::move(msg->Record));

        NCloud::Reply(ctx, *RequestInfo, std::move(response));
        Die(ctx);
    }

    void HandleInternalMountVolumeRequest(
        const TEvServicePrivate::TEvInternalMountVolumeRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        auto response =
            std::make_unique<TEvService::TEvMountVolumeResponse>(
                MakeTabletIsDeadError(E_REJECTED, __LOCATION__));

        NCloud::Reply(ctx, *RequestInfo, std::move(response));
        Die(ctx);
    }
};

STFUNC(TMountVolumeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvServicePrivate::TEvInternalMountVolumeResponse,
            HandleInternalMountVolumeResponse);

        HFunc(
            TEvServicePrivate::TEvInternalMountVolumeRequest,
            HandleInternalMountVolumeRequest);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleMountVolume(
    const TEvService::TEvMountVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& diskId = msg->Record.GetDiskId();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    auto volume = State.GetOrAddVolume(diskId);

    if (Config->GetRemoteMountOnly()) {
        msg->Record.SetVolumeMountMode(NProto::VOLUME_MOUNT_REMOTE);
    }

    if (!volume->VolumeSessionActor) {
        volume->VolumeSessionActor = NCloud::RegisterLocal(
            ctx,
            CreateVolumeSessionActor(
                volume,
                Config,
                DiagnosticsConfig,
                ProfileLog,
                BlockDigestGenerator,
                TraceSerializer,
                EndpointEventHandler,
                RdmaClient,
                Counters,
                SharedCounters,
                TemporaryServer));
    }

    NCloud::Register(
        ctx,
        std::make_unique<TMountVolumeActor>(
            volume->VolumeSessionActor,
            std::move(requestInfo),
            std::move(ev->Get()->Record)));
}

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleSessionActorDied(
    const TEvServicePrivate::TEvSessionActorDied::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    auto* msg = ev->Get();
    const auto& diskId = msg->DiskId;

    auto volume = State.GetVolume(diskId);

    if (volume) {
        State.RemoveVolume(volume);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
