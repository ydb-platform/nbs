#include "acquire_release_devices_actors.h"

#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/storage/core/libs/actors/helpers.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage::NAcquireReleaseDevices {

////////////////////////////////////////////////////////////////////////////////

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReleaseDevicesActor final
    : public TActorBootstrapped<TReleaseDevicesActor>
{
private:
    const TActorId Owner;
    TVector<NProto::TDeviceConfig> Devices;
    const TString DiskId;
    const TString ClientId;
    const ui32 VolumeGeneration;
    const TDuration RequestTimeout;
    bool MuteIOErrors;
    NLog::EComponent Component;

    int PendingRequests = 0;

    TVector<TAgentReleaseDevicesCachedRequest> SentReleaseRequests;

public:
    TReleaseDevicesActor(
        const TActorId& owner,
        TAcquireReleaseDevicesInfo releaseDevicesInfo,
        NLog::EComponent component);

    void Bootstrap(const TActorContext& ctx);

private:
    void PrepareRequest(NProto::TReleaseDevicesRequest& request);
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

    void OnReleaseResponse(
        const TActorContext& ctx,
        ui64 cookie,
        NProto::TError error);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleReleaseDevicesResponse(
        const TEvDiskAgent::TEvReleaseDevicesResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleReleaseDevicesUndelivery(
        const TEvDiskAgent::TEvReleaseDevicesRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleTimeout(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    TString LogTargets() const;
};

////////////////////////////////////////////////////////////////////////////////

TReleaseDevicesActor::TReleaseDevicesActor(
        const TActorId& owner,
        TAcquireReleaseDevicesInfo releaseDevicesInfo,
        NLog::EComponent component)
    : Owner(owner)
    , Devices(std::move(releaseDevicesInfo.Devices))
    , DiskId(std::move(releaseDevicesInfo.DiskId))
    , ClientId(std::move(releaseDevicesInfo.ClientId))
    , VolumeGeneration(releaseDevicesInfo.VolumeGeneration)
    , RequestTimeout(releaseDevicesInfo.RequestTimeout)
    , MuteIOErrors(releaseDevicesInfo.MuteIOErrors)
    , Component(component)
{}

void TReleaseDevicesActor::PrepareRequest(
    NProto::TReleaseDevicesRequest& request)
{
    request.MutableHeaders()->SetClientId(ClientId);
    request.SetDiskId(DiskId);
    request.SetVolumeGeneration(VolumeGeneration);
}

void TReleaseDevicesActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    SortBy(Devices, [](auto& d) { return d.GetNodeId(); });

    auto it = Devices.begin();
    while (it != Devices.end()) {
        auto request =
            std::make_unique<TEvDiskAgent::TEvReleaseDevicesRequest>();
        NProto::TReleaseDevicesRequest requestCopy;
        PrepareRequest(request->Record);
        PrepareRequest(requestCopy);

        const ui32 nodeId = it->GetNodeId();
        const TString& agentId = it->GetAgentId();

        for (; it != Devices.end() && it->GetNodeId() == nodeId; ++it) {
            *request->Record.AddDeviceUUIDs() = it->GetDeviceUUID();
            *requestCopy.AddDeviceUUIDs() = it->GetDeviceUUID();
        }

        ++PendingRequests;
        SentReleaseRequests.emplace_back(agentId, std::move(requestCopy));
        NCloud::Send(
            ctx,
            MakeDiskAgentServiceId(nodeId),
            std::move(request),
            nodeId);
    }

    ctx.Schedule(RequestTimeout, new TEvents::TEvWakeup());
}

void TReleaseDevicesActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvDevicesReleaseFinished>(
            DiskId,
            ClientId,
            std::move(SentReleaseRequests),
            std::move(error)));

    Die(ctx);
}

void TReleaseDevicesActor::OnReleaseResponse(
    const TActorContext& ctx,
    ui64 cookie,
    NProto::TError error)
{
    Y_ABORT_UNLESS(PendingRequests > 0);

    if (HasError(error)) {
        LOG_LOG(
            ctx,
            MuteIOErrors ? NLog::PRI_WARN : NLog::PRI_ERROR,
            Component,
            "ReleaseDevices %s error: %s, %llu",
            LogTargets().c_str(),
            FormatError(error).c_str(),
            cookie);
    }

    if (--PendingRequests == 0) {
        ReplyAndDie(ctx, {});
    }
}

void TReleaseDevicesActor::HandleReleaseDevicesResponse(
    const TEvDiskAgent::TEvReleaseDevicesResponse::TPtr& ev,
    const TActorContext& ctx)
{
    OnReleaseResponse(ctx, ev->Cookie, ev->Get()->GetError());
}

void TReleaseDevicesActor::HandleReleaseDevicesUndelivery(
    const TEvDiskAgent::TEvReleaseDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    OnReleaseResponse(ctx, ev->Cookie, MakeError(E_REJECTED, "not delivered"));
}

void TReleaseDevicesActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

void TReleaseDevicesActor::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    const auto err = TStringBuilder()
                     << "TReleaseDevicesActor timeout." << " DiskId: " << DiskId
                     << " ClientId: " << ClientId
                     << " Targets: " << LogTargets()
                     << " VolumeGeneration: " << VolumeGeneration
                     << " PendingRequests: " << PendingRequests;

    LOG_WARN(ctx, Component, err);

    ReplyAndDie(ctx, MakeError(E_TIMEOUT, std::move(err)));
}

STFUNC(TReleaseDevicesActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvents::TEvWakeup, HandleTimeout);

        HFunc(
            TEvDiskAgent::TEvReleaseDevicesResponse,
            HandleReleaseDevicesResponse);
        HFunc(
            TEvDiskAgent::TEvReleaseDevicesRequest,
            HandleReleaseDevicesUndelivery);

        default:
            HandleUnexpectedEvent(ev, Component);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

TString TReleaseDevicesActor::LogTargets() const
{
    return LogDevices(Devices);
}

}   // namespace

TActorId CreateReleaseDevicesActor(
    const NActors::TActorContext& ctx,
    const TActorId& owner,
    TAcquireReleaseDevicesInfo releaseDevicesInfo,
    NActors::NLog::EComponent component)
{
    return NCloud::Register<TReleaseDevicesActor>(
        ctx,
        owner,
        releaseDevicesInfo,
        component);
}
}   // namespace NCloud::NBlockStore::NStorage::NAcquireReleaseDevices
