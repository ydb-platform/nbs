#include "acquire_release_disk.h"

#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage::NAcquireReleaseDevices {
using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAcquireDevicesActor final: public TActorBootstrapped<TAcquireDevicesActor>
{
private:
    const TActorId Owner;
    TVector<NProto::TDeviceConfig> Devices;
    TString DiskId;
    TString ClientId;
    const NProto::EVolumeAccessMode AccessMode;
    const ui64 MountSeqNumber;
    const ui32 VolumeGeneration;
    const TDuration RequestTimeout;
    const bool MuteIOErrors;
    NLog::EComponent Component;

    int PendingRequests = 0;

    TVector<TAgentAcquireDevicesCachedRequest> SentAcquireRequests;

public:
    TAcquireDevicesActor(
        const TActorId& owner,
        TVector<NProto::TDeviceConfig> devices,
        TString diskId,
        TString clientId,
        NProto::EVolumeAccessMode accessMode,
        ui64 mountSeqNumber,
        ui32 volumeGeneration,
        TDuration requestTimeout,
        bool muteIOErrors,
        NLog::EComponent component);

    void Bootstrap(const TActorContext& ctx);

private:
    void PrepareRequest(NProto::TAcquireDevicesRequest& request) const;
    void PrepareRequest(NProto::TReleaseDevicesRequest& request) const;

    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

    void OnAcquireResponse(
        const TActorContext& ctx,
        ui32 nodeId,
        NProto::TError error);

    template <typename TRequest>
    struct TSentRequest
    {
        TString AgentId;
        ui32 NodeId = 0;
        decltype(TRequest::Record) Record;
    };

    template <typename TRequest>
    TVector<TSentRequest<TRequest>> CreateRequests() const;

    template <typename TRequest>
    void SendRequests(
        const TActorContext& ctx,
        const TVector<TSentRequest<TRequest>>& requests);

private:
    STFUNC(StateAcquire);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleAcquireDevicesResponse(
        const TEvDiskAgent::TEvAcquireDevicesResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAcquireDevicesUndelivery(
        const TEvDiskAgent::TEvAcquireDevicesRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    TString LogTargets() const;
};

////////////////////////////////////////////////////////////////////////////////

TAcquireDevicesActor::TAcquireDevicesActor(
        const TActorId& owner,
        TVector<NProto::TDeviceConfig> devices,
        TString diskId,
        TString clientId,
        NProto::EVolumeAccessMode accessMode,
        ui64 mountSeqNumber,
        ui32 volumeGeneration,
        TDuration requestTimeout,
        bool muteIOErrors,
        NLog::EComponent component)
    : Owner(owner)
    , Devices(std::move(devices))
    , DiskId(std::move(diskId))
    , ClientId(std::move(clientId))
    , AccessMode(accessMode)
    , MountSeqNumber(mountSeqNumber)
    , VolumeGeneration(volumeGeneration)
    , RequestTimeout(requestTimeout)
    , MuteIOErrors(muteIOErrors)
    , Component(component)
{
    SortBy(Devices, [](auto& d) { return d.GetNodeId(); });
}

void TAcquireDevicesActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateAcquire);

    if (Devices.empty()) {
        ReplyAndDie(ctx, {});
        return;
    }

    ctx.Schedule(RequestTimeout, new TEvents::TEvWakeup());

    LOG_DEBUG(
        ctx,
        Component,
        "[%s] Sending acquire devices requests for disk %s, targets %s",
        ClientId.c_str(),
        DiskId.c_str(),
        LogTargets().c_str());

    auto sentRequests =
        CreateRequests<TEvDiskAgent::TEvAcquireDevicesRequest>();
    SendRequests(ctx, sentRequests);

    Y_ABORT_UNLESS(SentAcquireRequests.empty());
    SentAcquireRequests.reserve(sentRequests.size());
    TInstant now = ctx.Now();
    for (auto& x: sentRequests) {
        SentAcquireRequests.push_back(TAgentAcquireDevicesCachedRequest{
            .AgentId = std::move(x.AgentId),
            .Request = std::move(x.Record),
            .RequestTime = now});
    }
}

void TAcquireDevicesActor::PrepareRequest(
    NProto::TAcquireDevicesRequest& request) const
{
    request.MutableHeaders()->SetClientId(ClientId);
    request.SetAccessMode(AccessMode);
    request.SetMountSeqNumber(MountSeqNumber);
    request.SetDiskId(DiskId);
    request.SetVolumeGeneration(VolumeGeneration);
}

void TAcquireDevicesActor::PrepareRequest(
    NProto::TReleaseDevicesRequest& request) const
{
    request.MutableHeaders()->SetClientId(ClientId);
}

template <typename TRequest>
auto TAcquireDevicesActor::CreateRequests() const
    -> TVector<TSentRequest<TRequest>>
{
    auto it = Devices.begin();
    TVector<TSentRequest<TRequest>> requests;
    while (it != Devices.end()) {
        const ui32 nodeId = it->GetNodeId();

        auto& request = requests.emplace_back();
        request.AgentId = it->GetAgentId();
        request.NodeId = nodeId;
        PrepareRequest(request.Record);

        for (; it != Devices.end() && it->GetNodeId() == nodeId; ++it) {
            *request.Record.AddDeviceUUIDs() = it->GetDeviceUUID();
        }
    }
    return requests;
}

template <typename TRequest>
void TAcquireDevicesActor::SendRequests(
    const TActorContext& ctx,
    const TVector<TSentRequest<TRequest>>& requests)
{
    PendingRequests = 0;

    for (const auto& r: requests) {
        auto request = std::make_unique<TRequest>(TCallContextPtr{}, r.Record);

        LOG_DEBUG(
            ctx,
            Component,
            "[%s] Send an acquire request to node #%d. Devices: %s",
            ClientId.c_str(),
            r.NodeId,
            JoinSeq(", ", request->Record.GetDeviceUUIDs()).c_str());

        auto event = std::make_unique<IEventHandle>(
            MakeDiskAgentServiceId(r.NodeId),
            ctx.SelfID,
            request.release(),
            IEventHandle::FlagForwardOnNondelivery,
            r.NodeId,
            &ctx.SelfID   // forwardOnNondelivery
        );

        ctx.Send(event.release());

        ++PendingRequests;
    }
}

void TAcquireDevicesActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            Component,
            "[%s] AcquireDisk %s targets %s error: %s",
            ClientId.c_str(),
            DiskId.c_str(),
            LogTargets().c_str(),
            FormatError(error).c_str());
    }
    auto response = std::make_unique<TEvDevicesAcquireFinished>(
        std::move(DiskId),
        std::move(ClientId),
        std::move(SentAcquireRequests),
        std::move(Devices),
        std::move(error));

    NCloud::Send(ctx, Owner, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TAcquireDevicesActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

void TAcquireDevicesActor::OnAcquireResponse(
    const TActorContext& ctx,
    ui32 nodeId,
    NProto::TError error)
{
    Y_ABORT_UNLESS(PendingRequests > 0);

    if (HasError(error) && !MuteIOErrors) {
        LOG_ERROR(
            ctx,
            Component,
            "[%s] AcquireDevices on the node #%d %s error: %s",
            ClientId.c_str(),
            nodeId,
            LogTargets().c_str(),
            FormatError(error).c_str());

        if (GetErrorKind(error) != EErrorKind::ErrorRetriable) {
            LOG_DEBUG(
                ctx,
                Component,
                "[%s] Canceling acquire operation for disk %s, targets %s",
                ClientId.c_str(),
                DiskId.c_str(),
                LogTargets().c_str());

            SendRequests(
                ctx,
                CreateRequests<TEvDiskAgent::TEvReleaseDevicesRequest>());
        }

        SentAcquireRequests.clear();
        ReplyAndDie(ctx, std::move(error));

        return;
    }

    if (--PendingRequests == 0) {
        ReplyAndDie(ctx, {});
    }
}

void TAcquireDevicesActor::HandleAcquireDevicesResponse(
    const TEvDiskAgent::TEvAcquireDevicesResponse::TPtr& ev,
    const TActorContext& ctx)
{
    OnAcquireResponse(
        ctx,
        SafeIntegerCast<ui32>(ev->Cookie),
        ev->Get()->GetError());
}

void TAcquireDevicesActor::HandleAcquireDevicesUndelivery(
    const TEvDiskAgent::TEvAcquireDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    OnAcquireResponse(
        ctx,
        SafeIntegerCast<ui32>(ev->Cookie),
        MakeError(E_REJECTED, "not delivered"));
}

void TAcquireDevicesActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    OnAcquireResponse(
        ctx,
        SafeIntegerCast<ui32>(ev->Cookie),
        MakeError(E_REJECTED, "timeout"));
}

////////////////////////////////////////////////////////////////////////////////

TString TAcquireDevicesActor::LogTargets() const
{
    return LogDevices(Devices);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TAcquireDevicesActor::StateAcquire)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvDiskAgent::TEvAcquireDevicesResponse,
            HandleAcquireDevicesResponse);
        HFunc(
            TEvDiskAgent::TEvAcquireDevicesRequest,
            HandleAcquireDevicesUndelivery);

        HFunc(TEvents::TEvWakeup, HandleWakeup);

        default:
            HandleUnexpectedEvent(ev, Component);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TActorId AcquireDevices(
    const NActors::TActorContext& ctx,
    const TActorId& owner,
    TVector<NProto::TDeviceConfig> devices,
    TString diskId,
    TString clientId,
    NProto::EVolumeAccessMode accessMode,
    ui64 mountSeqNumber,
    ui32 volumeGeneration,
    TDuration requestTimeout,
    bool muteIOErrors,
    NLog::EComponent component)
{
    return NCloud::Register<TAcquireDevicesActor>(
        ctx,
        owner,
        std::move(devices),
        diskId,
        std::move(clientId),
        accessMode,
        mountSeqNumber,
        volumeGeneration,
        requestTimeout,
        muteIOErrors,
        component);
}

}   // namespace NCloud::NBlockStore::NStorage::NAcquireReleaseDevices
