#include "disk_registry_actor.h"
#include "util/string/join.h"

#include <cloud/blockstore/libs/storage/core/monitoring_utils.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NMonitoringUtils;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TChangeDeviceStateActor final
    : public TActorBootstrapped<TChangeDeviceStateActor>
{
private:
    const TActorId Owner;
    const ui64 TabletID;
    const TRequestInfoPtr RequestInfo;
    const TString DeviceUUID;
    const NProto::EDeviceState NewState;
    const NProto::EDeviceState OldState;

public:
    TChangeDeviceStateActor(
        const TActorId& owner,
        ui64 tabletID,
        TRequestInfoPtr requestInfo,
        TString deviceId,
        NProto::EDeviceState newState,
        NProto::EDeviceState oldState);

    void Bootstrap(const TActorContext& ctx);

private:
    void Notify(
        const TActorContext& ctx,
        TString message,
        const EAlertLevel alertLevel);

    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleChangeDeviceStateResponse(
        const TEvDiskRegistry::TEvChangeDeviceStateResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TChangeDeviceStateActor::TChangeDeviceStateActor(
        const TActorId& owner,
        ui64 tabletID,
        TRequestInfoPtr requestInfo,
        TString deviceId,
        NProto::EDeviceState newState,
        NProto::EDeviceState oldState)
    : Owner(owner)
    , TabletID(tabletID)
    , RequestInfo(std::move(requestInfo))
    , DeviceUUID(std::move(deviceId))
    , NewState(newState)
    , OldState(oldState)
{}

void TChangeDeviceStateActor::Bootstrap(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvDiskRegistry::TEvChangeDeviceStateRequest>();

    request->Record.SetDeviceUUID(DeviceUUID);
    request->Record.SetDeviceState(NewState);
    request->Record.SetReason("monpage action");

    NCloud::Send(ctx, Owner, std::move(request));

    Become(&TThis::StateWork);
}

void TChangeDeviceStateActor::Notify(
    const TActorContext& ctx,
    TString message,
    const EAlertLevel alertLevel)
{
    TStringStream out;

    BuildNotifyPageWithRedirect(
        out,
        std::move(message),
        TStringBuilder() << "./app?action=dev&TabletID=" << TabletID
                         << "&DeviceUUID=" << DeviceUUID,
        alertLevel);

    auto response = std::make_unique<NMon::TEvRemoteHttpInfoRes>(out.Str());
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
}

void TChangeDeviceStateActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (!HasError(error)) {
        Notify(ctx, "Operation successfully completed", EAlertLevel::SUCCESS);
    } else {
        Notify(
            ctx,
            TStringBuilder()
                << "failed to change device " << DeviceUUID.Quote()
                << " state from: " << EDeviceState_Name(OldState) << "to "
                << EDeviceState_Name(NewState) << ": " << FormatError(error),
            EAlertLevel::DANGER);
    }

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvDiskRegistryPrivate::TEvOperationCompleted>());

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TChangeDeviceStateActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

void TChangeDeviceStateActor::HandleChangeDeviceStateResponse(
    const TEvDiskRegistry::TEvChangeDeviceStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* response = ev->Get();

    ReplyAndDie(ctx, response->GetError());
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TChangeDeviceStateActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvDiskRegistry::TEvChangeDeviceStateResponse,
            HandleChangeDeviceStateResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_REGISTRY_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleHttpInfo_ChangeDeviseState(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    if (!Config->GetEnableToChangeStatesFromDiskRegistryMonpage()) {
        RejectHttpRequest(ctx, *requestInfo, "Can't change state from monpage");
        return;
    }
    const auto& newStateRaw = params.Get("NewState");
    const auto& deviceUUID = params.Get("DeviceUUID");

    if (!newStateRaw) {
        RejectHttpRequest(ctx, *requestInfo, "No new state is given");
        return;
    }
    if (!deviceUUID) {
        RejectHttpRequest(ctx, *requestInfo, "No device id is given");
        return;
    }

    NProto::EDeviceState newState;
    if (!EDeviceState_Parse(newStateRaw, &newState)) {
        RejectHttpRequest(ctx, *requestInfo, "Invalid new state");
        return;
    }

    switch (newState) {
        case NProto::DEVICE_STATE_ONLINE:
        case NProto::DEVICE_STATE_WARNING:
            break;
        default:
            RejectHttpRequest(ctx, *requestInfo, "Invalid new state");
            return;
    }

    const auto& device = State->GetDevice(deviceUUID);
    switch (device.GetState()) {
        case NProto::DEVICE_STATE_ONLINE:
        case NProto::DEVICE_STATE_WARNING:
            break;
        case NProto::DEVICE_STATE_ERROR:
            if (Config->GetEnableToChangeErrorStatesFromDiskRegistryMonpage()) {
                break;
            }
            [[fallthrough]];
        default:
            RejectHttpRequest(
                ctx,
                *requestInfo,
                "Can't change device state from " +
                    EDeviceState_Name(device.GetState()));
            return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "Change state of device[%s] on monitoring page from %s to %s",
        deviceUUID.Quote().c_str(),
        EDeviceState_Name(device.GetState()).c_str(),
        EDeviceState_Name(newState).c_str());

    auto actor = NCloud::Register<TChangeDeviceStateActor>(
        ctx,
        SelfId(),
        TabletID(),
        std::move(requestInfo),
        deviceUUID,
        newState,
        device.GetState());

    Actors.insert(actor);
}

}   // namespace NCloud::NBlockStore::NStorage
