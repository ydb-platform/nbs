#include "disk_registry_actor.h"
#include "util/string/join.h"

#include <ranges>
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

public:
    TChangeDeviceStateActor(
        const TActorId& owner,
        ui64 tabletID,
        TRequestInfoPtr requestInfo,
        TString deviceId,
        NProto::EDeviceState newState);

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
    NProto::EDeviceState newState)
    : Owner(owner)
    , TabletID(tabletID)
    , RequestInfo(std::move(requestInfo))
    , DeviceUUID(std::move(deviceId))
    , NewState(newState)
{}

void TChangeDeviceStateActor::Bootstrap(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvDiskRegistry::TEvChangeDeviceStateRequest>();

    request->Record.SetDeviceUUID(DeviceUUID);
    request->Record.SetDeviceState(NewState);

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
        TStringBuilder() << "./app?action=dev&TabletId=" << TabletID
                         << "&DeviceUUID=" << DeviceUUID,
        alertLevel);

    auto response =
        std::make_unique<NMon::TEvRemoteHttpInfoRes>(std::move(out.Str()));
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
}

void TChangeDeviceStateActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (SUCCEEDED(error.GetCode())) {
        Notify(ctx, "Operation successfully completed", EAlertLevel::SUCCESS);
    } else {
        Notify(
            ctx,
            TStringBuilder()
                << "failed to change device " << DeviceUUID.Quote()
                << " state to " << static_cast<int>(NewState) << ": " << FormatError(error),
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
    ReplyAndDie(ctx, MakeError(E_REJECTED, "Tablet is dead"));
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
                TBlockStoreComponents::DISK_REGISTRY_WORKER);
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

    static const std::vector<std::pair<NProto::EDeviceState, TString>>
        NewStateWhiteList = {
            {NProto::EDeviceState::DEVICE_STATE_ONLINE,
             ToString(
                 static_cast<int>(NProto::EDeviceState::DEVICE_STATE_ONLINE))},
            {NProto::EDeviceState::DEVICE_STATE_WARNING,
             ToString(
                 static_cast<int>(NProto::EDeviceState::DEVICE_STATE_WARNING))},
        };
    auto it = FindIf(
        NewStateWhiteList,
        [&](const auto& state) { return state.second == newStateRaw; });
    auto newState = it->first;
    Cerr << "Change state req." << Endl;
    Cerr << newStateRaw << Endl;
    Cerr << "Whitelist: "
         << JoinSeq(
                " ",
                NewStateWhiteList |
                    std::views::transform([](const auto& val)
                                          { return val.second; }))
                .c_str() << Endl;
    if (it == NewStateWhiteList.end()) {
        RejectHttpRequest(ctx, *requestInfo, "Invalid new state");
        return;
    }

    const auto& device = State->GetDevice(deviceUUID);
    if (device.GetState() == NProto::DEVICE_STATE_ERROR) {
        RejectHttpRequest(
            ctx,
            *requestInfo,
            "Can't change state of device in ERROR state");
        return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "Change state of device[%s] from monitoring page to %s",
        deviceUUID.Quote().c_str(),
        newStateRaw.c_str());

    auto actor = NCloud::Register<TChangeDeviceStateActor>(
        ctx,
        SelfId(),
        TabletID(),
        std::move(requestInfo),
        deviceUUID,
        newState);

    Actors.insert(actor);
}

}   // namespace NCloud::NBlockStore::NStorage
