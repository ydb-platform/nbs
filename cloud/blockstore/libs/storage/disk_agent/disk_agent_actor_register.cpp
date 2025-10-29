#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <ydb/core/base/appdata.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRegisterActor final
    : public TActorBootstrapped<TRegisterActor>
{
private:
    const TActorId Owner;
    TRequestInfoPtr RequestInfo;
    NProto::TAgentConfig Config;

public:
    TRegisterActor(
        const TActorId& owner,
        TRequestInfoPtr requestInfo,
        NProto::TAgentConfig config);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleRegisterAgentResponse(
        const TEvDiskRegistry::TEvRegisterAgentResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TRegisterActor::TRegisterActor(
        const TActorId& owner,
        TRequestInfoPtr requestInfo,
        NProto::TAgentConfig config)
    : Owner(owner)
    , RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
{}

void TRegisterActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LOG_INFO(ctx, TBlockStoreComponents::DISK_AGENT_WORKER,
        "Send RegisterAgent request: NodeId=%u, AgentId=%s"
        ", SeqNo=%lu, Devices=[%s]",
        Config.GetNodeId(),
        Config.GetAgentId().c_str(),
        Config.GetSeqNumber(),
        [this] {
            TStringStream out;
            for (const auto& config: Config.GetDevices()) {
                out << config.GetDeviceUUID()
                    << " ("
                    << config.GetDeviceName() << ", PhysicalOffset="
                    << config.GetPhysicalOffset() << ", Size="
                    << config.GetBlocksCount() << " x "
                    << config.GetBlockSize() << ", Rack="
                    << config.GetRack() << ", SN="
                    << config.GetSerialNumber()
                    << "); ";
            }
            return out.Str();
        }().c_str());

    auto request = std::make_unique<TEvDiskRegistry::TEvRegisterAgentRequest>();
    *request->Record.MutableAgentConfig() = std::move(Config);

    ctx.Send(MakeDiskRegistryProxyServiceId(), request.release());
}

void TRegisterActor::HandleRegisterAgentResponse(
    const TEvDiskRegistry::TEvRegisterAgentResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto response = std::make_unique<TEvDiskAgentPrivate::TEvRegisterAgentResponse>(
        msg->GetError());
    response->DevicesToDisableIO.assign(
        msg->Record.GetDevicesToDisableIO().cbegin(),
        msg->Record.GetDevicesToDisableIO().cend());
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
}

STFUNC(TRegisterActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvDiskRegistry::TEvRegisterAgentResponse, HandleRegisterAgentResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_AGENT_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TUpdateDevicesWithSuspendedIOActor
    : public TActorBootstrapped<TUpdateDevicesWithSuspendedIOActor>
{
private:
    const TString CachePath;
    const TVector<TString> DevicesToDisableIO;

public:
    TUpdateDevicesWithSuspendedIOActor(
            TString cachePath,
            TVector<TString> devicesToSuspendIO)
        : CachePath{std::move(cachePath)}
        , DevicesToDisableIO{std::move(devicesToSuspendIO)}
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        auto error = NStorage::UpdateDevicesWithSuspendedIO(
            CachePath,
            DevicesToDisableIO);

        if (HasError(error)) {
            LOG_ERROR_S(
                ctx,
                TBlockStoreComponents::DISK_AGENT,
                "Can't update DevicesWithSuspendedIO in the config cache "
                "file: "
                    << FormatError(error));
        } else {
            LOG_INFO(
                ctx,
                TBlockStoreComponents::DISK_AGENT,
                "DevicesWithSuspendedIO has been successfully updated");
        }

        Die(ctx);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::HandleRegisterAgent(
    const TEvDiskAgentPrivate::TEvRegisterAgentRequest::TPtr& ev,
    const TActorContext& ctx)
{
    NProto::TAgentConfig config;

    config.SetNodeId(ctx.SelfID.NodeId());
    config.SetAgentId(AgentConfig->GetAgentId());
    config.SetSeqNumber(AgentConfig->GetSeqNumber());
    config.SetDedicatedDiskAgent(AgentConfig->GetDedicatedDiskAgent());
    config.SetTemporaryAgent(AgentConfig->GetTemporaryAgent());

    for (auto& device: State->GetDevices()) {
        *config.AddDevices() = std::move(device);
    }

    const auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    NCloud::Register<TRegisterActor>(
        ctx,
        ctx.SelfID,
        std::move(requestInfo),
        std::move(config));
}

void TDiskAgentActor::ProcessDevicesToDisableIO(
    const NActors::TActorContext& ctx,
    TVector<TString> devicesToDisableIO)
{
    if (!AgentConfig->GetDisableBrokenDevices()) {
        return;
    }

    const THashSet<TString> uuids(
        devicesToDisableIO.cbegin(),
        devicesToDisableIO.cend());

    for (const auto& uuid: State->GetDeviceIds()) {
        if (uuids.contains(uuid)) {
            LOG_INFO_S(
                ctx,
                TBlockStoreComponents::DISK_AGENT,
                "Disable device " << uuid);
            State->DisableDevice(uuid);
        } else {
            State->EnableDevice(uuid);
        }
    }

    TString cachePath = Config->GetCachedDiskAgentConfigPath().empty()
                            ? AgentConfig->GetCachedConfigPath()
                            : Config->GetCachedDiskAgentConfigPath();

    if (cachePath.empty()) {
        return;
    }

    auto actor = std::make_unique<TUpdateDevicesWithSuspendedIOActor>(
        std::move(cachePath),
        std::move(devicesToDisableIO));

    // Starting an actor on the IO pool to avoid file operations in the User
    // pool
    ctx.Register(
        actor.release(),
        TMailboxType::HTSwap,
        NKikimr::AppData()->IOPoolId);
}

void TDiskAgentActor::HandleRegisterAgentResponse(
    const TEvDiskAgentPrivate::TEvRegisterAgentResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (!HasError(msg->GetError())) {
        RegistrationState = ERegistrationState::Registered;
        LOG_INFO(ctx, TBlockStoreComponents::DISK_AGENT, "Register completed");
        ProcessDevicesToDisableIO(ctx, std::move(msg->DevicesToDisableIO));
        RestartDeviceHealthChecking(ctx);
    } else {
        LOG_WARN(ctx, TBlockStoreComponents::DISK_AGENT,
            "Register failed: %s. Try later", FormatError(msg->GetError()).c_str());

        auto request = std::make_unique<TEvDiskAgentPrivate::TEvRegisterAgentRequest>();

        ctx.Schedule(
            AgentConfig->GetRegisterRetryTimeout(),
            std::make_unique<IEventHandle>(ctx.SelfID, ctx.SelfID, request.get()));

        request.release();
    }
}

void TDiskAgentActor::HandleSubscribeResponse(
    const TEvDiskRegistryProxy::TEvSubscribeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (FAILED(msg->GetStatus()) || msg->Discovered) {
        SendRegisterRequest(ctx);
    }
}

void TDiskAgentActor::SendRegisterRequest(const TActorContext& ctx)
{
    if (RegistrationState == ERegistrationState::InProgress) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Registration in progress");
        return;
    }

    RegistrationState = ERegistrationState::InProgress;
    NCloud::Send(
        ctx,
        ctx.SelfID,
        std::make_unique<TEvDiskAgentPrivate::TEvRegisterAgentRequest>());
}

void TDiskAgentActor::HandleConnectionEstablished(
    const TEvDiskRegistryProxy::TEvConnectionEstablished::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    SendRegisterRequest(ctx);
}

void TDiskAgentActor::HandleConnectionLost(
    const TEvDiskRegistryProxy::TEvConnectionLost::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    if (State->GetPartiallySuspended()) {
        return;
    }

    SendRegisterRequest(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
