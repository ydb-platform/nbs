#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

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
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
}

STFUNC(TRegisterActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvDiskRegistry::TEvRegisterAgentResponse, HandleRegisterAgentResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::DISK_AGENT_WORKER);
            break;
    }
}

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

void TDiskAgentActor::HandleRegisterAgentResponse(
    const TEvDiskAgentPrivate::TEvRegisterAgentResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (!HasError(msg->GetError())) {
        RegistrationInProgress = false;

        LOG_INFO(ctx, TBlockStoreComponents::DISK_AGENT, "Register completed");

        NCloud::Send(
            ctx,
            MakeDiskRegistryProxyServiceId(),
            std::make_unique<TEvDiskRegistryProxy::TEvSubscribeRequest>(ctx.SelfID));

    } else {
        LOG_WARN(ctx, TBlockStoreComponents::DISK_AGENT,
            "Register failed: %s. Try later", FormatError(msg->GetError()).c_str());

        auto request = std::make_unique<TEvDiskAgentPrivate::TEvRegisterAgentRequest>();

        ctx.ExecutorThread.Schedule(
            AgentConfig->GetRegisterRetryTimeout(),
            new IEventHandle(ctx.SelfID, ctx.SelfID, request.get()));

        request.release();
    }
}

void TDiskAgentActor::HandleSubscribeResponse(
    const TEvDiskRegistryProxy::TEvSubscribeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (SUCCEEDED(msg->GetStatus()) && !msg->Connected) {
        SendRegisterRequest(ctx);
    }
}

void TDiskAgentActor::SendRegisterRequest(const TActorContext& ctx)
{
    if (RegistrationInProgress) {
        LOG_INFO(ctx, TBlockStoreComponents::DISK_AGENT, "Registration in progress");
        return;
    }

    if (State->GetDevicesCount() == 0) {
        LOG_WARN(ctx, TBlockStoreComponents::DISK_AGENT, "No devices to register");
        return;
    }

    RegistrationInProgress = true;
    NCloud::Send(
        ctx,
        ctx.SelfID,
        std::make_unique<TEvDiskAgentPrivate::TEvRegisterAgentRequest>());
}

void TDiskAgentActor::HandleConnectionLost(
    const TEvDiskRegistryProxy::TEvConnectionLost::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    SendRegisterRequest(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
