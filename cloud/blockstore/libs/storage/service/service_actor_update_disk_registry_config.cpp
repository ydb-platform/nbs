#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TAgentConfig CreateAgentConfig(const NProto::TKnownDiskAgent& knownAgent)
{
    NProto::TAgentConfig config;

    config.SetAgentId(knownAgent.GetAgentId());

    for (auto& uuid: knownAgent.GetDevices()) {
        config.AddDevices()->SetDeviceUUID(uuid);
    }

    for (const NProto::TKnownDevice& knownDevice: knownAgent.GetKnownDevices()) {
        auto& device = *config.AddDevices();
        device.SetDeviceUUID(knownDevice.GetDeviceUUID());
        device.SetSerialNumber(knownDevice.GetSerialNumber());
    }

    return config;
}

////////////////////////////////////////////////////////////////////////////////

class TUpdateDiskRegistryConfigActor final
    : public TActorBootstrapped<TUpdateDiskRegistryConfigActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const NProto::TUpdateDiskRegistryConfigRequest Request;

public:
    TUpdateDiskRegistryConfigActor(
        TRequestInfoPtr requestInfo,
        NProto::TUpdateDiskRegistryConfigRequest request);

    void Bootstrap(const TActorContext& ctx);

private:
    void UpdateConfig(const TActorContext& ctx);

    void HandleUpdateConfigResponse(
        const TEvDiskRegistry::TEvUpdateConfigResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvUpdateDiskRegistryConfigResponse> response);

    auto CreateConfig() const -> TResultOrError<NProto::TDiskRegistryConfig>;

private:
    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////

TUpdateDiskRegistryConfigActor::TUpdateDiskRegistryConfigActor(
        TRequestInfoPtr requestInfo,
        NProto::TUpdateDiskRegistryConfigRequest request)
    : RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
{}

void TUpdateDiskRegistryConfigActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    UpdateConfig(ctx);
}

auto TUpdateDiskRegistryConfigActor::CreateConfig() const
    -> TResultOrError<NProto::TDiskRegistryConfig>
{
    NProto::TDiskRegistryConfig config;
    config.SetVersion(Request.GetVersion());

    for (const auto& agent: Request.GetKnownAgents()) {
        if (agent.DevicesSize() && agent.KnownDevicesSize()) {
            return MakeError(
                E_ARGUMENT,
                "Fields Devices and KnownDevices can't be set at the same time."
            );
        }

        *config.AddKnownAgents() = CreateAgentConfig(agent);
    }

    for (const auto& deviceOverride: Request.GetDeviceOverrides()) {
        *config.AddDeviceOverrides() = deviceOverride;
    }

    for (const auto& devicePool: Request.GetKnownDevicePools()) {
        auto* pool = config.AddDevicePoolConfigs();
        pool->SetName(devicePool.GetName());
        pool->SetKind(devicePool.GetKind());
        pool->SetAllocationUnit(devicePool.GetAllocationUnit());
    }

    return config;
}

void TUpdateDiskRegistryConfigActor::UpdateConfig(const TActorContext& ctx)
{
    auto [config, error] = CreateConfig();
    if (HasError(error)) {
        ReplyAndDie(
            ctx,
            std::make_unique<TEvService::TEvUpdateDiskRegistryConfigResponse>(
                std::move(error)));

        return;
    }

    auto request = std::make_unique<TEvDiskRegistry::TEvUpdateConfigRequest>();

    *request->Record.MutableConfig() = std::move(config);
    request->Record.SetIgnoreVersion(Request.GetIgnoreVersion());

    NCloud::Send(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TUpdateDiskRegistryConfigActor::HandleUpdateConfigResponse(
    const TEvDiskRegistry::TEvUpdateConfigResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
            "Update Disk Registry config failed: %s. Affected disks: %s",
            FormatError(error).data(),
            JoinSeq(", ", msg->Record.GetAffectedDisks()).c_str());
    }

    auto response = std::make_unique<TEvService::TEvUpdateDiskRegistryConfigResponse>(
        error);

    response->Record.MutableAffectedDisks()->Swap(msg->Record.MutableAffectedDisks());

    ReplyAndDie(ctx, std::move(response));
}

void TUpdateDiskRegistryConfigActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvUpdateDiskRegistryConfigResponse> response)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TUpdateDiskRegistryConfigActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvDiskRegistry::TEvUpdateConfigResponse, HandleUpdateConfigResponse);

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

void TServiceActor::HandleUpdateDiskRegistryConfig(
    const TEvService::TEvUpdateDiskRegistryConfigRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Update Disk Registry config");

    NCloud::Register<TUpdateDiskRegistryConfigActor>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

}   // namespace NCloud::NBlockStore::NStorage
