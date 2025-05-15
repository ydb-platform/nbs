#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDescribeDiskRegistryConfigActor final
    : public TActorBootstrapped<TDescribeDiskRegistryConfigActor>
{
private:
    const TRequestInfoPtr RequestInfo;

public:
    explicit TDescribeDiskRegistryConfigActor(TRequestInfoPtr requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeConfig(const TActorContext& ctx);

    void HandleDescribeConfigResponse(
        const TEvDiskRegistry::TEvDescribeConfigResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvDescribeDiskRegistryConfigResponse> response);

private:
    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////

TDescribeDiskRegistryConfigActor::TDescribeDiskRegistryConfigActor(
        TRequestInfoPtr requestInfo)
    : RequestInfo(std::move(requestInfo))
{}

void TDescribeDiskRegistryConfigActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    DescribeConfig(ctx);
}

void TDescribeDiskRegistryConfigActor::DescribeConfig(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvDiskRegistry::TEvDescribeConfigRequest>();

    NCloud::Send(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TDescribeDiskRegistryConfigActor::HandleDescribeConfigResponse(
    const TEvDiskRegistry::TEvDescribeConfigResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
            "Describe Disk Registry config failed: %s",
            FormatError(error).data());
    }

    auto response = std::make_unique<TEvService::TEvDescribeDiskRegistryConfigResponse>(error);

    const auto& config = msg->Record.GetConfig();
    auto& result = response->Record;

    result.SetVersion(config.GetVersion());

    for (const auto& src: config.GetKnownAgents()) {
        NProto::TKnownDiskAgent& dst = *result.AddKnownAgents();
        dst.SetAgentId(src.GetAgentId());

        for (const NProto::TDeviceConfig& device: src.GetDevices()) {
            NProto::TKnownDevice& knownDevice = *dst.AddKnownDevices();
            knownDevice.SetDeviceUUID(device.GetDeviceUUID());
            knownDevice.SetSerialNumber(device.GetSerialNumber());
        }

        SortBy(*dst.MutableKnownDevices(), [] (const auto& device) {
            return device.GetDeviceUUID();
        });
    }

    *result.MutableDeviceOverrides() = config.GetDeviceOverrides();

    for (const auto& src: config.GetDevicePoolConfigs()) {
        auto* dst = result.AddKnownDevicePools();
        dst->SetName(src.GetName());
        dst->SetKind(src.GetKind());
        dst->SetAllocationUnit(src.GetAllocationUnit());
    }

    SortBy(*result.MutableKnownAgents(), [] (const auto& a) {
        return a.GetAgentId();
    });

    ReplyAndDie(ctx, std::move(response));
}

void TDescribeDiskRegistryConfigActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvDescribeDiskRegistryConfigResponse> response)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDescribeDiskRegistryConfigActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvDiskRegistry::TEvDescribeConfigResponse, HandleDescribeConfigResponse);

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

void TServiceActor::HandleDescribeDiskRegistryConfig(
    const TEvService::TEvDescribeDiskRegistryConfigRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Describe Disk Registry config");

    NCloud::Register<TDescribeDiskRegistryConfigActor>(
        ctx,
        std::move(requestInfo));
}


}   // namespace NCloud::NBlockStore::NStorage
