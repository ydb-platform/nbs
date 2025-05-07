#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/volume_model.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

NKikimrBlockStore::TVolumeConfig ConvertToVolumeConfig(
    const NProto::TCreateVolumeFromDevicesRequest& inRequest)
{
    NKikimrBlockStore::TVolumeConfig volumeConfig;
    volumeConfig.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
    volumeConfig.SetDiskId(inRequest.GetDiskId());
    volumeConfig.SetProjectId(inRequest.GetProjectId());
    volumeConfig.SetFolderId(inRequest.GetFolderId());
    volumeConfig.SetCloudId(inRequest.GetCloudId());
    volumeConfig.SetBlockSize(inRequest.GetBlockSize());
    return volumeConfig;
}

std::unique_ptr<TEvDiskRegistry::TEvCreateDiskFromDevicesRequest> CreateRequest(
    const NProto::TCreateVolumeFromDevicesRequest& inRequest)
{
    auto outRequest = std::make_unique<
        TEvDiskRegistry::TEvCreateDiskFromDevicesRequest>();

    for (const auto& deviceUUID: inRequest.GetDeviceUUIDs()) {
        outRequest->Record.MutableDevices()->Add()->SetDeviceUUID(deviceUUID);
    }

    *outRequest->Record.MutableVolumeConfig() =
        ConvertToVolumeConfig(inRequest);

    return outRequest;
}

////////////////////////////////////////////////////////////////////////////////

class TCreateDiskFromDevicesActor final
    : public TActorBootstrapped<TCreateDiskFromDevicesActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigPtr Config;
    const TString Input;

    NProto::TCreateVolumeFromDevicesRequest Request;

public:
    TCreateDiskFromDevicesActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleCreateDiskFromDevicesResponse(
        const TEvDiskRegistry::TEvCreateDiskFromDevicesResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleCreateVolumeResponse(
        const TEvSSProxy::TEvCreateVolumeResponse::TPtr& ev,
        const TActorContext& ctx);
};

TCreateDiskFromDevicesActor::TCreateDiskFromDevicesActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , Input(std::move(input))
{}

void TCreateDiskFromDevicesActor::Bootstrap(const TActorContext& ctx)
{
    if (auto status =
        google::protobuf::util::JsonStringToMessage(Input, &Request);
            !status.ok())
    {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, TStringBuilder()
            << "Failed to parse input: " << status.ToString()));
        return;
    }

    if (!Request.GetDiskId()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "DiskId cannot be empty"));
        return;
    }

    if (!Request.GetBlockSize()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "BlockSize cannot be zero"));
        return;
    }

    if (Request.GetDeviceUUIDs().size() == 0) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "DeviceUUIDs cannot be empty"));
        return;
    }

    Become(&TThis::StateWork);

    ctx.Send(
        MakeDiskRegistryProxyServiceId(),
        CreateRequest(Request).release());
}

void TCreateDiskFromDevicesActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_creatediskfromdevices",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCreateDiskFromDevicesActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvCreateDiskFromDevicesResponse,
            HandleCreateDiskFromDevicesResponse);

        HFunc(
            TEvSSProxy::TEvCreateVolumeResponse,
            HandleCreateVolumeResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TCreateDiskFromDevicesActor::HandleCreateDiskFromDevicesResponse(
    const TEvDiskRegistry::TEvCreateDiskFromDevicesResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();
    auto volumeConfig = ConvertToVolumeConfig(Request);

    if (HasError(error)) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::SERVICE,
            "Creation of disk "
            << volumeConfig.GetDiskId().Quote()
            << " failed: "
            << error.GetMessage());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    TVolumeParams volumeParams {
        .BlockSize = volumeConfig.GetBlockSize(),
        .BlocksCountPerPartition = msg->Record.GetBlockCount(),
        .PartitionsCount = 1,
        .MediaKind = static_cast<NProto::EStorageMediaKind>(
            volumeConfig.GetStorageMediaKind())
    };

    ResizeVolume(*Config, volumeParams, {}, {}, volumeConfig);

    volumeConfig.SetCreationTs(ctx.Now().MicroSeconds());

    LOG_INFO_S(ctx, TBlockStoreComponents::SERVICE,
        "Sending createvolume request for volume "
        << volumeConfig.GetDiskId().Quote());

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvCreateVolumeRequest>(volumeConfig),
        RequestInfo->Cookie);
}

void TCreateDiskFromDevicesActor::HandleCreateVolumeResponse(
    const TEvSSProxy::TEvCreateVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (HasError(error)) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::SERVICE,
            "Creation of disk "
            << Request.GetDiskId().Quote()
            << " failed: "
            << error.GetMessage());
    }

    ReplyAndDie(ctx, error);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateCreateDiskFromDevicesActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TCreateDiskFromDevicesActor>(
        std::move(requestInfo),
        Config,
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
