#include "service_actor.h"

#include <cloud/blockstore/libs/common/constants.h>
#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/volume_model.h>
#include <cloud/blockstore/private/api/protos/disk.pb.h>

#include <cloud/storage/core/libs/common/media.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/json/json_reader.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCreateVolumeFromDeviceActor final
    : public TActorBootstrapped<TCreateVolumeFromDeviceActor>
{
private:
    const TRequestInfoPtr RequestInfo;

    const TStorageConfigPtr Config;
    const NProto::TCreateVolumeFromDeviceRequest Request;

    NProto::TError Error;

public:
    TCreateVolumeFromDeviceActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        NProto::TCreateVolumeFromDeviceRequest request);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});

    void CreateVolumeFromDevice(const TActorContext& ctx);

    NKikimrBlockStore::TVolumeConfig CreateVolumeConfig() const;

private:
    STFUNC(StateWork);

    void HandleCreateDiskFromDevicesResponse(
        const TEvDiskRegistry::TEvCreateDiskFromDevicesResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleCreateVolumeResponse(
        const TEvSSProxy::TEvCreateVolumeResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TCreateVolumeFromDeviceActor::TCreateVolumeFromDeviceActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        NProto::TCreateVolumeFromDeviceRequest request)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , Request(std::move(request))
{}

void TCreateVolumeFromDeviceActor::Bootstrap(const TActorContext& ctx)
{
    if (!Request.GetDiskId()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "DiskId cannot be empty"));
        return;
    }

    if (!Request.GetAgentId()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "AgentId cannot be empty"));
        return;
    }

    if (!Request.GetPath()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Path cannot be empty"));
        return;
    }

    Become(&TThis::StateWork);

    CreateVolumeFromDevice(ctx);
}

void TCreateVolumeFromDeviceActor::CreateVolumeFromDevice(
    const TActorContext& ctx)
{
    auto request = std::make_unique<TEvDiskRegistry::TEvCreateDiskFromDevicesRequest>();

    auto* device = request->Record.MutableDevices()->Add();
    device->SetAgentId(Request.GetAgentId());
    device->SetDeviceName(Request.GetPath());

    *request->Record.MutableVolumeConfig() = CreateVolumeConfig();

    ctx.Send(MakeDiskRegistryProxyServiceId(), request.release());
}

void TCreateVolumeFromDeviceActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvService::TEvCreateVolumeFromDeviceResponse>(error);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

auto TCreateVolumeFromDeviceActor::CreateVolumeConfig() const
    -> NKikimrBlockStore::TVolumeConfig
{
    NKikimrBlockStore::TVolumeConfig volume;

    volume.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD_LOCAL);
    volume.SetDiskId(Request.GetDiskId());
    volume.SetProjectId(Request.GetProjectId());
    volume.SetFolderId(Request.GetFolderId());
    volume.SetCloudId(Request.GetCloudId());
    volume.SetBlockSize(DefaultLocalSSDBlockSize);

    return volume;
}

////////////////////////////////////////////////////////////////////////////////

void TCreateVolumeFromDeviceActor::HandleCreateDiskFromDevicesResponse(
    const TEvDiskRegistry::TEvCreateDiskFromDevicesResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (HasError(error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "Creation of disk %s failed: %s",
            Request.GetDiskId().Quote().c_str(),
            error.GetMessage().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    Y_DEBUG_ABORT_UNLESS(msg->Record.GetBlockCount() != 0);

    auto volumeConfig = CreateVolumeConfig();

    TVolumeParams volumeParams;
    volumeParams.BlockSize = volumeConfig.GetBlockSize();
    volumeParams.MediaKind = static_cast<NProto::EStorageMediaKind>(
        volumeConfig.GetStorageMediaKind());
    volumeParams.BlocksCountPerPartition = msg->Record.GetBlockCount();
    volumeParams.PartitionsCount = 1;

    ResizeVolume(*Config, volumeParams, {}, {}, volumeConfig);

    volumeConfig.SetCreationTs(ctx.Now().MicroSeconds());

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "Sending createvolume request for volume %s",
        volumeConfig.GetDiskId().Quote().c_str());

    auto request = std::make_unique<TEvSSProxy::TEvCreateVolumeRequest>(
        std::move(volumeConfig));

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TCreateVolumeFromDeviceActor::HandleCreateVolumeResponse(
    const TEvSSProxy::TEvCreateVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (HasError(error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "Creation of volume %s failed: %s",
            Request.GetDiskId().Quote().c_str(),
            error.GetMessage().c_str());
    }

    ReplyAndDie(ctx, error);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCreateVolumeFromDeviceActor::StateWork)
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleCreateVolumeFromDevice(
    const TEvService::TEvCreateVolumeFromDeviceRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& request = msg->Record;

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "Creating volume from device: %s, %s, %s, %s, %s, %s",
        request.GetDiskId().Quote().c_str(),
        request.GetProjectId().Quote().c_str(),
        request.GetFolderId().Quote().c_str(),
        request.GetCloudId().Quote().c_str(),
        request.GetAgentId().Quote().c_str(),
        request.GetPath().Quote().c_str());

    NCloud::Register<TCreateVolumeFromDeviceActor>(
        ctx,
        std::move(requestInfo),
        Config,
        request);
}

}   // namespace NCloud::NBlockStore::NStorage
