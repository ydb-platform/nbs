#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/volume_model.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

NKikimrBlockStore::TVolumeConfig ConvertToVolumeConfig(
    const NProto::TCreateMirroredVolumeFromDevicesRequest& inRequest)
{
    NKikimrBlockStore::TVolumeConfig volumeConfig;
    volumeConfig.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
    volumeConfig.SetDiskId(inRequest.GetMasterDiskId());
    volumeConfig.SetProjectId(inRequest.GetProjectId());
    volumeConfig.SetFolderId(inRequest.GetFolderId());
    volumeConfig.SetCloudId(inRequest.GetCloudId());
    volumeConfig.SetBlockSize(inRequest.GetBlockSize());
    return volumeConfig;
}

std::unique_ptr<TEvDiskRegistry::TEvCreateMirroredDiskFromDevicesRequest> CreateRequest(
    const NProto::TCreateMirroredVolumeFromDevicesRequest& inRequest)
{
    auto outRequest = std::make_unique<
        TEvDiskRegistry::TEvCreateMirroredDiskFromDevicesRequest>();

    for (const auto& replica: inRequest.GetReplicas()) {
        auto* outReplica = outRequest->Record.MutableReplicas()->Add();
        for (const auto& deviceUUID: replica.GetDeviceUUIDs()) {
            outReplica->MutableDevices()->Add()->SetDeviceUUID(deviceUUID);
        }
    }

    *outRequest->Record.MutableVolumeConfig() =
        ConvertToVolumeConfig(inRequest);
    outRequest->Record.SetForce(inRequest.GetForce());
    outRequest->Record.SetKeepBlockCount(inRequest.GetKeepBlockCount());

    return outRequest;
}

////////////////////////////////////////////////////////////////////////////////

class TCreateMirroredDiskFromDevicesActor final
    : public TActorBootstrapped<TCreateMirroredDiskFromDevicesActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigConstPtr Config;
    const TString Input;

    NProto::TCreateMirroredVolumeFromDevicesRequest Request;
    ui64 BlockCount = 0;

public:
    TCreateMirroredDiskFromDevicesActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigConstPtr config,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleCreateMirroredDiskFromDevicesResponse(
        const TEvDiskRegistry::TEvCreateMirroredDiskFromDevicesResponse::TPtr&
            ev,
        const TActorContext& ctx);

    void HandleDescribeVolumeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleCreateVolumeResponse(
        const TEvSSProxy::TEvCreateVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleReallocateDiskResponse(
        const TEvVolume::TEvReallocateDiskResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TCreateMirroredDiskFromDevicesActor::TCreateMirroredDiskFromDevicesActor(
    TRequestInfoPtr requestInfo,
    TStorageConfigConstPtr config,
    TString input)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , Input(std::move(input))
{}

void TCreateMirroredDiskFromDevicesActor::Bootstrap(const TActorContext& ctx)
{
    if (!google::protobuf::util::JsonStringToMessage(Input, &Request).ok()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    const auto& volumeConfig = ConvertToVolumeConfig(Request);
    if (!volumeConfig.GetDiskId()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "DiskId cannot be empty"));
        return;
    }

    if (!volumeConfig.GetBlockSize()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "BlockSize cannot be zero"));
        return;
    }

    if (Request.GetReplicas().size() == 0) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Replicas cannot be empty"));
        return;
    }

    Become(&TThis::StateWork);

    ctx.Send(
        MakeDiskRegistryProxyServiceId(),
        CreateRequest(Request).release());
}

void TCreateMirroredDiskFromDevicesActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
        std::move(error));

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_createmirroreddiskfromdevices",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TCreateMirroredDiskFromDevicesActor::
    HandleCreateMirroredDiskFromDevicesResponse(
        const TEvDiskRegistry::TEvCreateMirroredDiskFromDevicesResponse::TPtr&
            ev,
        const TActorContext& ctx)
{
    const auto& error = ev->Get()->GetError();
    const auto& diskId = Request.GetMasterDiskId();

    if (HasError(error)) {
        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Creation of disk " << diskId.Quote()
                                << " failed: " << error.GetMessage());

        ReplyAndDie(ctx, error);
        return;
    }

    BlockCount = ev->Get()->Record.GetBlockCount();

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Sending describe request for volume " << diskId.Quote());

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(diskId, true));
}

void TCreateMirroredDiskFromDevicesActor::HandleDescribeVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();
    const auto& diskId = Request.GetMasterDiskId();

    if (HasError(error)) {
        const bool volumeExists =
            error.GetCode() != E_NOT_FOUND &&
            error.GetCode() !=
                MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist);

        if (volumeExists) {
            LOG_ERROR_S(
                ctx,
                TBlockStoreComponents::SERVICE,
                "Describe of volume " << diskId.Quote()
                                      << " failed: " << error.GetMessage());

            ReplyAndDie(ctx, error);
            return;
        }

        auto volumeConfig = ConvertToVolumeConfig(Request);

        TVolumeParams volumeParams{
            .BlockSize = volumeConfig.GetBlockSize(),
            .BlocksCountPerPartition = BlockCount,
            .PartitionsCount = 1,
            .MediaKind = static_cast<NProto::EStorageMediaKind>(
                volumeConfig.GetStorageMediaKind()),
            .IsSystem = volumeConfig.GetIsSystem()};

        ResizeVolume(*Config, volumeParams, {}, {}, volumeConfig);

        volumeConfig.SetCreationTs(ctx.Now().MicroSeconds());

        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Sending createvolume request for volume "
                << volumeConfig.GetDiskId().Quote());

        NCloud::Send(
            ctx,
            MakeSSProxyServiceId(),
            std::make_unique<TEvSSProxy::TEvCreateVolumeRequest>(volumeConfig),
            RequestInfo->Cookie);
        return;
    }

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Sending reallocate request for volume " << diskId.Quote());

    auto request = std::make_unique<TEvVolume::TEvReallocateDiskRequest>();
    request->Record.SetDiskId(diskId);

    NCloud::Send(ctx, MakeVolumeProxyServiceId(), std::move(request));
}

void TCreateMirroredDiskFromDevicesActor::HandleReallocateDiskResponse(
    const TEvVolume::TEvReallocateDiskResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (HasError(error)) {
        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Reallocation of volume "
                << Request.GetMasterDiskId().Quote()
                << " failed: " << error.GetMessage());
    }

    ReplyAndDie(ctx, error);
}

void TCreateMirroredDiskFromDevicesActor::HandleCreateVolumeResponse(
    const TEvSSProxy::TEvCreateVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (HasError(error)) {
        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Creation of disk " << Request.GetMasterDiskId().Quote()
                                << " failed: " << error.GetMessage());
    }

    ReplyAndDie(ctx, error);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCreateMirroredDiskFromDevicesActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvCreateMirroredDiskFromDevicesResponse,
            HandleCreateMirroredDiskFromDevicesResponse);

        HFunc(
            TEvSSProxy::TEvDescribeVolumeResponse,
            HandleDescribeVolumeResponse);

        HFunc(TEvSSProxy::TEvCreateVolumeResponse, HandleCreateVolumeResponse);

        HFunc(
            TEvVolume::TEvReallocateDiskResponse,
            HandleReallocateDiskResponse);

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

TResultOrError<IActorPtr>
TServiceActor::CreateCreateMirroredDiskFromDevicesActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TCreateMirroredDiskFromDevicesActor>(
        std::move(requestInfo),
        Config,
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
