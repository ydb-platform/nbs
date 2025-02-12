#include "service_actor.h"

#include "cloud/blockstore/libs/storage/core/proto_helpers.h"

#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/disk_validation.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/volume_model.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/media.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/size_literals.h>
#include <util/string/ascii.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCreateVolumeLinkActor final
    : public TActorBootstrapped<TCreateVolumeLinkActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigPtr Config;
    const TString SourceDiskId;
    const TString TargetDiskId;

    NProto::TVolume SourceVolume;
    NProto::TVolume TargetVolume;

public:
    TCreateVolumeLinkActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TString sourceDiskId,
        TString targetDiskId);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeVolume(const TActorContext& ctx);
    void LinkVolumes(const TActorContext& ctx);
    void AddLinkTag(const NActors::TActorContext& ctx);

    void HandleDescribeVolumeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const TActorContext& ctx);
    void HandleAddLinkTagResponse(
        const TEvService::TEvExecuteActionResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvCreateVolumeLinkResponse> response);

private:
    STFUNC(StateDescribeVolume);
};

////////////////////////////////////////////////////////////////////////////////

TCreateVolumeLinkActor::TCreateVolumeLinkActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TString sourceDiskId,
        TString targetDiskId)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , SourceDiskId(std::move(sourceDiskId))
    , TargetDiskId(std::move(targetDiskId))
{}

void TCreateVolumeLinkActor::Bootstrap(const TActorContext& ctx)
{
    DescribeVolume(ctx);
}

void TCreateVolumeLinkActor::DescribeVolume(const TActorContext& ctx)
{
    Become(&TThis::StateDescribeVolume);

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(SourceDiskId),
        0);
    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(TargetDiskId),
        1);
}

void TCreateVolumeLinkActor::LinkVolumes(const TActorContext& ctx)
{
    if (!SourceVolume.GetDiskId() || !TargetVolume.GetDiskId()) {
        return;
    }

    const auto sourceSize =
        SourceVolume.GetBlocksCount() * SourceVolume.GetBlockSize();
    const auto targetSize =
        TargetVolume.GetBlocksCount() * TargetVolume.GetBlockSize();

    if (sourceSize > targetSize) {
        auto errorMessage =
            TStringBuilder()
            << "The size of the source disk " << SourceDiskId.Quote()
            << "is larger than the target disk " << TargetDiskId.Quote() << " "
            << sourceSize << " > " << targetSize;
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE, errorMessage.c_str());

        ReplyAndDie(
            ctx,
            std::make_unique<TEvService::TEvCreateVolumeLinkResponse>(
                MakeError(E_INVALID_STATE, errorMessage)));
        return;
    }

    AddLinkTag(ctx);
}

void TCreateVolumeLinkActor::AddLinkTag(const NActors::TActorContext& ctx)
{
    auto requestInfo = CreateRequestInfo(
        SelfId(),
        0,   // cookie
        MakeIntrusive<TCallContext>());

    auto linkTag = TStringBuilder() << "link-to=" << TargetVolume.GetDiskId();
    TVector<TString> tags({linkTag});
    auto request = std::make_unique<TEvService::TEvAddTagsRequest>(

        SourceVolume.GetDiskId(),
        std::move(tags));

    ctx.Send(MakeStorageServiceId(), std::move(request));
}

void TCreateVolumeLinkActor::HandleDescribeVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& diskId = ev->Cookie == 0 ? SourceDiskId : TargetDiskId;
    auto& volume = ev->Cookie == 0 ? SourceVolume : TargetVolume;

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Volume %s: describe failed: %s",
            diskId.Quote().data(),
            FormatError(error).data());

        ReplyAndDie(
            ctx,
            std::make_unique<TEvService::TEvCreateVolumeLinkResponse>(error));
        return;
    }

    const auto& pathDescription = msg->PathDescription;
    const auto& volumeDescription =
        pathDescription.GetBlockStoreVolumeDescription();
    const auto& volumeConfig = volumeDescription.GetVolumeConfig();

    VolumeConfigToVolume(volumeConfig, volume);
    volume.SetTokenVersion(volumeDescription.GetTokenVersion());

    LinkVolumes(ctx);
}

void TCreateVolumeLinkActor::HandleAddLinkTagResponse(
    const TEvService::TEvExecuteActionResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    ReplyAndDie(
        ctx,
        std::make_unique<TEvService::TEvCreateVolumeLinkResponse>(error));
}

void TCreateVolumeLinkActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvCreateVolumeLinkResponse> response)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCreateVolumeLinkActor::StateDescribeVolume)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvSSProxy::TEvDescribeVolumeResponse,
            HandleDescribeVolumeResponse);
        HFunc(TEvService::TEvExecuteActionResponse, HandleAddLinkTagResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::SERVICE);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleCreateVolumeLink(
    const TEvService::TEvCreateVolumeLinkRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    const auto& request = msg->Record;

    if (request.GetSourceDiskId().empty()) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Empty Source DiskId in CreateVolumeLink");

        auto response =
            std::make_unique<TEvService::TEvCreateVolumeLinkResponse>(
                MakeError(E_ARGUMENT, "Source volume name cannot be empty"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
    if (request.GetTargetDiskId().empty()) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Empty Target DiskId in CreateVolumeLink");

        auto response =
            std::make_unique<TEvService::TEvCreateVolumeLinkResponse>(
                MakeError(E_ARGUMENT, "Target volume name cannot be empty"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
    if (request.GetSourceDiskId() == request.GetTargetDiskId()) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Source and Target should be different in CreateVolumeLink");

        auto response =
            std::make_unique<TEvService::TEvCreateVolumeLinkResponse>(
                MakeError(E_ARGUMENT, "Source and Target should be different"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "CreateVolumeLink source: %s, target: %s",
        request.GetSourceDiskId().Quote().data(),
        request.GetTargetDiskId().Quote().data());

    NCloud::Register<TCreateVolumeLinkActor>(
        ctx,
        std::move(requestInfo),
        Config,
        request.GetSourceDiskId(),
        request.GetTargetDiskId());
}

}   // namespace NCloud::NBlockStore::NStorage
