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

class TDestroyVolumeLinkActor final
    : public TActorBootstrapped<TDestroyVolumeLinkActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigPtr Config;
    const TString SourceDiskId;
    const TString TargetDiskId;

    NProto::TVolume SourceVolume;

public:
    TDestroyVolumeLinkActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TString sourceDiskId,
        TString targetDiskId);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeVolume(const TActorContext& ctx);
    void RemoveLinkTag(const NActors::TActorContext& ctx);

    void HandleDescribeVolumeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const TActorContext& ctx);
    void HandleRemoveLinkTagResponse(
        const TEvService::TEvExecuteActionResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvDestroyVolumeLinkResponse> response);

private:
    STFUNC(StateDescribeVolume);
};

////////////////////////////////////////////////////////////////////////////////

TDestroyVolumeLinkActor::TDestroyVolumeLinkActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TString sourceDiskId,
        TString targetDiskId)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , SourceDiskId(std::move(sourceDiskId))
    , TargetDiskId(std::move(targetDiskId))
{}

void TDestroyVolumeLinkActor::Bootstrap(const TActorContext& ctx)
{
    DescribeVolume(ctx);
}

void TDestroyVolumeLinkActor::DescribeVolume(const TActorContext& ctx)
{
    Become(&TThis::StateDescribeVolume);

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(SourceDiskId),
        0);
}

void TDestroyVolumeLinkActor::RemoveLinkTag(const NActors::TActorContext& ctx)
{
    auto requestInfo = CreateRequestInfo(
        SelfId(),
        0,   // cookie
        MakeIntrusive<TCallContext>());

    auto linkTag = TStringBuilder() << "link-to=" << TargetDiskId;
    TVector<TString> tags({linkTag});
    auto request = std::make_unique<TEvService::TEvRemoveTagsRequest>(
        SourceVolume.GetDiskId(),
        std::move(tags));

    ctx.Send(MakeStorageServiceId(), std::move(request));
}

void TDestroyVolumeLinkActor::HandleDescribeVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Volume %s: describe failed: %s",
            SourceDiskId.Quote().data(),
            FormatError(error).data());

        ReplyAndDie(
            ctx,
            std::make_unique<TEvService::TEvDestroyVolumeLinkResponse>(error));
        return;
    }

    const auto& pathDescription = msg->PathDescription;
    const auto& volumeDescription =
        pathDescription.GetBlockStoreVolumeDescription();
    const auto& volumeConfig = volumeDescription.GetVolumeConfig();

    VolumeConfigToVolume(volumeConfig, SourceVolume);
    SourceVolume.SetTokenVersion(volumeDescription.GetTokenVersion());

    RemoveLinkTag(ctx);
}

void TDestroyVolumeLinkActor::HandleRemoveLinkTagResponse(
    const TEvService::TEvExecuteActionResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    ReplyAndDie(
        ctx,
        std::make_unique<TEvService::TEvDestroyVolumeLinkResponse>(error));
}

void TDestroyVolumeLinkActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvDestroyVolumeLinkResponse> response)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDestroyVolumeLinkActor::StateDescribeVolume)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvSSProxy::TEvDescribeVolumeResponse,
            HandleDescribeVolumeResponse);
        HFunc(TEvService::TEvExecuteActionResponse, HandleRemoveLinkTagResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::SERVICE);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleDestroyVolumeLink(
    const TEvService::TEvDestroyVolumeLinkRequest::TPtr& ev,
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
            "Empty Source DiskId in DestroyVolumeLink");

        auto response =
            std::make_unique<TEvService::TEvDestroyVolumeLinkResponse>(
                MakeError(E_ARGUMENT, "Source volume name cannot be empty"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
    if (request.GetTargetDiskId().empty()) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Empty Target DiskId in DestroyVolumeLink");

        auto response =
            std::make_unique<TEvService::TEvDestroyVolumeLinkResponse>(
                MakeError(E_ARGUMENT, "Target volume name cannot be empty"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
    if (request.GetSourceDiskId() == request.GetTargetDiskId()) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Source and Target should be different in DestroyVolumeLink");

        auto response =
            std::make_unique<TEvService::TEvDestroyVolumeLinkResponse>(
                MakeError(E_ARGUMENT, "Source and Target should be different"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "DestroyVolumeLink source: %s, target: %s",
        request.GetSourceDiskId().Quote().data(),
        request.GetTargetDiskId().Quote().data());

    NCloud::Register<TDestroyVolumeLinkActor>(
        ctx,
        std::move(requestInfo),
        Config,
        request.GetSourceDiskId(),
        request.GetTargetDiskId());
}

}   // namespace NCloud::NBlockStore::NStorage
