#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include "cloud/blockstore/libs/storage/ss_proxy/ss_proxy_actor.h"
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRebaseVolumeActionActor final
    : public TActorBootstrapped<TRebaseVolumeActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NPrivateProto::TRebaseVolumeRequest Request;
    NKikimrBlockStore::TVolumeConfig VolumeConfig;
    NProto::TError Error;

public:
    TRebaseVolumeActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeBaseVolume(const TActorContext& ctx);
    void DescribeVolume(const TActorContext& ctx);
    void AlterVolume(
        const TActorContext& ctx,
        const TString& path,
        ui64 pathId,
        ui64 version);
    void WaitReady(const TActorContext& ctx);
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateDescribeBaseVolume);
    STFUNC(StateDescribeVolume);
    STFUNC(StateAlterVolume);
    STFUNC(StateWaitReady);

    void HandleDescribeBaseVolumeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDescribeVolumeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAlterVolumeResponse(
        const TEvSSProxy::TEvModifySchemeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWaitReadyResponse(
        const TEvVolume::TEvWaitReadyResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TRebaseVolumeActionActor::TRebaseVolumeActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TRebaseVolumeActionActor::Bootstrap(const TActorContext& ctx)
{
    if (!google::protobuf::util::JsonStringToMessage(Input, &Request).ok()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    if (!Request.GetDiskId()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "DiskId should be supplied"));
        return;
    }

    if (!Request.GetTargetBaseDiskId()) {
        ReplyAndDie(
            ctx,
            MakeError(E_ARGUMENT, "TargetBaseDiskId should be supplied")
        );
        return;
    }

    if (!Request.GetConfigVersion()) {
        ReplyAndDie(
            ctx,
            MakeError(E_ARGUMENT, "ConfigVersion should be supplied")
        );
        return;
    }

    VolumeConfig.SetDiskId(Request.GetDiskId());
    VolumeConfig.SetBaseDiskId(Request.GetTargetBaseDiskId());
    VolumeConfig.SetVersion(Request.GetConfigVersion());
    DescribeBaseVolume(ctx);
}

void TRebaseVolumeActionActor::DescribeBaseVolume(const TActorContext& ctx)
{
    Become(&TThis::StateDescribeBaseVolume);

    const auto& baseDiskId = Request.GetTargetBaseDiskId();

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Sending describe request for base volume %s",
        Request.GetTargetBaseDiskId().Quote().c_str());

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(baseDiskId));
}

void TRebaseVolumeActionActor::DescribeVolume(const TActorContext& ctx)
{
    Become(&TThis::StateDescribeVolume);

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Sending describe request for volume %s",
        Request.GetDiskId().Quote().c_str());

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(Request.GetDiskId()));
}

void TRebaseVolumeActionActor::AlterVolume(
    const TActorContext& ctx,
    const TString& path,
    ui64 pathId,
    ui64 version)
{
    Become(&TThis::StateAlterVolume);

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Sending RebaseVolume->Alter request for %s",
        path.Quote().c_str());

    auto request = CreateModifySchemeRequestForAlterVolume(
        path, pathId, version, VolumeConfig);
    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TRebaseVolumeActionActor::WaitReady(const TActorContext& ctx)
{
    Become(&TThis::StateWaitReady);

    auto request = std::make_unique<TEvVolume::TEvWaitReadyRequest>();
    request->Record.SetDiskId(Request.GetDiskId());

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request)
    );
}

void TRebaseVolumeActionActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(std::move(error));
    google::protobuf::util::MessageToJsonString(
        NPrivateProto::TRebaseVolumeResponse(),
        response->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_rebasevolume",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TRebaseVolumeActionActor::HandleDescribeBaseVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    NProto::TError error = msg->GetError();

    if (FAILED(error.GetCode())) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::SERVICE, "Base volume "
            << VolumeConfig.GetBaseDiskId().Quote() << ": describe failed: "
            << FormatError(error));

        ReplyAndDie(ctx, std::move(error));

        return;
    }

    const auto& pathDescr = msg->PathDescription;
    const auto& volumeDescr = pathDescr.GetBlockStoreVolumeDescription();
    const auto& tabletId = volumeDescr.GetVolumeTabletId();

    LOG_INFO_S(ctx, TBlockStoreComponents::SERVICE, "Resolved base disk id "
        << VolumeConfig.GetBaseDiskId().Quote()
        << " to tablet id " << tabletId);

    VolumeConfig.SetBaseDiskTabletId(tabletId);

    DescribeVolume(ctx);
}

void TRebaseVolumeActionActor::HandleDescribeVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    NProto::TError error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "Volume %s: describe failed: %s",
            Request.GetDiskId().Quote().c_str(),
            FormatError(error).c_str());
        ReplyAndDie(ctx, std::move(error));
        return;
    }

    AlterVolume(
        ctx,
        msg->Path,
        msg->PathDescription.GetSelf().GetPathId(),
        msg->PathDescription.GetSelf().GetPathVersion());
}

void TRebaseVolumeActionActor::HandleAlterVolumeResponse(
    const TEvSSProxy::TEvModifySchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    NProto::TError error = msg->GetError();
    ui32 errorCode = error.GetCode();

    if (FAILED(errorCode)) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "RebaseVolume->Alter of volume %s failed: %s",
            Request.GetDiskId().Quote().c_str(),
            msg->GetErrorReason().c_str());

        ReplyAndDie(ctx, std::move(error));
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Sending WaitReady request to volume %s",
        Request.GetDiskId().Quote().c_str());

    WaitReady(ctx);
}

void TRebaseVolumeActionActor::HandleWaitReadyResponse(
    const TEvVolume::TEvWaitReadyResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    NProto::TError error = msg->GetError();

    if (HasError(error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "RebaseVolume->WaitReady request failed for volume %s, error: %s",
            Request.GetDiskId().Quote().c_str(),
            msg->GetErrorReason().Quote().c_str());
    } else {
        LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
            "Successfully done RebaseVolume with TargetBaseDiskId %s for volume %s",
            Request.GetTargetBaseDiskId().Quote().c_str(),
            Request.GetDiskId().Quote().c_str());
    }

    ReplyAndDie(ctx, std::move(error));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TRebaseVolumeActionActor::StateDescribeBaseVolume)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvSSProxy::TEvDescribeVolumeResponse,
            HandleDescribeBaseVolumeResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TRebaseVolumeActionActor::StateDescribeVolume)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSSProxy::TEvDescribeVolumeResponse, HandleDescribeVolumeResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TRebaseVolumeActionActor::StateAlterVolume)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSSProxy::TEvModifySchemeResponse, HandleAlterVolumeResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TRebaseVolumeActionActor::StateWaitReady)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvWaitReadyResponse, HandleWaitReadyResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateRebaseVolumeActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TRebaseVolumeActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
