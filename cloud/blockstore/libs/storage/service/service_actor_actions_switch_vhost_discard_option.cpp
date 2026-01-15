#include "service_actor.h"

#include "cloud/blockstore/libs/storage/ss_proxy/ss_proxy_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

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

class TSetVhostDiscardEnabledFlagActionActor final
    : public TActorBootstrapped<TSetVhostDiscardEnabledFlagActionActor>
class TSetVhostDiscardEnabledFlagActionActor final
    : public TActorBootstrapped<TSetVhostDiscardEnabledFlagActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NPrivateProto::TSetVhostDiscardEnabledFlagRequest Request;
    NPrivateProto::TSetVhostDiscardEnabledFlagRequest Request;
    NKikimrBlockStore::TVolumeConfig VolumeConfig;

    using TResponseCreateFunc =
        std::function<NActors::IEventBasePtr(NProto::TError)>;

    TResponseCreateFunc CreateResponse;

public:
    TSetVhostDiscardEnabledFlagActionActor(
    TSetVhostDiscardEnabledFlagActionActor(
        TRequestInfoPtr requestInfo,
        TString input,
        TResponseCreateFunc createResponse);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeVolume(const TActorContext& ctx);
    void AlterVolume(
        const TActorContext& ctx,
        const TString& path,
        ui64 pathId,
        ui64 version);
    void WaitReady(const TActorContext& ctx);
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateDescribeVolume);
    STFUNC(StateAlterVolume);
    STFUNC(StateWaitReady);

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

TSetVhostDiscardEnabledFlagActionActor::TSetVhostDiscardEnabledFlagActionActor(
TSetVhostDiscardEnabledFlagActionActor::TSetVhostDiscardEnabledFlagActionActor(
    TRequestInfoPtr requestInfo,
    TString input,
    TResponseCreateFunc createResponse)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
    , CreateResponse(std::move(createResponse))
{}

void TSetVhostDiscardEnabledFlagActionActor::Bootstrap(const TActorContext& ctx)
void TSetVhostDiscardEnabledFlagActionActor::Bootstrap(const TActorContext& ctx)
{
    if (!google::protobuf::util::JsonStringToMessage(Input, &Request).ok()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    if (!Request.GetDiskId()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "DiskId should be supplied"));
        return;
    }

    VolumeConfig.SetDiskId(Request.GetDiskId());
    if (Request.GetConfigVersion()) {
        VolumeConfig.SetVersion(Request.GetConfigVersion());
    }

    DescribeVolume(ctx);
}

void TSetVhostDiscardEnabledFlagActionActor::DescribeVolume(
void TSetVhostDiscardEnabledFlagActionActor::DescribeVolume(
    const TActorContext& ctx)
{
    Become(&TThis::StateDescribeVolume);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Sending describe request for volume %s",
        Request.GetDiskId().Quote().c_str());

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(
            Request.GetDiskId()));
}

void TSetVhostDiscardEnabledFlagActionActor::AlterVolume(
void TSetVhostDiscardEnabledFlagActionActor::AlterVolume(
    const TActorContext& ctx,
    const TString& path,
    ui64 pathId,
    ui64 version)
{
    Become(&TThis::StateAlterVolume);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Sending SetVhostDiscardEnabledFlag->Alter request for %s",
        "Sending SetVhostDiscardEnabledFlag->Alter request for %s",
        path.Quote().c_str());

    auto request = CreateModifySchemeRequestForAlterVolume(
        path,
        pathId,
        version,
        VolumeConfig);
    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TSetVhostDiscardEnabledFlagActionActor::WaitReady(const TActorContext& ctx)
void TSetVhostDiscardEnabledFlagActionActor::WaitReady(const TActorContext& ctx)
{
    Become(&TThis::StateWaitReady);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Sending WaitReady request to volume %s",
        Request.GetDiskId().Quote().c_str());

    auto request = std::make_unique<TEvVolume::TEvWaitReadyRequest>();
    request->Record.SetDiskId(Request.GetDiskId());

    NCloud::Send(ctx, MakeVolumeProxyServiceId(), std::move(request));
}

void TSetVhostDiscardEnabledFlagActionActor::ReplyAndDie(
void TSetVhostDiscardEnabledFlagActionActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = CreateResponse(std::move(error));

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_setvhostdiscardenabledflag",
        "ExecuteAction_setvhostdiscardenabledflag",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TSetVhostDiscardEnabledFlagActionActor::HandleDescribeVolumeResponse(
void TSetVhostDiscardEnabledFlagActionActor::HandleDescribeVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    NProto::TError error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Volume %s: describe failed: %s",
            Request.GetDiskId().Quote().c_str(),
            FormatError(error).c_str());
        ReplyAndDie(ctx, std::move(error));
        return;
    }

    const auto& volumeDescription =
        msg->PathDescription.GetBlockStoreVolumeDescription();

    if (!VolumeConfig.GetVersion()) {
        VolumeConfig.SetVersion(
            volumeDescription.GetVolumeConfig().GetVersion());
    }

    if (volumeDescription.GetVolumeConfig().GetVhostDiscardEnabled() ==
        Request.GetVhostDiscardEnabled())
    {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Skipping setting VhostDiscardEnabled to %d; "
            "VhostDiscardEnabled option is alreasy as desired for volume %s",
            Request.GetVhostDiscardEnabled(),
            Request.GetDiskId().Quote().c_str());
        ReplyAndDie(ctx, error);
        return;
    }

    VolumeConfig.SetVhostDiscardEnabled(Request.GetVhostDiscardEnabled());

    AlterVolume(
        ctx,
        msg->Path,
        msg->PathDescription.GetSelf().GetPathId(),
        msg->PathDescription.GetSelf().GetPathVersion());
}

void TSetVhostDiscardEnabledFlagActionActor::HandleAlterVolumeResponse(
void TSetVhostDiscardEnabledFlagActionActor::HandleAlterVolumeResponse(
    const TEvSSProxy::TEvModifySchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    NProto::TError error = msg->GetError();
    ui32 errorCode = error.GetCode();

    if (FAILED(errorCode)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "SetVhostDiscardEnabledFlag->Alter of volume %s failed: %s",
            "SetVhostDiscardEnabledFlag->Alter of volume %s failed: %s",
            Request.GetDiskId().Quote().c_str(),
            msg->GetErrorReason().c_str());

        ReplyAndDie(ctx, std::move(error));
        return;
    }

    WaitReady(ctx);
}

void TSetVhostDiscardEnabledFlagActionActor::HandleWaitReadyResponse(
void TSetVhostDiscardEnabledFlagActionActor::HandleWaitReadyResponse(
    const TEvVolume::TEvWaitReadyResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    NProto::TError error = msg->GetError();

    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "SetVhostDiscardEnabledFlag->WaitReady request failed for volume %s, "
            "error: %s",
            Request.GetDiskId().Quote().c_str(),
            msg->GetErrorReason().Quote().c_str());
    } else {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Successfully done SetVhostDiscardEnabledFlag for volume %s",
            "Successfully done SetVhostDiscardEnabledFlag for volume %s",
            Request.GetDiskId().Quote().c_str());
    }

    ReplyAndDie(ctx, std::move(error));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TSetVhostDiscardEnabledFlagActionActor::StateDescribeVolume)
STFUNC(TSetVhostDiscardEnabledFlagActionActor::StateDescribeVolume)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvSSProxy::TEvDescribeVolumeResponse,
            HandleDescribeVolumeResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TSetVhostDiscardEnabledFlagActionActor::StateAlterVolume)
STFUNC(TSetVhostDiscardEnabledFlagActionActor::StateAlterVolume)
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

STFUNC(TSetVhostDiscardEnabledFlagActionActor::StateWaitReady)
STFUNC(TSetVhostDiscardEnabledFlagActionActor::StateWaitReady)
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

// TODO:_ how can we guarantee that SetVhostDiscardEnabledFlagActionActor will not
// leak in case of multiple lablet reboots?
void TServiceActor::HandleSetVhostDiscardEnabledKekFlag(
    const TEvService::TEvSetVhostDiscardEnabledKekFlagRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    NPrivateProto::TSetVhostDiscardEnabledFlagRequest
        setVhostDiscardEnabledFlagRequest;
    setVhostDiscardEnabledFlagRequest.SetDiskId(msg->DiskId);
    setVhostDiscardEnabledFlagRequest.SetVhostDiscardEnabled(
        msg->VhostDiscardEnabled);

    TString input;
    google::protobuf::util::MessageToJsonString(
        setVhostDiscardEnabledFlagRequest,
        &input);

    NCloud::Register(
        ctx,
        std::make_unique<TSetVhostDiscardEnabledFlagActionActor>(
            std::move(requestInfo),
            std::move(input),
            [](NProto::TError error)
            {
                auto response = std::make_unique<
                    TEvService::TEvSetVhostDiscardEnabledKekFlagResponse>(
                    std::move(error));
                return response;
            }));
}

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr>
TServiceActor::CreateSetVhostDiscardEnabledFlagActionActor(
TServiceActor::CreateSetVhostDiscardEnabledFlagActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TSetVhostDiscardEnabledFlagActionActor>(
    return {std::make_unique<TSetVhostDiscardEnabledFlagActionActor>(
        std::move(requestInfo),
        std::move(input),
        [](NProto::TError error)
        {
            auto response =
                std::make_unique<TEvService::TEvExecuteActionResponse>(
                    std::move(error));
            google::protobuf::util::MessageToJsonString(
                NPrivateProto::TSetVhostDiscardEnabledFlagResponse(),
                response->Record.MutableOutput());
        })};
}

}   // namespace NCloud::NBlockStore::NStorage
