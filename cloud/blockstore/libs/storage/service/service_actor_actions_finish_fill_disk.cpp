#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include "cloud/blockstore/libs/storage/ss_proxy/ss_proxy_actor.h"
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

class TFinishFillDiskActionActor final
    : public TActorBootstrapped<TFinishFillDiskActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    NPrivateProto::TFinishFillDiskRequest Request;
    NKikimrBlockStore::TVolumeConfig VolumeConfig;
    NProto::TError Error;

public:
    TFinishFillDiskActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

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

TFinishFillDiskActionActor::TFinishFillDiskActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TFinishFillDiskActionActor::Bootstrap(const TActorContext& ctx)
{
    if (!google::protobuf::util::JsonStringToMessage(Input, &Request).ok()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    if (!Request.GetDiskId()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "DiskId should be supplied"));
        return;
    }

    if (!Request.GetConfigVersion()) {
        ReplyAndDie(
            ctx,
            MakeError(E_ARGUMENT, "ConfigVersion should be supplied")
        );
        return;
    }

    if (!Request.GetFillGeneration()) {
        ReplyAndDie(
            ctx,
            MakeError(E_ARGUMENT, "FillGeneration should be supplied")
        );
        return;
    }

    VolumeConfig.SetIsFillFinished(true);
    VolumeConfig.SetVersion(Request.GetConfigVersion());
    VolumeConfig.SetFillGeneration(Request.GetFillGeneration());
    DescribeVolume(ctx);
}

void TFinishFillDiskActionActor::DescribeVolume(const TActorContext& ctx)
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

void TFinishFillDiskActionActor::AlterVolume(
    const TActorContext& ctx,
    const TString& path,
    ui64 pathId,
    ui64 version)
{
    Become(&TThis::StateAlterVolume);

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Sending FinishFillDisk->Alter request for %s",
        path.Quote().c_str());

    auto request = CreateModifySchemeRequestForAlterVolume(
        path, pathId, version, VolumeConfig);
    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TFinishFillDiskActionActor::WaitReady(const TActorContext& ctx)
{
    Become(&TThis::StateWaitReady);

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Sending WaitReady request to volume %s",
        Request.GetDiskId().Quote().c_str());

    auto request = std::make_unique<TEvVolume::TEvWaitReadyRequest>();
    request->Record.SetDiskId(Request.GetDiskId());

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request)
    );
}

void TFinishFillDiskActionActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(std::move(error));
    google::protobuf::util::MessageToJsonString(
        NPrivateProto::TFinishFillDiskResponse(),
        response->Record.MutableOutput()
    );

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_finishfilldisk",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TFinishFillDiskActionActor::HandleDescribeVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    NProto::TError error = msg->GetError();
    if (FAILED(error.GetCode())) {
        DescribeErrorLog(
            ctx,
            error,
            "Volume",
            Request.GetDiskId().Quote().c_str());
        ReplyAndDie(ctx, std::move(error));
        return;
    }

    AlterVolume(
        ctx,
        msg->Path,
        msg->PathDescription.GetSelf().GetPathId(),
        msg->PathDescription.GetSelf().GetPathVersion());
}

void TFinishFillDiskActionActor::HandleAlterVolumeResponse(
    const TEvSSProxy::TEvModifySchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    NProto::TError error = msg->GetError();
    ui32 errorCode = error.GetCode();

    if (FAILED(errorCode)) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "FinishFillDisk->Alter of volume %s failed: %s",
            Request.GetDiskId().Quote().c_str(),
            msg->GetErrorReason().c_str());

        ReplyAndDie(ctx, std::move(error));
        return;
    }

    WaitReady(ctx);
}

void TFinishFillDiskActionActor::HandleWaitReadyResponse(
    const TEvVolume::TEvWaitReadyResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    NProto::TError error = msg->GetError();

    if (HasError(error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "FinishFillDisk->WaitReady request failed for volume %s, error: %s",
            Request.GetDiskId().Quote().c_str(),
            msg->GetErrorReason().Quote().c_str());
    } else {
        LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
            "Successfully done FinishFillDisk for volume %s",
            Request.GetDiskId().Quote().c_str());
    }

    ReplyAndDie(ctx, std::move(error));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TFinishFillDiskActionActor::StateDescribeVolume)
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

STFUNC(TFinishFillDiskActionActor::StateAlterVolume)
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

STFUNC(TFinishFillDiskActionActor::StateWaitReady)
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

TResultOrError<IActorPtr> TServiceActor::CreateFinishFillDiskActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TFinishFillDiskActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
