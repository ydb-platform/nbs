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

class TSetVhostDiscardFlagActor final
    : public TActorBootstrapped<TSetVhostDiscardFlagActor>
{
private:
    const TRequestInfoPtr RequestInfo;

    const TString DiskId;
    const bool VhostDiscardEnabled;
    ui32 ConfigVersion = 0;

    NKikimrBlockStore::TVolumeConfig VolumeConfig;

public:
    TSetVhostDiscardFlagActor(
        TRequestInfoPtr requestInfo,
        TString diskId,
        bool vhostDiscardEnabled,
        ui32 configVersion);

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

TSetVhostDiscardFlagActor::TSetVhostDiscardFlagActor(
    TRequestInfoPtr requestInfo,
    TString diskId,
    bool vhostDiscardEnabled,
    ui32 configVersion)
    : RequestInfo(std::move(requestInfo))
    , DiskId(std::move(diskId))
    , VhostDiscardEnabled(vhostDiscardEnabled)
    , ConfigVersion(configVersion)
{}

void TSetVhostDiscardFlagActor::Bootstrap(const TActorContext& ctx)
{
    if (DiskId.empty()) {
        ReplyAndDie(ctx, MakeError(E_ARGUMENT, "DiskId should be supplied"));
        return;
    }

    VolumeConfig.SetDiskId(DiskId);
    if (ConfigVersion) {
        VolumeConfig.SetVersion(ConfigVersion);
    }

    DescribeVolume(ctx);
}

void TSetVhostDiscardFlagActor::DescribeVolume(
    const TActorContext& ctx)
{
    Become(&TThis::StateDescribeVolume);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Sending describe request for volume %s",
        DiskId.Quote().c_str());

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(DiskId));
}

void TSetVhostDiscardFlagActor::AlterVolume(
    const TActorContext& ctx,
    const TString& path,
    ui64 pathId,
    ui64 version)
{
    Become(&TThis::StateAlterVolume);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Sending SetVhostDiscardFlag->Alter request for %s",
        path.Quote().c_str());

    auto request = CreateModifySchemeRequestForAlterVolume(
        path,
        pathId,
        version,
        VolumeConfig);
    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TSetVhostDiscardFlagActor::WaitReady(const TActorContext& ctx)
{
    Become(&TThis::StateWaitReady);

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Sending SetVhostDiscardFlag->WaitReady request to volume %s",
        DiskId.Quote().c_str());

    auto request = std::make_unique<TEvVolume::TEvWaitReadyRequest>();
    request->Record.SetDiskId(DiskId);

    NCloud::Send(ctx, MakeVolumeProxyServiceId(), std::move(request));
}

void TSetVhostDiscardFlagActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response =
        std::make_unique<TEvService::TEvSetVhostDiscardFlagResponse>(
            std::move(error));

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "SetVhostDiscardFlag",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TSetVhostDiscardFlagActor::HandleDescribeVolumeResponse(
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
            DiskId.Quote().c_str(),
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
        VhostDiscardEnabled)
    {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Skipping setting VhostDiscardEnabled to %d; "
            "VhostDiscardEnabled option is already as desired for volume %s",
            VhostDiscardEnabled,
            DiskId.Quote().c_str());
        ReplyAndDie(ctx, error);
        return;
    }

    VolumeConfig.SetVhostDiscardEnabled(VhostDiscardEnabled);

    AlterVolume(
        ctx,
        msg->Path,
        msg->PathDescription.GetSelf().GetPathId(),
        msg->PathDescription.GetSelf().GetPathVersion());
}

void TSetVhostDiscardFlagActor::HandleAlterVolumeResponse(
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
            "SetVhostDiscardFlag->Alter of volume %s failed: %s",
            DiskId.Quote().c_str(),
            msg->GetErrorReason().c_str());

        ReplyAndDie(ctx, std::move(error));
        return;
    }

    WaitReady(ctx);
}

void TSetVhostDiscardFlagActor::HandleWaitReadyResponse(
    const TEvVolume::TEvWaitReadyResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    NProto::TError error = msg->GetError();

    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "SetVhostDiscardFlag->WaitReady request failed for volume "
            "%s, "
            "error: %s",
            DiskId.Quote().c_str(),
            msg->GetErrorReason().Quote().c_str());
    } else {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Successfully done SetVhostDiscardFlag for volume %s",
            DiskId.Quote().c_str());
    }

    ReplyAndDie(ctx, std::move(error));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TSetVhostDiscardFlagActor::StateDescribeVolume)
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

STFUNC(TSetVhostDiscardFlagActor::StateAlterVolume)
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

STFUNC(TSetVhostDiscardFlagActor::StateWaitReady)
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

void TServiceActor::HandleSetVhostDiscardFlagRequest(
    const TEvService::TEvSetVhostDiscardFlagRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    NCloud::Register(
        ctx,
        std::make_unique<TSetVhostDiscardFlagActor>(
            std::move(requestInfo),
            msg->DiskId,
            msg->VhostDiscardEnabled,
            0));   // ConfigVersion = 0 means use version from DescribeVolume
                   // response.
}

}   // namespace NCloud::NBlockStore::NStorage
