#include "volume_session_actor.h"

#include "service_actor.h"

#include "volume_client_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <contrib/ydb/core/tablet/tablet_setup.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUnmountRequestActor final
    : public TActorBootstrapped<TUnmountRequestActor>
{
private:
    const TStorageConfigPtr Config;
    const TRequestInfoPtr RequestInfo;
    const TString DiskId;
    const TString ClientId;
    const NProto::EVolumeMountMode MountMode;
    const NProto::EControlRequestSource Source;

    NProto::TError Error;

    TActorId SessionActorId;
    TActorId VolumeProxy;

    ui64 TabletId = 0;
    bool DiskRecreated = false;

public:
    TUnmountRequestActor(
        TStorageConfigPtr config,
        TRequestInfoPtr requestInfo,
        TString diskId,
        TString clientId,
        NProto::EVolumeMountMode mountMode,
        NProto::EControlRequestSource source,
        TActorId SessionActorId,
        TActorId volumeProxy,
        ui64 tabletId);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeVolume(const TActorContext& ctx);

    void RemoveClient(const TActorContext& ctx);

    void RequestVolumeStop(const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleVolumeRemoveClientResponse(
        const TEvVolume::TEvRemoveClientResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleStopVolumeResponse(
        const TEvServicePrivate::TEvStopVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDescribeVolumeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TUnmountRequestActor::TUnmountRequestActor(
        TStorageConfigPtr config,
        TRequestInfoPtr requestInfo,
        TString diskId,
        TString clientId,
        NProto::EVolumeMountMode mountMode,
        NProto::EControlRequestSource source,
        TActorId sessionActorId,
        TActorId volumeProxy,
        ui64 tabletId)
    : Config(std::move(config))
    , RequestInfo(std::move(requestInfo))
    , DiskId(std::move(diskId))
    , ClientId(std::move(clientId))
    , MountMode(mountMode)
    , Source(source)
    , SessionActorId(sessionActorId)
    , VolumeProxy(volumeProxy)
    , TabletId(tabletId)
{}

void TUnmountRequestActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    DescribeVolume(ctx);
}

void TUnmountRequestActor::DescribeVolume(const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Sending describe request for volume: %s",
        DiskId.Quote().data());

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(DiskId));
}

void TUnmountRequestActor::RemoveClient(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvRemoveClientRequest>();
    request->Record.SetDiskId(DiskId);
    request->Record.MutableHeaders()->SetClientId(ClientId);

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Sending remove client %s to volume %s",
        ClientId.Quote().data(),
        DiskId.Quote().data());

    auto proxy = VolumeProxy ? VolumeProxy : MakeVolumeProxyServiceId();

    NCloud::Send(ctx, proxy, std::move(request));
}

void TUnmountRequestActor::RequestVolumeStop(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvServicePrivate::TEvStopVolumeRequest>();

    NCloud::Send(ctx, SessionActorId, std::move(request));
}

void TUnmountRequestActor::ReplyAndDie(const TActorContext& ctx)
{
    auto response = std::make_unique<TEvService::TEvUnmountVolumeResponse>(Error);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    // notify service
    auto notify = std::make_unique<TEvServicePrivate::TEvUnmountRequestProcessed>(
        Error,
        DiskId,
        ClientId,
        RequestInfo->Sender,
        Source,
        DiskRecreated);

    NCloud::Send(ctx, SessionActorId, std::move(notify));

    Die(ctx);
}

void TUnmountRequestActor::HandleVolumeRemoveClientResponse(
    const TEvVolume::TEvRemoveClientResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    Error = msg->GetError();

    if (FAILED(Error.GetCode())) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "Failed to remove client %s from volume %s with code %u",
            ClientId.Quote().data(),
            DiskId.Quote().data(),
            Error.GetCode());

        if (GetErrorKind(Error) == EErrorKind::ErrorRetriable) {
            ReplyAndDie(ctx);
        } else {
            ReplyAndDie(ctx);
        }
        return;
    }

    if (MountMode == NProto::VOLUME_MOUNT_LOCAL) {
        RequestVolumeStop(ctx);
        return;
    }

    ReplyAndDie(ctx);
}

void TUnmountRequestActor::HandleDescribeVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_WARN(ctx, TBlockStoreComponents::SERVICE,
        "HandleDescribeVolumeResponse $#");

    if (msg->GetStatus() ==
        MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist))
    {
        LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
            "Volume %s is already destroyed before unmount for client %s",
            DiskId.Quote().data(),
            ClientId.Quote().data());

        Error = MakeError(S_ALREADY, "Volume is already destroyed");
        ReplyAndDie(ctx);
    } else if (msg->GetStatus() == NKikimrScheme::StatusSuccess) {
        auto volumeTabletId = msg->
            PathDescription.
            GetBlockStoreVolumeDescription().
            GetVolumeTabletId();

        if (volumeTabletId != TabletId) {
            DiskRecreated = true;
            Error = MakeError(S_ALREADY, "Volume is already destroyed");
            ReplyAndDie(ctx);
        }
    }

    RemoveClient(ctx);
}

void TUnmountRequestActor::HandleStopVolumeResponse(
    const TEvServicePrivate::TEvStopVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& error = ev->Get()->GetError();
    if (FAILED(error.GetCode())) {
        Error = error;
    }

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TUnmountRequestActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSSProxy::TEvDescribeVolumeResponse, HandleDescribeVolumeResponse);

        HFunc(TEvVolume::TEvRemoveClientResponse, HandleVolumeRemoveClientResponse);

        HFunc(TEvServicePrivate::TEvStopVolumeResponse, HandleStopVolumeResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::SERVICE);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeSessionActor::SendUnmountVolumeResponse(
    const TActorContext& ctx,
    const TEvService::TEvUnmountVolumeRequest::TPtr& ev,
    NProto::TError error)
{
    auto response = std::make_unique<TEvService::TEvUnmountVolumeResponse>(error);

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TVolumeSessionActor::HandleUnmountVolume(
    const TEvService::TEvUnmountVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (ShuttingDown) {
        SendUnmountVolumeResponse(
            ctx,
            ev,
            MakeError(S_ALREADY, "Volume is already unmounted"));
        return;
    }

    const auto* msg = ev->Get();
    const auto& diskId = GetDiskId(*msg);
    const auto& clientId = GetClientId(*msg);

    if (MountRequestActor || UnmountRequestActor) {
        LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
            "Queuing unmount volume %s by client %s request",
            diskId.Quote().data(),
            clientId.Quote().data());

        MountUnmountRequests.emplace(ev.Release());
        return;
    }

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "Unmounting volume: %s (client: %s)",
        diskId.Quote().data(),
        clientId.Quote().data());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    auto mountMode = NProto::VOLUME_MOUNT_REMOTE;
    auto* clientInfo = VolumeInfo->GetClientInfo(clientId);
    if (clientInfo) {
        mountMode = clientInfo->VolumeMountMode;
    }

    UnmountRequestActor = NCloud::Register<TUnmountRequestActor>(
        ctx,
        Config,
        std::move(requestInfo),
        diskId,
        clientId,
        mountMode,
        msg->Record.GetHeaders().GetInternal().GetControlSource(),
        SelfId(),
        VolumeClient,
        TabletId);
}

void TVolumeSessionActor::HandleUnmountRequestProcessed(
    const TEvServicePrivate::TEvUnmountRequestProcessed::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& clientId = msg->ClientId;
    const auto& diskId = msg->DiskId;

    UnmountRequestActor = {};

    if (HasError(msg->GetError())) {
        LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
            "Failed to unmount client %s from volume %s",
            clientId.Quote().data(),
            diskId.Quote().data());

        if (GetErrorKind(msg->GetError()) == EErrorKind::ErrorRetriable
                && msg->RequestSender == SelfId())
        {
            // this failed request was issued because of inactivity timeout
            // and it failed with retriable error -> retry it

            LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
                "Retry unmounting volume: %s (client: %s)",
                diskId.Quote().data(),
                clientId.Quote().data());

            auto requestInfo = CreateRequestInfo(
                SelfId(),
                0,
                MakeIntrusive<TCallContext>());

            auto mountMode = NProto::VOLUME_MOUNT_REMOTE;
            auto* clientInfo = VolumeInfo->GetClientInfo(clientId);
            if (clientInfo) {
                mountMode = clientInfo->VolumeMountMode;
            }

            UnmountRequestActor = NCloud::Register<TUnmountRequestActor>(
                ctx,
                Config,
                std::move(requestInfo),
                diskId,
                clientId,
                mountMode,
                msg->Source,
                SelfId(),
                VolumeClient,
                TabletId);

            return;
        }
    } else {
        LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
            "Client %s is removed from volume %s",
            clientId.Quote().data(),
            diskId.Quote().data());

        auto* clientInfo = VolumeInfo->GetClientInfo(clientId);
        if (clientInfo) {
            VolumeInfo->RemoveClientInfo(clientInfo);
            VolumeInfo->OnClientRemoved(*SharedCounters);
        }

        if (msg->Source == NProto::SOURCE_SERVICE_MONITORING) {
            // reset volume client if unmount comes from monitoring
            NCloud::Send<TEvents::TEvPoisonPill>(ctx, VolumeClient);

            VolumeClient = NCloud::Register(
                ctx,
                CreateVolumeClient(
                    Config,
                    TraceSerializer,
                    EndpointEventHandler,
                    SelfId(),
                    VolumeInfo->DiskId,
                    TabletId
                ));
            VolumeInfo->VolumeClientActor = VolumeClient;
        }

        if (msg->VolumeSessionRestartRequired) {
            // fail outstanding mount and unmount requests
            // so that the next mount/unmount request triggers a describe request
            FailPendingRequestsAndDie(
                ctx,
                MakeError(E_REJECTED, "Disk tablet is changed. Retrying"));
            return;
        }
    }

    if (!MountUnmountRequests.empty()) {
        ReceiveNextMountOrUnmountRequest(ctx);
    } else if (!VolumeInfo->IsMounted()) {
        NotifyAndDie(ctx);
    }
}

void TVolumeSessionActor::PostponeUnmountVolume(
    const TEvService::TEvUnmountVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (ShuttingDown) {
        SendUnmountVolumeResponse(
            ctx,
            ev,
            MakeError(S_ALREADY, "Volume is already unmounted"));
        return;
    }

    MountUnmountRequests.emplace(ev.Release());
}

}   // namespace NCloud::NBlockStore::NStorage
