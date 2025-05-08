#include "volume_session_actor.h"

#include "volume_client_actor.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/volume_proxy/volume_proxy.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TVolumeSessionActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateDescribe);
    DescribeVolume(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeSessionActor::DescribeVolume(const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Sending describe request for volume: %s",
        VolumeInfo->DiskId.Quote().data());

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(VolumeInfo->DiskId));
}

void TVolumeSessionActor::HandleDescribeVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "Describe request failed for volume %s: %s",
            VolumeInfo->DiskId.Quote().data(),
            FormatError(error).data());
        FailPendingRequestsAndDie(ctx, error);
    } else {
        const auto& pathDescription = msg->PathDescription;
        const auto& volumeDescription =
            pathDescription.GetBlockStoreVolumeDescription();

        TabletId = volumeDescription.GetVolumeTabletId();

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
        VolumeInfo->StorageMediaKind = static_cast<NProto::EStorageMediaKind>(
            volumeDescription.GetVolumeConfig().GetStorageMediaKind());

        Become(&TThis::StateWork);
        if (!MountUnmountRequests.empty()) {
            ReceiveNextMountOrUnmountRequest(ctx);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeSessionActor::ReceiveNextMountOrUnmountRequest(
    const TActorContext&)
{
    if (MountUnmountRequests.empty()) {
        return;
    }

    auto request = std::move(MountUnmountRequests.front());
    MountUnmountRequests.pop();

    TAutoPtr<IEventHandle> handle(request.release());
    Receive(handle);
}

void TVolumeSessionActor::RemoveInactiveClients(const TActorContext& ctx)
{
    TInstant now = ctx.Now();
    TDuration inactiveClientsTimeout = Config->GetInactiveClientsTimeout();

    auto& clientInfos = VolumeInfo->ClientInfosByClientId;
    for (auto& pair: clientInfos) {
        auto* clientInfo = pair.second;
        TDuration passedTime = now - clientInfo->LastActivityTime;
        if (passedTime < inactiveClientsTimeout) {
            continue;
        }

        if (MountUnmountRequests.empty()
            && !MountRequestActor
            && !UnmountRequestActor)
        {
            LOG_WARN(ctx, TBlockStoreComponents::SERVICE,
                "Unmounting client %s from volume %s as it has been inactive "
                "for too long (last activity at %s, timeout is %s)",
                clientInfo->ClientId.Quote().data(),
                VolumeInfo->DiskId.Quote().data(),
                ToString(clientInfo->LastActivityTime).data(),
                inactiveClientsTimeout.ToString().data());

            auto request = std::make_unique<TEvService::TEvUnmountVolumeRequest>();
            request->Record.MutableHeaders()->SetClientId(clientInfo->ClientId);
            request->Record.SetDiskId(VolumeInfo->DiskId);
            request->Record.SetSessionId(VolumeInfo->SessionId);

            NCloud::Send(ctx, SelfId(), std::move(request));
        } else {
            // reset inactivity timeout since there are mount/unmount
            // requests in a queue.
            LOG_WARN_S(ctx, TBlockStoreComponents::SERVICE,
                "Skip unmounting inactive client " << clientInfo->ClientId
                 << " (timeout " << (now - clientInfo->LastActivityTime) << ")"
                 << " from volume " << VolumeInfo->DiskId);

            clientInfo->LastActivityTime = now;
        }
    }

    ScheduleInactiveClientsRemoval(ctx);
}

void TVolumeSessionActor::ScheduleInactiveClientsRemoval(const TActorContext& ctx)
{
    if (IsClientsCheckScheduled) {
        return;
    }

    TDuration nextTimeout;
    bool onceSetNextTimeout = false;

    TInstant now = ctx.Now();

    // Increase the "accurate" timeout value a little to prevent race
    // between clients trying to ping the service and the service
    // evicting inactive clients
    TDuration inactiveClientsTimeout = Config->GetInactiveClientsTimeout();
    inactiveClientsTimeout *= 1.05;

    for (const auto& clientInfo: VolumeInfo->ClientInfos) {
        TInstant timeout = clientInfo.LastActivityTime + inactiveClientsTimeout;
        TDuration remainingTime = timeout - now;
        if (remainingTime == TDuration::Zero()) {
            // This client is being unmounted right now
            continue;
        }
        if (!onceSetNextTimeout) {
            nextTimeout = remainingTime;
            onceSetNextTimeout = true;
        } else if (nextTimeout > remainingTime) {
            nextTimeout = remainingTime;
        }
    }

    if (nextTimeout == TDuration::Zero()) {
        // No clients with active timeout were found, no need to schedule
        // inactive clients removal
        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Sleeping for %s before checking volume %s for inactive clients",
        nextTimeout.ToString().Quote().data(),
        VolumeInfo->DiskId.Quote().data());

    IsClientsCheckScheduled = true;
    ctx.Schedule(
        nextTimeout,
        new TEvServicePrivate::TEvInactiveClientsTimeout);
}

void TVolumeSessionActor::NotifyAndDie(const TActorContext& ctx)
{
    VolumeInfo->VolumeClientActor = {};
    VolumeInfo->VolumeSessionActor = {};

    if (StartVolumeActor) {
        NCloud::Send<TEvents::TEvPoisonPill>(ctx, StartVolumeActor);
        StartVolumeActor = {};
    }

    auto notification = std::make_unique<TEvServicePrivate::TEvSessionActorDied>();
    notification->DiskId = VolumeInfo->DiskId;
    NCloud::Send(ctx, MakeStorageServiceId(), std::move(notification));

    NCloud::Send<TEvents::TEvPoisonPill>(ctx, VolumeClient);

    Die(ctx);
}

void TVolumeSessionActor::FailPendingRequestsAndDie(
    const NActors::TActorContext& ctx,
    NProto::TError error)
{
    ShuttingDown = true;

    Y_DEBUG_ABORT_UNLESS(
        FAILED(error.GetCode()),
        "Shutdown requested with a successful code: %u", error.GetCode());
    ShuttingDownError = std::move(error);

    while (!MountUnmountRequests.empty()) {
        ReceiveNextMountOrUnmountRequest(ctx);
    }
    NotifyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TVolumeSessionActor::StateDescribe)
{
    switch (ev->GetTypeRewrite()) {

        HFunc(
            TEvSSProxy::TEvDescribeVolumeResponse,
            HandleDescribeVolumeResponse);

        HFunc(
            TEvServicePrivate::TEvInternalMountVolumeRequest,
            PostponeMountVolume);

        HFunc(TEvService::TEvUnmountVolumeRequest, PostponeUnmountVolume);

        HFunc(
            TEvService::TEvChangeVolumeBindingRequest,
            PostponeChangeVolumeBindingRequest);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TVolumeSessionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {

        HFunc(
            TEvServicePrivate::TEvInternalMountVolumeRequest,
            HandleInternalMountVolume);

        HFunc(TEvService::TEvUnmountVolumeRequest, HandleUnmountVolume);

        HFunc(
            TEvServicePrivate::TEvInactiveClientsTimeout,
            HandleInactiveClientsTimeout);

        HFunc(
            TEvServicePrivate::TEvMountRequestProcessed,
            HandleMountRequestProcessed);

        HFunc(
            TEvServicePrivate::TEvUnmountRequestProcessed,
            HandleUnmountRequestProcessed);

        HFunc(
            TEvServicePrivate::TEvStartVolumeRequest,
            HandleStartVolumeRequest);

        HFunc(
            TEvServicePrivate::TEvStopVolumeRequest,
            HandleStopVolumeRequest);

        HFunc(
            TEvServicePrivate::TEvVolumeTabletStatus,
            HandleVolumeTabletStatus);

        HFunc(
            TEvServicePrivate::TEvStartVolumeActorStopped,
            HandleStartVolumeActorStopped);

        HFunc(TEvServicePrivate::TEvVolumePipeReset , HandleVolumePipeReset);

        HFunc(
            TEvService::TEvChangeVolumeBindingRequest,
            HandleChangeVolumeBindingRequest);

        IgnoreFunc(TEvService::TEvUnmountVolumeResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TVolumeSessionActor::HandleInactiveClientsTimeout(
    const TEvServicePrivate::TEvInactiveClientsTimeout::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    IsClientsCheckScheduled = false;
    RemoveInactiveClients(ctx);
}

void TVolumeSessionActor::HandleVolumePipeReset(
    const TEvServicePrivate::TEvVolumePipeReset::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LastPipeResetTick = msg->ResetTick;
    LOG_INFO_S(ctx, TBlockStoreComponents::SERVICE,
        "Pipe to volume " << VolumeInfo->DiskId.Quote() <<
        " is disconnected at " << LastPipeResetTick);
}

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateVolumeSessionActor(
    TVolumeInfoPtr volumeInfo,
    TStorageConfigPtr config,
    TDiagnosticsConfigPtr diagnosticsConfig,
    IProfileLogPtr profileLog,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    ITraceSerializerPtr traceSerializer,
    NServer::IEndpointEventHandlerPtr endpointEventHandler,
    NRdma::IClientPtr rdmaClient,
    std::shared_ptr<NKikimr::TTabletCountersBase> counters,
    TSharedServiceCountersPtr sharedCounters)
{
    return std::make_unique<TVolumeSessionActor>(
        std::move(volumeInfo),
        std::move(config),
        std::move(diagnosticsConfig),
        std::move(profileLog),
        std::move(blockDigestGenerator),
        std::move(traceSerializer),
        std::move(endpointEventHandler),
        std::move(rdmaClient),
        std::move(counters),
        std::move(sharedCounters));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
