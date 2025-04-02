#include "service_actor.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
bool ConvertToRemoteRequestAndForward(
    const TActorContext& ctx,
    const typename TMethod::TRequest::TPtr& ev,
    const TVolumeInfo& volume)
{
    if (volume.VolumeClientActor) {
        ForwardRequestWithNondeliveryTracking(ctx, volume.VolumeClientActor, *ev);
        return true;
    }
    return false;
}

template <>
bool ConvertToRemoteRequestAndForward<TEvService::TReadBlocksLocalMethod>(
    const TActorContext& ctx,
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TVolumeInfo& volume)
{
    NCloud::Register(ctx, CreateReadBlocksRemoteActor(
        ev,
        volume.VolumeInfo->GetBlockSize(),
        volume.VolumeClientActor));
    return true;
}

template <>
bool ConvertToRemoteRequestAndForward<TEvService::TWriteBlocksLocalMethod>(
    const TActorContext& ctx,
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TVolumeInfo& volume)
{
    NCloud::Register(ctx, CreateWriteBlocksRemoteActor(
        ev,
        volume.VolumeInfo->GetBlockSize(),
        volume.VolumeClientActor));
    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TServiceActor::ForwardRequest(
    const TActorContext& ctx,
    const typename TMethod::TRequest::TPtr& ev)
{
    const auto* msg = ev->Get();

    LWTRACK(
        RequestReceived_Service,
        msg->CallContext->LWOrbit,
        TMethod::Name,
        msg->CallContext->RequestId);

    const TString& diskId = GetDiskId(*msg);
    const TString& sessionId = GetSessionId(*msg);
    const TString& clientId = GetClientId(*msg);

    auto replyError = [=] (
        const TActorContext& ctx,
        const typename TMethod::TRequest::TPtr& ev,
        ui32 errorCode,
        TString errorMessage)
    {
        auto response = std::make_unique<typename TMethod::TResponse>(
            MakeError(errorCode, std::move(errorMessage)));

        NCloud::Reply(ctx, *ev, std::move(response));
    };

    if constexpr (!RequiresMount<TMethod>) {
        auto volume = State.GetVolume(diskId);
        if (volume) {
            auto* clientInfo = volume->GetClientInfo(clientId);
            if (clientInfo) {
                // forward request directly to the volume
                LOG_TRACE(ctx, TBlockStoreComponents::SERVICE,
                    "Forward %s request to volume: %lu %s",
                    TMethod::Name,
                    volume->TabletId,
                    ToString(volume->VolumeActor).data());
                TActorId target;
                if (volume->VolumeActor) {
                    target = volume->VolumeActor;
                } else if (volume->VolumeClientActor) {
                    target = volume->VolumeClientActor;
                }
                if (target) {
                    ForwardRequestWithNondeliveryTracking(ctx, target, *ev);
                    return;
                }
            }
        }
        // forward request through tablet pipe
        ctx.Send(ev->Forward(MakeVolumeProxyServiceId()));
        return;
    }

    auto volume = State.GetVolume(diskId);
    if (!volume) {
        replyError(
            ctx,
            ev,
            E_BS_INVALID_SESSION,
            "Invalid session (volume not found)");
        return;
    }

    auto* clientInfo = volume->GetClientInfo(clientId);
    if (!clientInfo) {
        replyError(
            ctx,
            ev,
            E_BS_INVALID_SESSION,
            "Invalid session (clientInfo not found)");
        return;
    }

    Y_ABORT_UNLESS(volume && clientInfo);

    if (volume->SessionId != sessionId) {
        replyError(
            ctx,
            ev,
            E_BS_INVALID_SESSION,
            "Invalid session (SessionId not match)");
        return;
    }

    clientInfo->LastActivityTime = ctx.Now();

    msg->CallContext->SetSilenceRetriableErrors(volume->RebindingIsInflight);

    if (volume->BindingType != NProto::BINDING_REMOTE &&
        clientInfo->VolumeMountMode == NProto::VOLUME_MOUNT_LOCAL)
    {
        if (volume->State == TVolumeInfo::INITIAL) {
            // Volume tablet is neither started nor starting: remount is
            // required to trigger volume tablet start
            replyError(
                ctx,
                ev,
                E_BS_INVALID_SESSION,
                "Invalid session (State=INITIAL)");
            return;
        }

        if (volume->State != TVolumeInfo::STARTED) {
            // Volume tablet is starting or rebooting
            replyError(ctx, ev, E_REJECTED, TStringBuilder()
                << "Volume not ready: " << diskId.Quote());
            return;
        }
    }

    if constexpr (IsReadOrWriteMethod<TMethod>) {
        if (volume->VolumeInfo.Defined()) {
            ReadWriteCounters.Update(
                ctx.Now(),
                Config->GetPingMetricsHalfDecayInterval(),
                CalculateBytesCount(
                    msg->Record,
                    volume->VolumeInfo->GetBlockSize()));
        } else {
            Y_DEBUG_ABORT_UNLESS(0);
        }
    }

    if (volume->VolumeActor) {
        // forward request directly to the volume
        LOG_TRACE(ctx, TBlockStoreComponents::SERVICE,
            "Forward %s request to volume: %lu %s",
            TMethod::Name,
            volume->TabletId,
            ToString(volume->VolumeActor).data());

        ForwardRequestWithNondeliveryTracking(
            ctx,
            volume->VolumeActor,
            *ev);
        return;
    }

    bool convertedToRemote = ConvertToRemoteRequestAndForward<TMethod>(
        ctx,
        ev,
        *volume);
    if (convertedToRemote) {
        return;
    }

    // forward request through tablet pipe
    ctx.Send(ev->Forward(MakeVolumeProxyServiceId()));
}

#define BLOCKSTORE_FORWARD_REQUEST(name, ns)                                   \
    void TServiceActor::Handle##name(                                          \
        const ns::TEv##name##Request::TPtr& ev,                                \
        const TActorContext& ctx)                                              \
    {                                                                          \
        ForwardRequest<ns::T##name##Method>(ctx, ev);                          \
    }                                                                          \
// BLOCKSTORE_FORWARD_REQUEST

BLOCKSTORE_FORWARD_REQUEST(ReadBlocks,          TEvService)
BLOCKSTORE_FORWARD_REQUEST(WriteBlocks,         TEvService)
BLOCKSTORE_FORWARD_REQUEST(ZeroBlocks,          TEvService)
BLOCKSTORE_FORWARD_REQUEST(StatVolume,          TEvService)
BLOCKSTORE_FORWARD_REQUEST(CreateCheckpoint,    TEvService)
BLOCKSTORE_FORWARD_REQUEST(DeleteCheckpoint,    TEvService)
BLOCKSTORE_FORWARD_REQUEST(GetChangedBlocks,    TEvService)
BLOCKSTORE_FORWARD_REQUEST(GetCheckpointStatus, TEvService)
BLOCKSTORE_FORWARD_REQUEST(ReadBlocksLocal,     TEvService)
BLOCKSTORE_FORWARD_REQUEST(WriteBlocksLocal,    TEvService)

#undef BLOCKSTORE_FORWARD_REQUEST

}   // namespace NCloud::NBlockStore::NStorage
