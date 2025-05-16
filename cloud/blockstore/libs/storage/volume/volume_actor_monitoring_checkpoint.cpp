#include "volume_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/string.h>
#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;
using namespace NMonitoringUtils;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class THttpCheckpointActor final
    : public TActorBootstrapped<THttpCheckpointActor>
{
public:
    enum EAction
    {
        CreateCheckpoint,
        DeleteCheckpoint,
    };

private:
    const TRequestInfoPtr RequestInfo;

    const TActorId VolumeActorId;
    const ui64 TabletId;
    const TString CheckpointName;
    const EAction Action;

public:
    THttpCheckpointActor(
        TRequestInfoPtr requestInfo,
        const TActorId& volumeActordId,
        ui64 tabletId,
        TString checkpointName,
        EAction action);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        const TString& action,
        const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleCreateCheckpointResponse(
        const TEvService::TEvCreateCheckpointResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDeleteCheckpointResponse(
        const TEvService::TEvDeleteCheckpointResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleCreateCheckpointRequest(
        const TEvService::TEvCreateCheckpointRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleDeleteCheckpointRequest(
        const TEvService::TEvDeleteCheckpointRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

THttpCheckpointActor::THttpCheckpointActor(
        TRequestInfoPtr requestInfo,
        const TActorId& volumeActorId,
        ui64 tabletId,
        TString checkpointName,
        EAction action)
    : RequestInfo(std::move(requestInfo))
    , VolumeActorId(volumeActorId)
    , TabletId(tabletId)
    , CheckpointName(std::move(checkpointName))
    , Action(action)
{}

void THttpCheckpointActor::Bootstrap(const TActorContext& ctx)
{
    switch (Action) {
        case CreateCheckpoint: {
            auto request = std::make_unique<TEvService::TEvCreateCheckpointRequest>();
            request->Record.SetCheckpointId(CheckpointName);

            NCloud::SendWithUndeliveryTracking(ctx, VolumeActorId, std::move(request));
            break;
        }
        case DeleteCheckpoint: {
            auto request = std::make_unique<TEvService::TEvDeleteCheckpointRequest>();
            request->Record.SetCheckpointId(CheckpointName);

            NCloud::SendWithUndeliveryTracking(ctx, VolumeActorId, std::move(request));
            break;
        }
        default:
            Y_ABORT("Invalid action");
    }

    Become(&TThis::StateWork);
}

void THttpCheckpointActor::ReplyAndDie(
    const TActorContext& ctx,
    const TString& action,
    const NProto::TError& error)
{
    using namespace NMonitoringUtils;

    TStringStream msg;
    if (FAILED(error.GetCode())) {
        msg << "[" << TabletId << "] ";
        msg << "Cannot " << action << " checkpoint " << CheckpointName.Quote() << Endl;
        msg << "Operation completed with error : " << FormatError(error);
        LOG_ERROR_S(ctx, TBlockStoreComponents::PARTITION, msg.Str());
    } else {
        msg << "Operation successfully completed";
    }

    LWTRACK(
        ResponseSent_Volume,
        RequestInfo->CallContext->LWOrbit,
        "HttpInfo",
        RequestInfo->CallContext->RequestId);

    TStringStream out;
    BuildTabletNotifyPageWithRedirect(out, msg.Str(), TabletId);

    auto response = std::make_unique<NMon::TEvRemoteHttpInfoRes>(out.Str());
    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void THttpCheckpointActor::HandleCreateCheckpointResponse(
    const TEvService::TEvCreateCheckpointResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ReplyAndDie(ctx, "create", msg->GetError());
}

void THttpCheckpointActor::HandleDeleteCheckpointResponse(
    const TEvService::TEvDeleteCheckpointResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ReplyAndDie(ctx, "delete", msg->GetError());
}

void THttpCheckpointActor::HandleCreateCheckpointRequest(
    const TEvService::TEvCreateCheckpointRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, "create", MakeError(E_REJECTED, "tablet is shutting down"));
}

void THttpCheckpointActor::HandleDeleteCheckpointRequest(
    const TEvService::TEvDeleteCheckpointRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, "delete", MakeError(E_REJECTED, "tablet is shutting down"));
}

void THttpCheckpointActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, Action == CreateCheckpoint ? "create" : "delete", MakeError(E_REJECTED, "tablet is shutting down"));
}

STFUNC(THttpCheckpointActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvCreateCheckpointResponse, HandleCreateCheckpointResponse);
        HFunc(TEvService::TEvDeleteCheckpointResponse, HandleDeleteCheckpointResponse);

        HFunc(TEvService::TEvCreateCheckpointRequest, HandleCreateCheckpointRequest);
        HFunc(TEvService::TEvDeleteCheckpointRequest, HandleDeleteCheckpointRequest);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleHttpInfo_CreateCheckpoint(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    const auto checkpointId = params.Get("checkpointid");

    if (!checkpointId) {
        RejectHttpRequest(
            ctx,
            *requestInfo,
            "No checkpoint id is given");
        return;
    }

    NCloud::Register<THttpCheckpointActor>(
        ctx,
        std::move(requestInfo),
        SelfId(),
        TabletID(),
        std::move(checkpointId),
        THttpCheckpointActor::CreateCheckpoint);
}

void TVolumeActor::HandleHttpInfo_DeleteCheckpoint(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    const auto checkpointId = params.Get("checkpointid");

    if (!checkpointId) {
        RejectHttpRequest(
            ctx,
            *requestInfo,
            "No checkpoint id is given");
        return;
    }

    NCloud::Register<THttpCheckpointActor>(
        ctx,
        std::move(requestInfo),
        SelfId(),
        TabletID(),
        std::move(checkpointId),
        THttpCheckpointActor::DeleteCheckpoint);
}

}   // namespace NCloud::NBlockStore::NStorage
