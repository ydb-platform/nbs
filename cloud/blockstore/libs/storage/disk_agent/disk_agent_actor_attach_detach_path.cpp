#include "disk_agent_actor.h"

#include "util/string/join.h"

#include <cloud/blockstore/libs/storage/core/request_info.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::HandleDetachPaths(
    const TEvDiskAgent::TEvDetachPathsRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& record = msg->Record;

    if (!Config->GetAttachDetachPathsEnabled() || Spdk) {
        // DR should handle errors with E_PRECONDITION_FAILED code.
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvDetachPathsResponse>(MakeError(
                E_PRECONDITION_FAILED,
                "attach/detach paths is disabled")));
        return;
    }

    if (PendingAttachDetachPathsRequest) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvDetachPathsResponse>(
                MakeError(E_REJECTED, "another request is in progress")));
        return;
    }

    PendingAttachDetachPathsRequest =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    TVector<TString> pathsToDetach{
        record.GetPathsToDetach().begin(),
        record.GetPathsToDetach().end()};

    auto future = State->DetachPaths(pathsToDetach);

    auto* actorSystem = TActivationContext::ActorSystem();
    auto daId = ctx.SelfID;

    future.Subscribe(
        [actorSystem, daId, pathsToDetach = std::move(pathsToDetach)](
            auto) mutable
        {
            auto response =
                std::make_unique<TEvDiskAgentPrivate::TEvPathsDetached>();
            response->PathsToDetach = std::move(pathsToDetach);
            actorSystem->Send(new IEventHandle{daId, daId, response.release()});
        });
}

void TDiskAgentActor::HandlePathsDetached(
    const TEvDiskAgentPrivate::TEvPathsDetached::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto& error = ev->Get()->Error;
    auto pathsToDetach = std::move(ev->Get()->PathsToDetach);

    Y_DEFER
    {
        PendingAttachDetachPathsRequest.Reset();
    };

    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Failed to detach paths [%s]: %s",
            JoinSeq(",", pathsToDetach).c_str(),
            FormatError(error).c_str());
    } else {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Detached paths [%s]",
            JoinSeq(",", pathsToDetach).c_str());
    }

    auto response =
        std::make_unique<TEvDiskAgent::TEvDetachPathsResponse>(error);
    NCloud::Reply(ctx, *PendingAttachDetachPathsRequest, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
