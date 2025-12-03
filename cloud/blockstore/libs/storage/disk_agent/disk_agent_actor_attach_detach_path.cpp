#include "disk_agent_actor.h"

#include <util/string/join.h>

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
        RestartDeviceHealthChecking(ctx);
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

void TDiskAgentActor::HandleAttachPaths(
    const TEvDiskAgent::TEvAttachPathsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& record = msg->Record;

    if (!Config->GetAttachDetachPathsEnabled() || Spdk) {
        // DR should handle errors with E_PRECONDITION_FAILED code.
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvAttachPathsResponse>(MakeError(
                E_PRECONDITION_FAILED,
                "attach/detach paths is disabled")));
        return;
    }

    if (PendingAttachDetachPathsRequest) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvAttachPathsResponse>(
                MakeError(E_REJECTED, "another request is in progress")));
        return;
    }

    PendingAttachDetachPathsRequest =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    TVector<TString> pathsToAttach{
        record.GetPathsToAttach().begin(),
        record.GetPathsToAttach().end()};

    auto future = State->AttachPaths(pathsToAttach);

    auto* actorSystem = TActivationContext::ActorSystem();
    auto selfId = ctx.SelfID;

    future.Subscribe(
        [actorSystem, selfId](
            TFuture<TResultOrError<TDiskAgentState::TAttachPathResult>>
                future) mutable
        {
            auto [result, error] = future.ExtractValue();

            auto response =
                std::make_unique<TEvDiskAgentPrivate::TEvPathsAttached>(error);
            response->AlreadyAttachedPaths =
                std::move(result.AlreadyAttachedPaths);
            response->PathsToAttach = std::move(result.PathsToAttach);
            response->Devices = std::move(result.Devices);
            response->Stats = std::move(result.Stats);
            response->Configs = std::move(result.Configs);

            actorSystem->Send(
                new IEventHandle{selfId, selfId, response.release()});
        });
}

void TDiskAgentActor::HandlePathsAttached(
    const TEvDiskAgentPrivate::TEvPathsAttached::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    Y_DEFER
    {
        PendingAttachDetachPathsRequest.Reset();
    };

    if (HasError(msg->Error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Failed to attach paths[%s]: %s",
            JoinSeq(",", msg->PathsToAttach).c_str(),
            FormatError(msg->Error).c_str());

        auto response =
            std::make_unique<TEvDiskAgent::TEvAttachPathsResponse>(msg->Error);
        NCloud::Reply(ctx, *PendingAttachDetachPathsRequest, std::move(response));
        return;
    }

    State->PathsAttached(
        std::move(msg->Configs),
        std::move(msg->Devices),
        std::move(msg->Stats),
        msg->PathsToAttach);

    THashSet<TString> paths{
        msg->PathsToAttach.begin(),
        msg->PathsToAttach.end()};
    paths.insert(
        msg->AlreadyAttachedPaths.begin(),
        msg->AlreadyAttachedPaths.end());

    auto deviceConfigs = State->GetDevicesByPath(paths);

    auto response = std::make_unique<TEvDiskAgent::TEvAttachPathsResponse>();
    for (auto& deviceConfig: deviceConfigs) {
        *response->Record.AddAttachedDevices() = std::move(deviceConfig);
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Attached paths [%s] with Disk Agent Generation %lu Already attached "
        "paths [%s]",
        JoinSeq(",", msg->PathsToAttach).c_str(),
        msg->DiskAgentGeneration,
        JoinSeq(",", msg->AlreadyAttachedPaths).c_str());

    NCloud::Reply(ctx, *PendingAttachDetachPathsRequest, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
