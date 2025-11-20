#include "disk_agent_actor.h"

#include "util/string/join.h"

#include <cloud/blockstore/libs/storage/core/request_info.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::HandleAttachPath(
    const TEvDiskAgent::TEvAttachPathRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& record = msg->Record;

    if (!Config->GetAttachDetachPathsEnabled() || Spdk) {
        // DR should handle errors with E_PRECONDITION_FAILED code.
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvAttachPathResponse>(MakeError(
                E_PRECONDITION_FAILED,
                "attach/detach paths is disabled")));
        return;
    }

    if (PendingAttachDetachPathRequest) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvAttachPathResponse>(
                MakeError(E_REJECTED, "another request is in progress")));
        return;
    }

    PendingAttachDetachPathRequest =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    TVector<TString> pathsToAttach{
        record.GetPathsToAttach().begin(),
        record.GetPathsToAttach().end()};

    auto future = State->AttachPath(
        record.GetDiskRegistryGeneration(),
        record.GetDiskAgentGeneration(),
        pathsToAttach);

    auto* actorSystem = TActivationContext::ActorSystem();
    auto daId = ctx.SelfID;

    future.Subscribe(
        [diskAgentGeneration = record.GetDiskAgentGeneration(),
         pathsToAttach = std::move(pathsToAttach),
         actorSystem,
         daId](
            TFuture<TResultOrError<TDiskAgentState::TAttachPathResult>>
                future) mutable
        {
            auto [result, error] = future.ExtractValue();

            auto response =
                std::make_unique<TEvDiskAgentPrivate::TEvPathAttached>(error);
            response->AlreadyAttachedPaths =
                std::move(result.AlreadyAttachedPaths);
            response->PathsToAttach = std::move(result.PathsToAttach);
            response->Devices = std::move(result.Devices);
            response->Stats = std::move(result.Stats);
            response->Configs = std::move(result.Configs);
            response->DiskAgentGeneration = diskAgentGeneration;

            actorSystem->Send(new IEventHandle{daId, daId, response.release()});
        });
}

void TDiskAgentActor::HandlePathAttached(
    const TEvDiskAgentPrivate::TEvPathAttached::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    Y_DEFER
    {
        PendingAttachDetachPathRequest.Reset();
    };

    if (HasError(msg->Error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Failed to attach paths[%s]: %s",
            JoinSeq(",", msg->PathsToAttach).c_str(),
            FormatError(msg->Error).c_str());

        auto response =
            std::make_unique<TEvDiskAgent::TEvAttachPathResponse>(msg->Error);
        NCloud::Reply(ctx, *PendingAttachDetachPathRequest, std::move(response));
        return;
    }

    State->PathAttached(
        msg->DiskAgentGeneration,
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

    auto deviceConfigs = State->GetAllDevicesForPaths(paths);

    auto response = std::make_unique<TEvDiskAgent::TEvAttachPathResponse>();
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

    NCloud::Reply(ctx, *PendingAttachDetachPathRequest, std::move(response));
}

void TDiskAgentActor::HandleDetachPath(
    const TEvDiskAgent::TEvDetachPathRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& record = msg->Record;

    if (!Config->GetAttachDetachPathsEnabled() || Spdk) {
        // DR should handle errors with E_PRECONDITION_FAILED code.
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvDetachPathResponse>(MakeError(
                E_PRECONDITION_FAILED,
                "attach/detach paths is disabled")));
        return;
    }

    if (PendingAttachDetachPathRequest) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvDetachPathResponse>(
                MakeError(E_REJECTED, "another request is in progress")));
        return;
    }

    PendingAttachDetachPathRequest =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    TVector<TString> pathsToDetach{
        record.GetPathsToDetach().begin(),
        record.GetPathsToDetach().end()};

    auto future = State->DetachPath(
        record.GetDiskRegistryGeneration(),
        record.GetDiskAgentGeneration(),
        pathsToDetach);

    auto* actorSystem = TActivationContext::ActorSystem();
    auto daId = ctx.SelfID;

    future.Subscribe(
        [actorSystem, daId, pathsToDetach = std::move(pathsToDetach)](
            TFuture<NProto::TError> future)
        {
            auto response =
                std::make_unique<TEvDiskAgentPrivate::TEvPathDetached>(
                    future.ExtractValue());
            actorSystem->Send(new IEventHandle{daId, daId, response.release()});
        });
}

void TDiskAgentActor::HandlePathDetached(
    const TEvDiskAgentPrivate::TEvPathDetached::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto& error = ev->Get()->Error;
    auto pathsToDetach = std::move(ev->Get()->PathsToDetach);

    Y_DEFER
    {
        PendingAttachDetachPathRequest.Reset();
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
        std::make_unique<TEvDiskAgent::TEvDetachPathResponse>(error);
    NCloud::Reply(ctx, *PendingAttachDetachPathRequest, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
