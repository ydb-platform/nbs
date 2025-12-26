#include "disk_agent_actor.h"

#include <util/string/join.h>

#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <algorithm>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

auto TDiskAgentActor::CheckAttachDetachPathsRequest(
    ui64 diskRegistryGeneration,
    ui64 diskAgentGeneration,
    TVector<TString> paths,
    EAction action) -> TResultOrError<TCheckAttachDetachPathRequestResult>
{
    if (!Config->GetAttachDetachPathsEnabled() || Spdk) {
        // DR should handle errors with E_PRECONDITION_FAILED code.
        return MakeError(
            E_PRECONDITION_FAILED,
            "attach/detach paths is disabled");
    }

    if (PendingAttachDetachPathsRequest) {
        return MakeError(E_REJECTED, "another request is in progress");
    }

    THashSet<TString> allKnownPaths;
    for (const auto& device: State->GetDevices()) {
        allKnownPaths.insert(device.GetDeviceName());
    }

    // Filter from unknown paths.
    auto unknownPaths = std::ranges::partition(
        paths.begin(),
        paths.end(),
        [&](const auto& p) { return allKnownPaths.contains(p); });
    paths.erase(unknownPaths.begin(), unknownPaths.end());

    // Split on paths that are already in desired state and paths that we want
    // to perform attach/detach.
    TVector<TString> alreadyAttachedPaths = std::move(paths);
    auto pathsToPerformAttachDetachRange = std::ranges::partition(
        alreadyAttachedPaths,
        [&](const auto& p) { return State->IsPathAttached(p) == attach; });
    TVector<TString> pathsToPerformAttachDetach(
        std::make_move_iterator(pathsToPerformAttachDetachRange.begin()),
        std::make_move_iterator(pathsToPerformAttachDetachRange.end()));
    alreadyAttachedPaths.erase(
        pathsToPerformAttachDetachRange.begin(),
        pathsToPerformAttachDetachRange.end());

    if (auto error = State->CheckAttachDetachPathsRequestGeneration(
            diskRegistryGeneration,
            diskAgentGeneration);
        HasError(error) && pathsToPerformAttachDetach)
    {
        return error;
    }

    return TCheckAttachDetachPathRequestResult{
        .AlreadyInWantedStatePaths = alreadyAttachedPaths,
        .PathToPerformAttachDetach = pathsToPerformAttachDetach,
    };
}

auto TDiskAgentActor::CheckAttachPathsRequest(
    ui64 diskRegistryGeneration,
    ui64 diskAgentGeneration,
    TVector<TString> paths)
    -> TResultOrError<TCheckAttachDetachPathRequestResult>
{
    return CheckAttachDetachPathsRequest(
        diskRegistryGeneration,
        diskAgentGeneration,
        std::move(paths),
        EAction::Attach);
}

auto TDiskAgentActor::CheckDetachPathsRequest(
    ui64 diskRegistryGeneration,
    ui64 diskAgentGeneration,
    TVector<TString> paths)
    -> TResultOrError<TCheckAttachDetachPathRequestResult>
{
    return CheckAttachDetachPathsRequest(
        diskRegistryGeneration,
        diskAgentGeneration,
        std::move(paths),
        EAction::Detach);
}

void TDiskAgentActor::HandleDetachPaths(
    const TEvDiskAgent::TEvDetachPathsRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& record = msg->Record;

    auto reply = [&](const NProto::TError& error)
    {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvDetachPathsResponse>(error));
    };

    auto [checkRes, error] = CheckDetachPathsRequest(
        record.GetDiskRegistryGeneration(),
        record.GetDiskAgentGeneration(),
        {record.GetPathsToDetach().begin(), record.GetPathsToDetach().end()});

    if (HasError(error)) {
        reply(error);
        return;
    }

    auto [alreadyDetachedPaths, pathsToDetach] = std::move(checkRes);

    if (!pathsToDetach) {
        reply({});
        return;
    }

    PendingAttachDetachPathsRequest =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    auto future = State->DetachPaths(pathsToDetach);

    auto* actorSystem = TActivationContext::ActorSystem();
    auto daId = ctx.SelfID;

    future.Subscribe(
        [actorSystem,
         daId,
         pathsToDetach = std::move(pathsToDetach),
         alreadyDetachedPaths = std::move(alreadyDetachedPaths),
         diskRegistryGeneration = record.GetDiskRegistryGeneration(),
         diskAgentGeneration = record.GetDiskAgentGeneration()](auto) mutable
        {
            auto response =
                std::make_unique<TEvDiskAgentPrivate::TEvPathsDetached>();
            response->PathsToDetach = std::move(pathsToDetach);
            response->AlreadyDetachedPaths = std::move(alreadyDetachedPaths);
            response->DiskRegistryGeneration = diskRegistryGeneration;
            response->DiskAgentGeneration = diskAgentGeneration;

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
            "Failed to detach paths [%s] with DR Generation: %lu DA "
            "Generation: %lu: %s",
            JoinSeq(",", pathsToDetach).c_str(),
            ev->Get()->DiskRegistryGeneration,
            ev->Get()->DiskAgentGeneration,
            FormatError(error).c_str());
    } else {
        RestartDeviceHealthChecking(ctx);
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Detached paths [%s] with DR Generation: %lu DA Generation: %lu "
            "Already detached paths [%s]",
            JoinSeq(",", pathsToDetach).c_str(),
            ev->Get()->DiskRegistryGeneration,
            ev->Get()->DiskAgentGeneration,
            JoinSeq(",", ev->Get()->AlreadyDetachedPaths).c_str());
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

    auto [checkRes, error] = CheckAttachPathsRequest(
        record.GetDiskRegistryGeneration(),
        record.GetDiskAgentGeneration(),
        {record.GetPathsToAttach().begin(), record.GetPathsToAttach().end()});

    if (HasError(error)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvAttachPathsResponse>(
                std::move(error)));
        return;
    }

    auto [alreadyAttachedPaths, pathsToAttach] = std::move(checkRes);

    if (!pathsToAttach) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "No paths to attach. Attached paths: [%s], DR generation: %lu, DA "
            "generation: %lu",
            JoinSeq(",", alreadyAttachedPaths).c_str(),
            record.GetDiskRegistryGeneration(),
            record.GetDiskAgentGeneration());

        auto deviceConfigs = State->GetDevicesByPath(alreadyAttachedPaths);
        auto response =
            std::make_unique<TEvDiskAgent::TEvAttachPathsResponse>();
        response->Record.MutableAttachedDevices()->Assign(
            std::make_move_iterator(deviceConfigs.begin()),
            std::make_move_iterator(deviceConfigs.end()));
        NCloud::Reply(ctx, *ev, std::move(response));

        return;
    }

    PendingAttachDetachPathsRequest =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    auto future = State->PreparePaths(pathsToAttach);

    future.Subscribe(
        [actorSystem = TActivationContext::ActorSystem(),
         selfId = ctx.SelfID,
         pathsToAttach = std::move(pathsToAttach),
         alreadyAttachedPaths = std::move(alreadyAttachedPaths),
         diskRegistryGeneration = record.GetDiskRegistryGeneration(),
         diskAgentGeneration = record.GetDiskAgentGeneration()](
            TFuture<TResultOrError<TDiskAgentState::TPreparePathsResult>>
                future) mutable
        {
            auto [result, error] = future.ExtractValue();

            auto response =
                std::make_unique<TEvDiskAgentPrivate::TEvPathsPrepared>(error);
            response->AlreadyAttachedPaths = std::move(alreadyAttachedPaths);
            response->PathsToAttach = std::move(pathsToAttach);
            response->Devices = std::move(result.Devices);
            response->Stats = std::move(result.Stats);
            response->Configs = std::move(result.Configs);

            response->DiskRegistryGeneration = diskRegistryGeneration;
            response->DiskAgentGeneration = diskAgentGeneration;

            actorSystem->Send(
                new IEventHandle{selfId, selfId, response.release()});
        });
}

void TDiskAgentActor::HandlePathsPrepared(
    const TEvDiskAgentPrivate::TEvPathsPrepared::TPtr& ev,
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
            "Failed to attach paths[%s] with DR Generation %lu and DA "
            "Generation %lu: %s",
            JoinSeq(",", msg->PathsToAttach).c_str(),
            msg->DiskRegistryGeneration,
            msg->DiskAgentGeneration,
            FormatError(msg->Error).c_str());

        auto response =
            std::make_unique<TEvDiskAgent::TEvAttachPathsResponse>(msg->Error);
        NCloud::Reply(ctx, *PendingAttachDetachPathsRequest, std::move(response));
        return;
    }

    State->AttachPaths(
        std::move(msg->Configs),
        std::move(msg->Devices),
        std::move(msg->Stats));

    auto deviceConfigs = State->GetDevicesByPath(msg->PathsToAttach);

    std::ranges::move(
        State->GetDevicesByPath(msg->AlreadyAttachedPaths),
        std::back_inserter(deviceConfigs));

    auto response = std::make_unique<TEvDiskAgent::TEvAttachPathsResponse>();
    response->Record.MutableAttachedDevices()->Assign(
        std::make_move_iterator(deviceConfigs.begin()),
        std::make_move_iterator(deviceConfigs.end()));

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Attached paths [%s] with DR generation %lu and DA generation %lu "
        "Already attached "
        "paths [%s]",
        JoinSeq(",", msg->PathsToAttach).c_str(),
        msg->DiskRegistryGeneration,
        msg->DiskAgentGeneration,
        JoinSeq(",", msg->AlreadyAttachedPaths).c_str());

    NCloud::Reply(ctx, *PendingAttachDetachPathsRequest, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
