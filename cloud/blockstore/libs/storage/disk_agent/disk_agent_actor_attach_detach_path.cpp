#include "disk_agent_actor.h"

#include <util/string/join.h>

#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <algorithm>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

NProto::TError TDiskAgentActor::IsAttachDetachPathsAvailable() const
{
    if (!Config->GetAttachDetachPathsEnabled() || Spdk) {
        // DR should handle errors with E_PRECONDITION_FAILED code.
        return MakeError(
            E_PRECONDITION_FAILED,
            "attach/detach paths is disabled");
    }

    return {};
}

NProto::TError TDiskAgentActor::UpdateControlPlaneRequestNumber(
    TControlPlaneRequestNumber controlPlaneRequestNumber)
{
    if (controlPlaneRequestNumber <= ControlPlaneRequestNumber) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "outdated control plane requesst: "
                             << ControlPlaneRequestNumber.Generation << ":"
                             << ControlPlaneRequestNumber.RequestNumber
                             << " vs " << controlPlaneRequestNumber.Generation
                             << ":" << controlPlaneRequestNumber.RequestNumber);
    }

    if (PendingControlPlaneRequest) {
        return MakeError(E_REJECTED, "another request is in progress");
    }

    ControlPlaneRequestNumber = controlPlaneRequestNumber;

    return {};
}

auto TDiskAgentActor::SplitPaths(TVector<TString> paths) const
    -> std::pair<TVector<TString>, TVector<TString>>
{
    THashSet<TString> knownPaths;

    for (const auto& config: State->GetDevices()) {
        knownPaths.insert(config.GetDeviceName());
    }

    std::ranges::remove_if(
        paths,
        [&](const auto& p) { return !knownPaths.contains(p); });

    auto [it, end] = std::ranges::partition(
        paths,
        [&](const auto& p) { return State->IsPathAttached(p); });

    TVector<TString> detached{
        std::make_move_iterator(it),
        std::make_move_iterator(end)};

    paths.erase(it, end);

    return {std::move(paths), std::move(detached)};
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

    if (auto error = IsAttachDetachPathsAvailable(); HasError(error)) {
        reply(error);
        return;
    }

    const TControlPlaneRequestNumber controlPlaneRequestNumber{
        .Generation = record.GetDiskRegistryGeneration(),
        .RequestNumber = record.GetDiskAgentGeneration()};

    if (auto error = UpdateControlPlaneRequestNumber(controlPlaneRequestNumber);
        HasError(error))
    {
        reply(error);
        return;
    }

    auto [pathsToDetach, alreadyDetachedPaths] = SplitPaths(
        {record.GetPathsToDetach().begin(), record.GetPathsToDetach().end()});

    if (!pathsToDetach) {
        reply({});
        return;
    }

    PendingControlPlaneRequest =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    auto future = State->DetachPaths(pathsToDetach);

    future.Subscribe(
        [actorSystem = TActivationContext::ActorSystem(),
         selfID = ctx.SelfID,
         pathsToDetach = std::move(pathsToDetach),
         alreadyDetachedPaths = std::move(alreadyDetachedPaths),
         controlPlaneRequestNumber](auto) mutable
        {
            auto response =
                std::make_unique<TEvDiskAgentPrivate::TEvPathsDetached>();
            response->PathsToDetach = std::move(pathsToDetach);
            response->AlreadyDetachedPaths = std::move(alreadyDetachedPaths);
            response->ControlPlaneRequestNumber = controlPlaneRequestNumber;

            actorSystem->Send(new IEventHandle{selfID, selfID, response.release()});
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
        PendingControlPlaneRequest.Reset();
    };

    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Failed to detach paths [%s] with DR Generation: %lu DA "
            "Generation: %lu: %s",
            JoinSeq(",", pathsToDetach).c_str(),
            ev->Get()->ControlPlaneRequestNumber.Generation,
            ev->Get()->ControlPlaneRequestNumber.RequestNumber,
            FormatError(error).c_str());
    } else {
        RestartDeviceHealthChecking(ctx);
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Detached paths [%s] with DR Generation: %lu DA Generation: %lu "
            "Already detached paths [%s]",
            JoinSeq(",", pathsToDetach).c_str(),
            ev->Get()->ControlPlaneRequestNumber.Generation,
            ev->Get()->ControlPlaneRequestNumber.RequestNumber,
            JoinSeq(",", ev->Get()->AlreadyDetachedPaths).c_str());
    }

    auto response =
        std::make_unique<TEvDiskAgent::TEvDetachPathsResponse>(error);
    NCloud::Reply(ctx, *PendingControlPlaneRequest, std::move(response));
}

void TDiskAgentActor::HandleAttachPaths(
    const TEvDiskAgent::TEvAttachPathsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& record = msg->Record;

    auto reply = [&](const NProto::TError& error)
    {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvAttachPathsResponse>(error));
    };

    if (auto error = IsAttachDetachPathsAvailable(); HasError(error)) {
        reply(error);
        return;
    }

    const TControlPlaneRequestNumber controlPlaneRequestNumber{
        .Generation = record.GetDiskRegistryGeneration(),
        .RequestNumber = record.GetDiskAgentGeneration()};

    if (auto error = UpdateControlPlaneRequestNumber(controlPlaneRequestNumber);
        HasError(error))
    {
        reply(error);
        return;
    }

    auto [alreadyAttachedPaths, pathsToAttach] = SplitPaths(
        {record.GetPathsToAttach().begin(), record.GetPathsToAttach().end()});

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

    PendingControlPlaneRequest =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    auto future = State->PreparePaths(pathsToAttach);

    future.Subscribe(
        [actorSystem = TActivationContext::ActorSystem(),
         selfId = ctx.SelfID,
         pathsToAttach = std::move(pathsToAttach),
         alreadyAttachedPaths = std::move(alreadyAttachedPaths),
         controlPlaneRequestNumber](
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

            response->ControlPlaneRequestNumber = controlPlaneRequestNumber;

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
        PendingControlPlaneRequest.Reset();
    };

    if (HasError(msg->Error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Failed to attach paths[%s] with DR Generation %lu and DA "
            "Generation %lu: %s",
            JoinSeq(",", msg->PathsToAttach).c_str(),
            msg->ControlPlaneRequestNumber.Generation,
            msg->ControlPlaneRequestNumber.RequestNumber,
            FormatError(msg->Error).c_str());

        auto response =
            std::make_unique<TEvDiskAgent::TEvAttachPathsResponse>(msg->Error);
        NCloud::Reply(ctx, *PendingControlPlaneRequest, std::move(response));
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
        msg->ControlPlaneRequestNumber.Generation,
        msg->ControlPlaneRequestNumber.RequestNumber,
        JoinSeq(",", msg->AlreadyAttachedPaths).c_str());

    NCloud::Reply(ctx, *PendingControlPlaneRequest, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
