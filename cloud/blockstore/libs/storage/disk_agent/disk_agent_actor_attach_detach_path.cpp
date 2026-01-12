#include "disk_agent_actor.h"

#include <util/string/join.h>

#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <algorithm>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NThreading;

namespace {

TString DescribeRequest(
    TDiskAgentActor::EAction action,
    const TVector<TString>& attachedPaths,
    const TVector<TString>& detachedPaths,
    TEvDiskAgentPrivate::TControlPlaneRequestNumber controlPlaneRequestNumber)
{
    return Sprintf(
        "%s request, Attached paths [%s], Detached paths [%s], "
        "ControlPlaneRequestNumber [%s]",
        action == TDiskAgentActor::EAction::Attach ? "Attach" : "Detach",
        JoinSeq(",", attachedPaths).c_str(),
        JoinSeq(",", detachedPaths).c_str(),
        ToString(controlPlaneRequestNumber).c_str());
}

TString DescribeRequest(
    TDiskAgentActor::EAction action,
    const TVector<TString>& paths,
    TEvDiskAgentPrivate::TControlPlaneRequestNumber controlPlaneRequestNumber)
{
    return Sprintf(
        "%s request, Paths [%s], "
        "ControlPlaneRequestNumber [%s]",
        action == TDiskAgentActor::EAction::Attach ? "Attach" : "Detach",
        JoinSeq(",", paths).c_str(),
        ToString(controlPlaneRequestNumber).c_str());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TError TDiskAgentActor::CheckAttachDetachPathsAvailable() const
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
            Sprintf(
                "outdated control plane request: [%s] vs [%s]",
                ToString(ControlPlaneRequestNumber).c_str(),
                ToString(controlPlaneRequestNumber).c_str()));
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

    TVector<TString> attachedPaths = std::move(paths);
    auto detachedPathsRange = std::ranges::partition(
        attachedPaths,
        [&](const auto& p) { return State->IsPathAttached(p); });
    TVector<TString> detachedPaths(
        std::make_move_iterator(detachedPathsRange.begin()),
        std::make_move_iterator(detachedPathsRange.end()));
    attachedPaths.erase(detachedPathsRange.begin(), detachedPathsRange.end());

    return {std::move(attachedPaths), std::move(detachedPaths)};
}

auto TDiskAgentActor::CheckAttachDetachPathsRequest(
    TControlPlaneRequestNumber controlPlaneRequestNumber,
    TVector<TString> paths,
    EAction action)
    -> TResultOrError<std::pair<TVector<TString>, TVector<TString>>>
{
    if (auto error = CheckAttachDetachPathsAvailable(); HasError(error)) {
        return error;
    }

    auto [attachedPaths, detachedPaths] = SplitPaths(std::move(paths));

    auto& pathsToSwitchState =
        action == EAction::Attach ? detachedPaths : attachedPaths;

    if (auto error = UpdateControlPlaneRequestNumber(controlPlaneRequestNumber);
        HasError(error) && pathsToSwitchState)
    {
        return error;
    }

    return std::make_pair(std::move(attachedPaths), std::move(detachedPaths));
}

void TDiskAgentActor::HandleDetachPaths(
    const TEvDiskAgent::TEvDetachPathsRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& record = msg->Record;

    const auto controlPlaneRequestNumber = TControlPlaneRequestNumber{
        .Generation =
            record.GetControlPlaneRequestNumber().GetDiskRegistryGeneration(),
        .RequestNumber =
            record.GetControlPlaneRequestNumber().GetDiskAgentGeneration(),
    };

    auto reply = [&](const NProto::TError& error)
    {
        bool hasError = HasError(error);
        TString errorDescription = hasError ? " " + FormatError(error) : "";
        LOG_LOG(
            ctx,
            hasError ? NLog::EPriority::PRI_ERROR : NLog::EPriority::PRI_INFO,
            TBlockStoreComponents::DISK_AGENT,
            "%s: %s%s",
            hasError ? "Failed request" : "Successful request",
            DescribeRequest(
                EAction::Detach,
                {record.GetPathsToDetach().begin(),
                 record.GetPathsToDetach().end()},
                controlPlaneRequestNumber)
                .c_str(),
            errorDescription.c_str());

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvDetachPathsResponse>(error));
    };

    auto [checkRes, error] = CheckAttachDetachPathsRequest(
        controlPlaneRequestNumber,
        {record.GetPathsToDetach().begin(), record.GetPathsToDetach().end()},
        EAction::Detach);

    if (HasError(error)) {
        reply(error);
        return;
    }

    auto [attachedPaths, detachedPaths] = std::move(checkRes);

    if (!attachedPaths) {
        reply({});
        return;
    }

    PendingControlPlaneRequest =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    auto future = State->DetachPaths(attachedPaths);

    future.Subscribe(
        [actorSystem = TActivationContext::ActorSystem(),
         selfId = ctx.SelfID,
         pathsToDetach = std::move(attachedPaths),
         alreadyDetachedPaths = std::move(detachedPaths),
         controlPlaneRequestNumber](auto) mutable
        {
            auto response =
                std::make_unique<TEvDiskAgentPrivate::TEvPathsDetached>();
            response->PathsToDetach = std::move(pathsToDetach);
            response->AlreadyDetachedPaths = std::move(alreadyDetachedPaths);
            response->ControlPlaneRequestNumber = controlPlaneRequestNumber;

            actorSystem->Send(
                new IEventHandle{selfId, selfId, response.release()});
        });
}

void TDiskAgentActor::HandlePathsDetached(
    const TEvDiskAgentPrivate::TEvPathsDetached::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    const auto& error = ev->Get()->Error;
    auto pathsToDetach = std::move(ev->Get()->PathsToDetach);
    auto alreadyDetachedPaths = std::move(ev->Get()->AlreadyDetachedPaths);

    Y_DEFER
    {
        PendingControlPlaneRequest.Reset();
    };

    auto requestDescription = DescribeRequest(
        EAction::Detach,
        pathsToDetach,
        alreadyDetachedPaths,
        ev->Get()->ControlPlaneRequestNumber);

    bool hasError = HasError(error);
    TString errorDescription = hasError ? FormatError(error) : "";
    LOG_LOG(
        ctx,
        hasError ? NLog::EPriority::PRI_ERROR : NLog::EPriority::PRI_INFO,
        TBlockStoreComponents::DISK_AGENT,
        "%s: %s%s",
        hasError ? "Failed request" : "Successful request",
        requestDescription.c_str(),
        errorDescription.c_str());

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

    const auto controlPlaneRequestNumber = TControlPlaneRequestNumber{
        .Generation =
            record.GetControlPlaneRequestNumber().GetDiskRegistryGeneration(),
        .RequestNumber =
            record.GetControlPlaneRequestNumber().GetDiskAgentGeneration(),
    };

    auto [checkRes, error] = CheckAttachDetachPathsRequest(
        controlPlaneRequestNumber,
        {record.GetPathsToAttach().begin(), record.GetPathsToAttach().end()},
        EAction::Attach);

    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Failed request: %s: %s",
            DescribeRequest(
                EAction::Attach,
                {record.GetPathsToAttach().begin(),
                 record.GetPathsToAttach().end()},
                controlPlaneRequestNumber)
                .c_str(),
            FormatError(error).c_str());

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvAttachPathsResponse>(
                std::move(error)));
        return;
    }

    auto [attachedPaths, detachedPaths] = std::move(checkRes);

    if (!detachedPaths) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Successful request: %s",
            DescribeRequest(
                EAction::Attach,
                attachedPaths,
                {},
                controlPlaneRequestNumber)
                .c_str());

        auto deviceConfigs = State->GetDevicesByPath(attachedPaths);
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

    auto future = State->PreparePaths(detachedPaths);

    future.Subscribe(
        [actorSystem = TActivationContext::ActorSystem(),
         selfId = ctx.SelfID,
         pathsToAttach = std::move(detachedPaths),
         alreadyAttachedPaths = std::move(attachedPaths),
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
    auto pathsToAttach = std::move(msg->PathsToAttach);
    auto alreadyAttachedPaths = std::move(msg->AlreadyAttachedPaths);

    Y_DEFER
    {
        PendingControlPlaneRequest.Reset();
    };

    const auto requestDescription = DescribeRequest(
        EAction::Attach,
        alreadyAttachedPaths,
        pathsToAttach,
        msg->ControlPlaneRequestNumber);

    if (HasError(msg->Error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Failed request: %s: %s",
            requestDescription.c_str(),
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
        "Successful request: %s",
        requestDescription.c_str());

    NCloud::Reply(ctx, *PendingControlPlaneRequest, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
