#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <util/string/join.h>

#include <algorithm>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString DescribeResponse(
    const TString& name,
    const TVector<TString>& attachedPaths,
    const TVector<TString>& detachedPaths,
    TEvDiskAgentPrivate::TControlPlaneRequestNumber requestNumber,
    const NProto::TError& error)
{
    TStringBuilder builder;
    builder << name << " request #" << requestNumber << " attached paths: ["
            << JoinSeq(", ", attachedPaths) << "] "
            << " detached paths: [" << JoinSeq(", ", detachedPaths) << "] ";

    if (HasError(error)) {
        builder << "failed: " << FormatError(error);
    } else {
        builder << "completed successfully";
    }

    return builder;
}

TString DescribeResponse(const TEvDiskAgentPrivate::TEvPathsPrepared& msg)

{
    return DescribeResponse(
        "AttachPaths",
        msg.AlreadyAttachedPaths,
        msg.PathsToAttach,
        msg.ControlPlaneRequestNumber,
        msg.Error);
}

TString DescribeResponse(const TEvDiskAgentPrivate::TEvPathsDetached& msg)
{
    return DescribeResponse(
        "DetachPaths",
        msg.PathsToDetach,
        msg.AlreadyDetachedPaths,
        msg.ControlPlaneRequestNumber,
        msg.Error);
}

std::unique_ptr<TEvDiskAgentPrivate::TEvPathsDetached>
MakePathsDetachedResponse(
    NProto::TError error,
    TVector<TString> pathsToDetach,
    TVector<TString> alreadyDetachedPaths,
    TEvDiskAgentPrivate::TControlPlaneRequestNumber controlPlaneRequestNumber)
{
    auto response = std::make_unique<TEvDiskAgentPrivate::TEvPathsDetached>(
        std::move(error));
    response->PathsToDetach = std::move(pathsToDetach);
    response->AlreadyDetachedPaths = std::move(alreadyDetachedPaths);
    response->ControlPlaneRequestNumber = controlPlaneRequestNumber;

    return response;
}

std::unique_ptr<TEvDiskAgentPrivate::TEvPathsPrepared>
MakePathsPreparedResponse(
    NProto::TError error,
    TVector<TString> pathsToAttach,
    TVector<TString> alreadyAttachedPaths,
    TEvDiskAgentPrivate::TControlPlaneRequestNumber controlPlaneRequestNumber,
    TDiskAgentState::TPreparePathsResult preparePathsResult)
{
    auto response = std::make_unique<TEvDiskAgentPrivate::TEvPathsPrepared>(
        std::move(error));
    response->PathsToAttach = std::move(pathsToAttach);
    response->AlreadyAttachedPaths = std::move(alreadyAttachedPaths);
    response->ControlPlaneRequestNumber = controlPlaneRequestNumber;

    response->Devices = std::move(preparePathsResult.Devices);
    response->Stats = std::move(preparePathsResult.Stats);
    response->Configs = std::move(preparePathsResult.Configs);

    return response;
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
                "outdated control plane request: %s vs %s",
                ToString(ControlPlaneRequestNumber).c_str(),
                ToString(controlPlaneRequestNumber).c_str()));
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
    auto [it, end] = std::ranges::partition(
        attachedPaths,
        [&](const auto& p) { return State->IsPathAttached(p); });
    TVector<TString> detachedPaths(
        std::make_move_iterator(it),
        std::make_move_iterator(end));
    attachedPaths.erase(it, end);

    return {std::move(attachedPaths), std::move(detachedPaths)};
}

void TDiskAgentActor::HandleDetachPaths(
    const TEvDiskAgent::TEvDetachPathsRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& record = msg->Record;

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Received DetachPaths request: " << record);

    if (auto error = CheckAttachDetachPathsAvailable(); HasError(error)) {
        auto response =
            std::make_unique<TEvDiskAgent::TEvDetachPathsResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    if (PendingControlPlaneRequest) {
        auto response = std::make_unique<TEvDiskAgent::TEvDetachPathsResponse>(
            MakeError(E_REJECTED, "another request is in progress"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    const TControlPlaneRequestNumber controlPlaneRequestNumber(
        record.GetControlPlaneRequestNumber());

    PendingControlPlaneRequest =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    auto [attachedPaths, detachedPaths] = SplitPaths(
        {record.GetPathsToDetach().begin(), record.GetPathsToDetach().end()});

    // We should ignore error if all paths already detached.
    if (auto error = UpdateControlPlaneRequestNumber(controlPlaneRequestNumber);
        HasError(error) && attachedPaths || !attachedPaths)
    {
        auto response = MakePathsDetachedResponse(
            attachedPaths ? std::move(error) : NProto::TError{},
            std::move(attachedPaths),
            std::move(detachedPaths),
            controlPlaneRequestNumber);

        NCloud::Send(ctx, SelfId(), std::move(response));
        return;
    }

    auto future = State->DetachPaths(attachedPaths);

    future.Subscribe(
        [actorSystem = TActivationContext::ActorSystem(),
         selfId = ctx.SelfID,
         pathsToDetach = std::move(attachedPaths),
         alreadyDetachedPaths = std::move(detachedPaths),
         controlPlaneRequestNumber](const auto& future) mutable
        {
            Y_UNUSED(future);

            auto response = MakePathsDetachedResponse(
                {},   // error
                std::move(pathsToDetach),
                std::move(alreadyDetachedPaths),
                controlPlaneRequestNumber);

            actorSystem->Send(
                new IEventHandle{selfId, selfId, response.release()});
        });
}

void TDiskAgentActor::HandlePathsDetached(
    const TEvDiskAgentPrivate::TEvPathsDetached::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& error = msg->Error;

    Y_DEFER
    {
        PendingControlPlaneRequest.Reset();
    };

    LOG_LOG(
        ctx,
        HasError(error) ? NLog::EPriority::PRI_ERROR
                        : NLog::EPriority::PRI_INFO,
        TBlockStoreComponents::DISK_AGENT,
        DescribeResponse(*msg));

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

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Received AttachPaths request: " << record);

    if (auto error = CheckAttachDetachPathsAvailable(); HasError(error)) {
        auto response =
            std::make_unique<TEvDiskAgent::TEvAttachPathsResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));

        return;
    }

    if (PendingControlPlaneRequest) {
        auto response = std::make_unique<TEvDiskAgent::TEvAttachPathsResponse>(
            MakeError(E_REJECTED, "another request is in progress"));

        NCloud::Reply(ctx, *ev, std::move(response));

        return;
    }

    PendingControlPlaneRequest =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    const TControlPlaneRequestNumber controlPlaneRequestNumber(
        record.GetControlPlaneRequestNumber());

    auto [attachedPaths, detachedPaths] = SplitPaths(
        {record.GetPathsToAttach().begin(), record.GetPathsToAttach().end()});

    // We should ignore error if all paths already attached.
    if (auto error = UpdateControlPlaneRequestNumber(controlPlaneRequestNumber);
        HasError(error) && detachedPaths || !detachedPaths)
    {
        auto response = MakePathsPreparedResponse(
            detachedPaths ? std::move(error) : NProto::TError{},
            std::move(detachedPaths),
            std::move(attachedPaths),
            controlPlaneRequestNumber,
            {});

        NCloud::Send(ctx, SelfId(), std::move(response));
        return;
    }

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

            auto response = MakePathsPreparedResponse(
                std::move(error),
                std::move(pathsToAttach),
                std::move(alreadyAttachedPaths),
                controlPlaneRequestNumber,
                std::move(result));

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

    LOG_LOG(
        ctx,
        HasError(msg->Error) ? NLog::EPriority::PRI_ERROR
                             : NLog::EPriority::PRI_INFO,
        TBlockStoreComponents::DISK_AGENT,
        DescribeResponse(*msg));

    if (HasError(msg->Error)) {
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

    NCloud::Reply(ctx, *PendingControlPlaneRequest, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
