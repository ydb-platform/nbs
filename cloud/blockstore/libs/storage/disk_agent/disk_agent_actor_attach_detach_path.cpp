#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/storage/core/request_info.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NThreading;

void TDiskAgentActor::HandleAttachPath(
    const TEvDiskAgent::TEvAttachPathRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& record = msg->Record;

    if (PendingAttachPathRequest) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvAttachPathResponse>(
                MakeError(E_REJECTED, "another request is in progress")));
        return;
    }

    TPendingAttachRequest request;
    request.RequestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    TVector<TString> paths;

    for (const auto& pathToGeneration: record.GetDisksToAttach()) {
        auto error = State->CheckCanAttachPath(
            record.GetDiskRegistryGeneration(),
            pathToGeneration.GetDiskPath(),
            pathToGeneration.GetGeneration());

        if (HasError(error)) {
            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvDiskAgent::TEvAttachPathResponse>(error));
            return;
        }

        request.PathToGeneration[pathToGeneration.GetDiskPath()] =
            pathToGeneration.GetGeneration();
        paths.emplace_back(pathToGeneration.GetDiskPath());
    }

    PendingAttachPathRequest = std::move(request);

    CheckIsSamePath(ctx, std::move(paths));
}

void TDiskAgentActor::HandlePathAttached(
    const TEvDiskAgentPrivate::TEvPathAttached::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    State->PathAttached(
        std::move(msg->Devices),
        PendingAttachPathRequest->PathToGeneration);

    THashSet<TString> paths;
    for (const auto& [path, _]:
         PendingAttachPathRequest->PathToGeneration)
    {
        paths.emplace(path);
    }

    auto deviceConfigs = State->GetAllDevicesForPaths(paths);

    auto response = std::make_unique<TEvDiskAgent::TEvAttachPathResponse>();
    for (auto& deviceConfig: deviceConfigs) {
        *response->Record.AddAttachedDevices() = std::move(deviceConfig);
    }

    NCloud::Reply(
        ctx,
        *PendingAttachPathRequest->RequestInfo,
        std::move(response));
    PendingAttachPathRequest.reset();
}

void TDiskAgentActor::HandleDetachPath(
    const TEvDiskAgent::TEvDetachPathRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& record = msg->Record;

    if (PendingAttachPathRequest) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvAttachPathResponse>(
                MakeError(E_REJECTED, "another request is in progress")));
        return;
    }

    THashMap<TString, ui64> pathToGeneration;
    for (const auto& diskToDetach: record.GetDisksToDetach()) {
        pathToGeneration[diskToDetach.GetDiskPath()] =
            diskToDetach.GetGeneration();
    }

    auto response =
        std::make_unique<TEvDiskAgent::TEvDetachPathResponse>(State->DetachPath(
            record.GetDiskRegistryGeneration(),
            pathToGeneration));
    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
