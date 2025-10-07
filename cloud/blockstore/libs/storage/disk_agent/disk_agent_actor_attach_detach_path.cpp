#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/storage/core/request_info.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString DescribePaths(const THashMap<TString, ui64>& pathToGeneration)
{
    TStringBuilder sb;
    for (const auto& [path, generation]: pathToGeneration) {
        sb << path << ":" << generation << " ";
    }
    return sb;
}

}   // namespace

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

    for (const auto& pathToGeneration: record.GetPathsToAttach()) {
        auto error = State->CheckCanAttachPath(
            record.GetDiskRegistryGeneration(),
            pathToGeneration.GetPath(),
            pathToGeneration.GetGeneration());

        if (HasError(error) && error.GetCode() != E_NOT_FOUND) {
            LOG_ERROR(
                ctx,
                TBlockStoreComponents::DISK_AGENT,
                "Can't attach path %s with generation %lu: %s",
                pathToGeneration.GetPath().c_str(),
                pathToGeneration.GetGeneration(),
                FormatError(error).c_str());
            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvDiskAgent::TEvAttachPathResponse>(error));
            return;
        }

        if (error.GetCode() == S_ALREADY || error.GetCode() == E_NOT_FOUND) {
            request
                .AlreadyAttachedPathsToGeneration[pathToGeneration.GetPath()] =
                pathToGeneration.GetGeneration();
        } else {
            request.PathToGenerationToAttach[pathToGeneration.GetPath()] =
                pathToGeneration.GetGeneration();
            paths.emplace_back(pathToGeneration.GetPath());
        }
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
        PendingAttachPathRequest->PathToGenerationToAttach,
        PendingAttachPathRequest->AlreadyAttachedPathsToGeneration);

    THashSet<TString> paths;
    for (const auto& [path, _]:
         PendingAttachPathRequest->PathToGenerationToAttach)
    {
        paths.emplace(path);
    }
    for (const auto& [path, _]:
         PendingAttachPathRequest->AlreadyAttachedPathsToGeneration)
    {
        paths.emplace(path);
    }

    auto deviceConfigs = State->GetAllDevicesForPaths(paths);

    auto response = std::make_unique<TEvDiskAgent::TEvAttachPathResponse>();
    for (auto& deviceConfig: deviceConfigs) {
        *response->Record.AddAttachedDevices() = std::move(deviceConfig);
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_AGENT,
        "Attached paths [%s] Already attached paths [%s]",
        DescribePaths(PendingAttachPathRequest->PathToGenerationToAttach)
            .c_str(),
        DescribePaths(
            PendingAttachPathRequest->AlreadyAttachedPathsToGeneration)
            .c_str());

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

    if (!Config->GetAttachDetachPathsEnabled() || Spdk) {
        // DR should handle errors with E_PRECONDITION_FAILED code.
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvDetachPathResponse>(
                MakeError(E_PRECONDITION_FAILED, "attach/detach paths is disabled")));
        return;
    }

    if (PendingAttachPathRequest) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskAgent::TEvDetachPathResponse>(
                MakeError(E_REJECTED, "another request is in progress")));
        return;
    }

    THashMap<TString, ui64> pathToGeneration;
    for (const auto& diskToDetach: record.GetPathsToDetach()) {
        pathToGeneration[diskToDetach.GetPath()] =
            diskToDetach.GetGeneration();
    }

    auto error =
        State->DetachPath(record.GetDiskRegistryGeneration(), pathToGeneration);

    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Failed to detach paths [%s]: %s",
            DescribePaths(pathToGeneration).c_str(),
            FormatError(error).c_str());
    } else {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "Detached paths [%s]",
            DescribePaths(pathToGeneration).c_str());
    }

    auto response =
        std::make_unique<TEvDiskAgent::TEvDetachPathResponse>(error);
    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
