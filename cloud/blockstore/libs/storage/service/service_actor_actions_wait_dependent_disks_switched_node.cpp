#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/json/json_reader.h>

#include <google/protobuf/util/json_util.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using NProto::TWaitDependentDisksToSwitchNodeResponse;
using EDiskState = TWaitDependentDisksToSwitchNodeResponse::EDiskState;
using TDependentDiskState =
    TWaitDependentDisksToSwitchNodeResponse::TDependentDiskState;

namespace {

///////////////////////////////////////////////////////////////////////////////

bool VolumeUsesNode(const NProto::TVolume& volume, ui32 nodeId)
{
    auto containsNodeIdPred = [nodeId](const NProto::TDevice& device)
    {
        return device.GetNodeId() == nodeId;
    };

    bool foundNodeId = AnyOf(volume.GetDevices(), containsNodeIdPred);
    if (foundNodeId) {
        return true;
    }

    for (const auto& replica: volume.GetReplicas()) {
        foundNodeId = AnyOf(replica.GetDevices(), containsNodeIdPred);
        if (foundNodeId) {
            return true;
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

class TWaitDependentDisksToSwitchNodeActor final
    : public TActorBootstrapped<TWaitDependentDisksToSwitchNodeActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;
    const TDuration RetryTimeout;

    NProto::TWaitDependentDisksToSwitchNodeRequest Request;

    THashMap<ui64, TDependentDiskState> DependentDiskStates;

public:
    TWaitDependentDisksToSwitchNodeActor(
        TRequestInfoPtr requestInfo,
        TString input,
        TDuration retryTimeout);

    void Bootstrap(const TActorContext& ctx);

private:
    void HandleSuccess(const TActorContext& ctx);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);
    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvExecuteActionResponse> response);

    void ScheduleGetVolumeInfoRequest(const TActorContext& ctx, ui64 cookie);

    void CheckAllVolumesAreSwitched(const TActorContext& ctx);
    void CheckAllVolumesAreReady(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleGetDependentDisksResponse(
        const TEvDiskRegistry::TEvGetDependentDisksResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetVolumeInfoResponse(
        const TEvVolume::TEvGetVolumeInfoResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWaitReadyResponse(
        const TEvVolume::TEvWaitReadyResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TWaitDependentDisksToSwitchNodeActor::TWaitDependentDisksToSwitchNodeActor(
        TRequestInfoPtr requestInfo,
        TString input,
        TDuration retryTimeout)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
    , RetryTimeout(retryTimeout)
{}

void TWaitDependentDisksToSwitchNodeActor::Bootstrap(const TActorContext& ctx)
{
    if (!google::protobuf::util::JsonStringToMessage(Input, &Request).ok()) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    if (Request.GetAgentId().empty()) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Empty AgentId"));
        return;
    }

    if (Request.GetOldNodeId() == 0) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Empty OldNodeId"));
        return;
    }

    Become(&TThis::StateWork);

    auto request =
        std::make_unique<TEvDiskRegistry::TEvGetDependentDisksRequest>();
    request->Record.SetHost(Request.GetAgentId());
    NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));
}

void TWaitDependentDisksToSwitchNodeActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    ReplyAndDie(
        ctx,
        std::make_unique<TEvService::TEvExecuteActionResponse>(error));
}

void TWaitDependentDisksToSwitchNodeActor::HandleSuccess(
    const TActorContext& ctx)
{
    ReplyAndDie(ctx, std::make_unique<TEvService::TEvExecuteActionResponse>());
}

void TWaitDependentDisksToSwitchNodeActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvExecuteActionResponse> response)
{
    NProto::TWaitDependentDisksToSwitchNodeResponse result;
    for (auto& kv: DependentDiskStates) {
        result.MutableDependentDiskStates()->Add(std::move(kv.second));
    }
    google::protobuf::util::MessageToJsonString(
        result,
        response->Record.MutableOutput());
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TWaitDependentDisksToSwitchNodeActor::ScheduleGetVolumeInfoRequest(
    const TActorContext& ctx,
    ui64 cookie)
{
    STORAGE_CHECK_PRECONDITION(DependentDiskStates.contains(cookie));
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Schedule GetVolumeInfoRequest for volume: %s, timeout: %s",
        DependentDiskStates[cookie].GetDiskId().c_str(),
        RetryTimeout.ToString().c_str());

    auto request = std::make_unique<TEvVolume::TEvGetVolumeInfoRequest>();
    request->Record.SetDiskId(DependentDiskStates[cookie].GetDiskId());
    ctx.Schedule(
        RetryTimeout,
        std::make_unique<IEventHandle>(
            MakeVolumeProxyServiceId(),
            SelfId(),
            request.release(),
            TEventFlags{0},
            cookie));
}

void TWaitDependentDisksToSwitchNodeActor::CheckAllVolumesAreSwitched(
    const TActorContext& ctx)
{
    if (DependentDiskStates.empty()) {
        HandleSuccess(ctx);
        return;
    }

    const bool allVolumesAreSwitched = AllOf(
        DependentDiskStates,
        [](const auto& pr)
        {
            return pr.second.GetDiskState() >
                   TWaitDependentDisksToSwitchNodeResponse::DISK_STATE_INITIAL;
        });
    if (!allVolumesAreSwitched) {
        return;
    }

    for (const auto& pr: DependentDiskStates) {
        auto request = std::make_unique<TEvVolume::TEvWaitReadyRequest>();
        request->Record.SetDiskId(pr.second.GetDiskId());
        NCloud::Send(
            ctx,
            MakeVolumeProxyServiceId(),
            std::move(request),
            pr.first);
    }
}

void TWaitDependentDisksToSwitchNodeActor::CheckAllVolumesAreReady(
    const TActorContext& ctx)
{
    if (DependentDiskStates.empty()) {
        HandleSuccess(ctx);
        return;
    }

    const bool allVolumesAreReady = AllOf(
        DependentDiskStates,
        [](const auto& pr)
        {
            return pr.second.GetDiskState() >
                   TWaitDependentDisksToSwitchNodeResponse::DISK_STATE_SWITCHED;
        });
    if (!allVolumesAreReady) {
        return;
    }

    HandleSuccess(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TWaitDependentDisksToSwitchNodeActor::HandleGetDependentDisksResponse(
    const TEvDiskRegistry::TEvGetDependentDisksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Dependent disks listing failed with error: %s",
            FormatError(msg->GetError()).c_str());

        if (GetErrorKind(msg->GetError()) == EErrorKind::ErrorRetriable) {
            auto request = std::make_unique<
                TEvDiskRegistry::TEvGetDependentDisksRequest>();
            request->Record.SetHost(Request.GetAgentId());

            ctx.Schedule(
                RetryTimeout,
                std::make_unique<IEventHandle>(
                    MakeDiskRegistryProxyServiceId(),
                    SelfId(),
                    request.release()));
            return;
        }

        HandleError(
            ctx,
            MakeError(
                msg->GetError().GetCode(),
                TStringBuilder() << "Couldn't list dependent disks with error: "
                                 << FormatError(msg->GetError())));
        return;
    }

    if (msg->Record.GetDependentDiskIds().empty()) {
        HandleSuccess(ctx);
        return;
    }

    ui64 cookie = 0;
    for (auto& diskId: *msg->Record.MutableDependentDiskIds()) {
        TDependentDiskState state;
        state.SetDiskId(std::move(diskId));
        state.SetDiskState(
            TWaitDependentDisksToSwitchNodeResponse::DISK_STATE_INITIAL);
        auto result = DependentDiskStates.emplace(cookie, std::move(state));
        STORAGE_CHECK_PRECONDITION(result.second);

        auto request = std::make_unique<TEvVolume::TEvGetVolumeInfoRequest>();
        request->Record.SetDiskId(result.first->second.GetDiskId());
        NCloud::Send(
            ctx,
            MakeVolumeProxyServiceId(),
            std::move(request),
            cookie++);
    }
}

void TWaitDependentDisksToSwitchNodeActor::HandleGetVolumeInfoResponse(
    const TEvVolume::TEvGetVolumeInfoResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    STORAGE_CHECK_PRECONDITION(DependentDiskStates.contains(ev->Cookie));
    STORAGE_CHECK_PRECONDITION_C(
        DependentDiskStates[ev->Cookie].GetDiskId() ==
            msg->Record.GetVolume().GetDiskId(),
        TStringBuilder() << "Stored disk id: "
                         << DependentDiskStates[ev->Cookie].GetDiskId()
                         << "; incoming disk id: "
                         << msg->Record.GetVolume().GetDiskId());

    if (HasError(msg->GetError())) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Volume [%s] info request responded with error: %s",
            DependentDiskStates[ev->Cookie].GetDiskId().c_str(),
            FormatError(msg->GetError()).c_str());

        if (GetErrorKind(msg->GetError()) == EErrorKind::ErrorRetriable) {
            ScheduleGetVolumeInfoRequest(ctx, ev->Cookie);
            return;
        }

        DependentDiskStates[ev->Cookie].SetDiskState(
            TWaitDependentDisksToSwitchNodeResponse::DISK_STATE_ERROR);
        CheckAllVolumesAreSwitched(ctx);
        return;
    }

    const bool oldNodeIdPresent =
        VolumeUsesNode(msg->Record.GetVolume(), Request.GetOldNodeId());
    if (oldNodeIdPresent) {
        ScheduleGetVolumeInfoRequest(ctx, ev->Cookie);
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Volume %s has switched its node",
        DependentDiskStates[ev->Cookie].GetDiskId().c_str());

    DependentDiskStates[ev->Cookie].SetDiskState(
        TWaitDependentDisksToSwitchNodeResponse::DISK_STATE_SWITCHED);
    CheckAllVolumesAreSwitched(ctx);
}

void TWaitDependentDisksToSwitchNodeActor::HandleWaitReadyResponse(
    const TEvVolume::TEvWaitReadyResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    STORAGE_CHECK_PRECONDITION(DependentDiskStates.contains(ev->Cookie));
    STORAGE_CHECK_PRECONDITION_C(
        DependentDiskStates[ev->Cookie].GetDiskId() ==
            msg->Record.GetVolume().GetDiskId(),
        TStringBuilder() << "Stored disk id: "
                         << DependentDiskStates[ev->Cookie].GetDiskId()
                         << "; incoming disk id: "
                         << msg->Record.GetVolume().GetDiskId());

    if (HasError(msg->GetError())) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Volume [%s] wait ready request responded with error: %s",
            DependentDiskStates[ev->Cookie].GetDiskId().c_str(),
            FormatError(msg->GetError()).c_str());

        DependentDiskStates[ev->Cookie].SetDiskState(
            TWaitDependentDisksToSwitchNodeResponse::DISK_STATE_ERROR);
    } else {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Volume %s is ready",
            DependentDiskStates[ev->Cookie].GetDiskId().c_str());

        DependentDiskStates[ev->Cookie].SetDiskState(
            TWaitDependentDisksToSwitchNodeResponse::DISK_STATE_READY);
    }

    CheckAllVolumesAreReady(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TWaitDependentDisksToSwitchNodeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvDiskRegistry::TEvGetDependentDisksResponse,
            HandleGetDependentDisksResponse);
        HFunc(TEvVolume::TEvGetVolumeInfoResponse, HandleGetVolumeInfoResponse);
        HFunc(TEvVolume::TEvWaitReadyResponse, HandleWaitReadyResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr>
TServiceActor::CreateWaitDependentDisksToSwitchNodeActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TWaitDependentDisksToSwitchNodeActor>(
        std::move(requestInfo),
        std::move(input),
        Config->GetWaitDependentDisksRetryRequestDelay())};
}

}   // namespace NCloud::NBlockStore::NStorage
