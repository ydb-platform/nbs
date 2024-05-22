#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <library/cpp/json/json_reader.h>

#include <google/protobuf/util/json_util.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

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

    TString AgentId;
    ui32 OldNodeId = 0;

    enum class EVolumeState
    {
        Initial,
        Switched,
        Ready,
    };
    // THashMap<TString, EVolumeState> DependentVolumeStates;
    struct TVolumeInfo
    {
        TString DiskId;
        EVolumeState State;
    };
    THashMap<ui64, TVolumeInfo> DependentVolumeStates;

public:
    TWaitDependentDisksToSwitchNodeActor(
        TRequestInfoPtr requestInfo,
        TString input,
        TDuration retryTimeout);

    void Bootstrap(const TActorContext& ctx);

private:
    void HandleSuccess(const TActorContext& ctx);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

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
    LOG_ERROR(
        ctx,
        TBlockStoreComponents::SERVICE,
        "HELLO HELLO HELLO HELLO HELLO HELLO HELLO HELLO HELLO");
    if (!Input) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Empty input"));
        return;
    }

    NJson::TJsonValue input;
    if (!NJson::ReadJsonTree(Input, &input, false)) {
        HandleError(
            ctx,
            MakeError(E_ARGUMENT, "Input should be in JSON format"));
        return;
    }

    if (!input.Has("AgentId")) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Empty AgentId"));
        return;
    }

    if (!input.Has("OldNodeId")) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Empty OldNodeId"));
        return;
    }

    AgentId = input["AgentId"].GetString();
    OldNodeId = input["OldNodeId"].GetUInteger();

    Cerr << "TWaitDependentDisksToSwitchNodeActor: AgentId = " << AgentId
         << "; OldNodeId = " << OldNodeId << Endl;

    auto request =
        std::make_unique<TEvDiskRegistry::TEvGetDependentDisksRequest>();
    request->Record.SetHost(AgentId);

    Become(&TThis::StateWork);

    NCloud::Send(ctx, MakeDiskRegistryProxyServiceId(), std::move(request));
}

void TWaitDependentDisksToSwitchNodeActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(error);
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TWaitDependentDisksToSwitchNodeActor::HandleSuccess(
    const TActorContext& ctx)
{
    NCloud::Reply(
        ctx,
        *RequestInfo,
        std::make_unique<TEvService::TEvExecuteActionResponse>());
    Die(ctx);
}

void TWaitDependentDisksToSwitchNodeActor::ScheduleGetVolumeInfoRequest(
    const TActorContext& ctx,
    ui64 cookie)
{
    Y_DEBUG_ABORT_UNLESS(DependentVolumeStates.contains(cookie));
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Schedule GetVolumeInfoRequest for volume: %s, timeout: %s",
        "Schedule GetVolumeInfoRequest for volume: %s, timeout: %s",
        DependentVolumeStates[cookie].DiskId.c_str(),
        RetryTimeout.ToString().c_str());

    auto request = std::make_unique<TEvVolume::TEvGetVolumeInfoRequest>();
    request->Record.SetDiskId(DependentVolumeStates[cookie].DiskId);
    ctx.ExecutorThread.Schedule(
        RetryTimeout,
        new IEventHandle(
            MakeVolumeProxyServiceId(),
            ctx.SelfID,
            request.release(),
            TEventFlags{0},
            cookie));
}

void TWaitDependentDisksToSwitchNodeActor::CheckAllVolumesAreSwitched(
    const TActorContext& ctx)
{
    if (DependentVolumeStates.empty()) {
        HandleSuccess(ctx);
        return;
    }

    const bool allVolumesAreSwitched = AllOf(
        DependentVolumeStates,
        [](const auto& pr)
        { return pr.second.State == EVolumeState::Switched; });
    if (!allVolumesAreSwitched) {
        return;
    }

    for (const auto& pr: DependentVolumeStates) {
        auto request = std::make_unique<TEvVolume::TEvWaitReadyRequest>();
        request->Record.SetDiskId(pr.second.DiskId);
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
    if (DependentVolumeStates.empty()) {
        HandleSuccess(ctx);
        return;
    }

    const bool allVolumesAreReady = AllOf(
        DependentVolumeStates,
        [](const auto& pr) { return pr.second.State == EVolumeState::Ready; });
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
            request->Record.SetHost(AgentId);

            ctx.ExecutorThread.Schedule(
                RetryTimeout,
                new IEventHandle(
                    MakeDiskRegistryProxyServiceId(),
                    ctx.SelfID,
                    request.release()));
            return;
        }

        HandleError(
            ctx,
            MakeError(
                E_FAIL,
                TStringBuilder() << "Couldn't list dependent disks with error: "
                                 << FormatError(msg->GetError())));
        return;
    }

    if (msg->Record.GetDependentDiskIds().empty()) {
        HandleSuccess(ctx);
        return;
    }

    ui64 cookie = 0;
    Cerr << "Got dependent disk ids: ";
    for (auto& diskId: *msg->Record.MutableDependentDiskIds()) {
        Cerr << diskId << ", ";
        auto result = DependentVolumeStates.emplace(
            cookie,
            TVolumeInfo{std::move(diskId), EVolumeState::Initial});
        Y_DEBUG_ABORT_UNLESS(result.second);

        auto request = std::make_unique<TEvVolume::TEvGetVolumeInfoRequest>();
        request->Record.SetDiskId(result.first->second.DiskId);
        NCloud::Send(
            ctx,
            MakeVolumeProxyServiceId(),
            std::move(request),
            cookie++);
    }
    Cerr << Endl;
}

void TWaitDependentDisksToSwitchNodeActor::HandleGetVolumeInfoResponse(
    const TEvVolume::TEvGetVolumeInfoResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    Y_DEBUG_ABORT_UNLESS(DependentVolumeStates.contains(ev->Cookie));

    if (HasError(msg->GetError())) {
        Cerr << "HandleGetVolumeInfoResponse: error: "
             << FormatError(msg->GetError()) << Endl;
        LOG_WARN(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Volume [%s] info request responded with error: %s",
            DependentVolumeStates[ev->Cookie].DiskId.c_str(),
            FormatError(msg->GetError()).c_str());

        if (GetErrorKind(msg->GetError()) == EErrorKind::ErrorRetriable) {
            ScheduleGetVolumeInfoRequest(ctx, ev->Cookie);
            return;
        }

        DependentVolumeStates.erase(ev->Cookie);
        CheckAllVolumesAreSwitched(ctx);
        return;
    }

    const bool oldNodeIdPresent =
        VolumeUsesNode(msg->Record.GetVolume(), OldNodeId);

    // const auto& devices = msg->Record.GetVolume().GetDevices();
    // TStringBuilder devicesstr;
    // for (const auto& device : devices) {
    //     devicesstr << device.GetDeviceUUID() << " : " << device.GetNodeId() << Endl;
    // }

    Cerr << "HandleGetVolumeInfoResponse: diskId = "
         << DependentVolumeStates[ev->Cookie].DiskId
         << "; devices.size = " << msg->Record.GetVolume().GetDevices().size();
        //  << "; oldNodeIdPresent = " << oldNodeIdPresent
        //  << "; devices:\n"
        //  << devicesstr << Endl;

    if (oldNodeIdPresent) {
        Cerr << "oldNodeIdPresent for "
             << DependentVolumeStates[ev->Cookie].DiskId << Endl;
        ScheduleGetVolumeInfoRequest(ctx, ev->Cookie);
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Volume %s has switched its node",
        DependentVolumeStates[ev->Cookie].DiskId.c_str());

    Cerr << DependentVolumeStates[ev->Cookie].DiskId << " has been switched"
         << Endl;

    DependentVolumeStates[ev->Cookie].State = EVolumeState::Switched;
    CheckAllVolumesAreSwitched(ctx);
}

void TWaitDependentDisksToSwitchNodeActor::HandleWaitReadyResponse(
    const TEvVolume::TEvWaitReadyResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    Y_DEBUG_ABORT_UNLESS(DependentVolumeStates.contains(ev->Cookie));

    if (HasError(msg->GetError())) {
        Cerr << "Volume " << DependentVolumeStates[ev->Cookie].DiskId
             << " wait ready request responded with error"
             << FormatError(msg->GetError()) << Endl;
        LOG_WARN(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Volume [%s] wait ready request responded with error: %s",
            DependentVolumeStates[ev->Cookie].DiskId.c_str(),
            FormatError(msg->GetError()).c_str());

        DependentVolumeStates.erase(ev->Cookie);
    } else {
        Cerr << "Volume " << DependentVolumeStates[ev->Cookie].DiskId
             << " is ready" << Endl;
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Volume %s is ready",
            DependentVolumeStates[ev->Cookie].DiskId.c_str());

        DependentVolumeStates[ev->Cookie].State = EVolumeState::Ready;
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
            HandleUnexpectedEvent(ev, TBlockStoreComponents::SERVICE);
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
