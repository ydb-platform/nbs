#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAttachDetachPathActor: public TActorBootstrapped<TAttachDetachPathActor>
{
private:
    const TActorId Owner;
    const TString AgentId;
    const ui64 NodeId;
    const bool IsAttach;
    const ui64 DrGeneration;
    const TDuration DiskAgentRequestTimeout;
    THashMap<TString, ui64> PathToGeneration;

    ui64 PendingRequests = 0;

public:
    TAttachDetachPathActor(
        TActorId owner,
        TString agentId,
        ui64 nodeId,
        bool isAttach,
        ui64 drGeneration,
        THashMap<TString, ui64> pathToGeneration,
        TDuration diskAgentRequestTimeout);

    void Bootstrap(const TActorContext& ctx);

public:
    template <typename TEvRequest>
    IEventBasePtr CreateAttachDetachRequest();

    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

    void UpdatePathAttachStates(const TActorContext& ctx);

    void UpdateDeviceStatesIfNeeded(
        const TActorContext& ctx,
        const NProto::TAttachPathResponse& response);

public:
    STFUNC(StateWaitAttachDetach);
    STFUNC(StateMarkErrorDevices);
    STFUNC(StateUpdatePathAttachState);

    template <typename TEvRequest>
    void HandleAttachDetachPathRequestUndelivered(
        const TEvRequest& ev,
        const TActorContext& ctx);

    void HandleAttachDetachPathTimeout(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    template <typename TEvent>
    void HandleAttachDetachPathResult(
        const TEvent& ev,
        const NActors::TActorContext& ctx);

    void HandleChangeDeviceStateResponse(
        const TEvDiskRegistry::TEvChangeDeviceStateResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleUpdatePathAttachStateResult(
        const TEvDiskRegistryPrivate::TEvUpdatePathAttachStateResponse::TPtr&
            ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TAttachDetachPathActor::TAttachDetachPathActor(
        TActorId owner,
        TString agentId,
        ui64 nodeId,
        bool isAttach,
        ui64 drGeneration,
        THashMap<TString, ui64> pathToGeneration,
        TDuration diskAgentRequestTimeout)
    : Owner(owner)
    , AgentId(std::move(agentId))
    , NodeId(nodeId)
    , IsAttach(isAttach)
    , DrGeneration(drGeneration)
    , DiskAgentRequestTimeout(diskAgentRequestTimeout)
    , PathToGeneration(std::move(pathToGeneration))
{}

void TAttachDetachPathActor::Bootstrap(const TActorContext& ctx)
{
    IEventBasePtr request =
        IsAttach
            ? CreateAttachDetachRequest<TEvDiskAgent::TEvAttachPathRequest>()
            : CreateAttachDetachRequest<TEvDiskAgent::TEvDetachPathRequest>();

    auto event = std::make_unique<IEventHandle>(
        MakeDiskAgentServiceId(NodeId),
        ctx.SelfID,
        request.release(),
        IEventHandle::FlagForwardOnNondelivery,
        NodeId,
        &ctx.SelfID   // forwardOnNondelivery
    );

    ctx.Send(std::move(event));
    ctx.Schedule(DiskAgentRequestTimeout, new TEvents::TEvWakeup());

    Become(&TThis::StateWaitAttachDetach);
}

template <typename TEvRequest>
IEventBasePtr TAttachDetachPathActor::CreateAttachDetachRequest()
{
    auto request = std::make_unique<TEvRequest>();
    request->Record.SetDiskRegistryGeneration(DrGeneration);

    auto addPathToGen = [&]()
    {
        if constexpr (
            std::is_same_v<TEvRequest, TEvDiskAgent::TEvAttachPathRequest>)
        {
            return request->Record.AddPathsToAttach();
        } else {
            return request->Record.AddPathsToDetach();
        }
    };

    for (const auto& [path, gen]: PathToGeneration) {
        auto* pathToGen = addPathToGen();

        pathToGen->SetPath(path);
        pathToGen->SetGeneration(gen);
    }

    return request;
}

void TAttachDetachPathActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto request = std::make_unique<
        TEvDiskRegistryPrivate::TEvAttachDetachPathOperationCompleted>(
        std::move(error));
    request->AgentId = AgentId;
    request->IsAttach = IsAttach;
    NCloud::Send(ctx, Owner, std::move(request));
    Die(ctx);
}

void TAttachDetachPathActor::UpdatePathAttachStates(const TActorContext& ctx)
{
    for (auto& [path, gen]: PathToGeneration) {
        auto request = std::make_unique<
            TEvDiskRegistryPrivate::TEvUpdatePathAttachStateRequest>();
        request->KnownGeneration = gen;
        request->Path = path;
        request->AgentId = AgentId;
        request->NewState = IsAttach ? NProto::PATH_ATTACH_STATE_ATTACHED
                                     : NProto::PATH_ATTACH_STATE_DETACHED;

        NCloud::Send(ctx, Owner, std::move(request));
        ++PendingRequests;
    }

    Become(&TThis::StateUpdatePathAttachState);
}

void TAttachDetachPathActor::UpdateDeviceStatesIfNeeded(
    const TActorContext& ctx,
    const NProto::TAttachPathResponse& response)
{
    bool hasErrorDevices = false;
    for (const auto& device: response.GetAttachedDevices()) {
        if (device.GetState() == NProto::DEVICE_STATE_ERROR) {
            hasErrorDevices = true;
            ++PendingRequests;

            auto request = std::make_unique<
                TEvDiskRegistry::TEvChangeDeviceStateRequest>();

            request->Record.SetDeviceUUID(device.GetDeviceUUID());
            request->Record.SetDeviceState(NProto::DEVICE_STATE_ERROR);
            request->Record.SetReason(device.GetStateMessage());

            NCloud::Send(ctx, Owner, std::move(request));
        }
    }

    if (hasErrorDevices) {
        Become(&TThis::StateMarkErrorDevices);
        return;
    }
    UpdatePathAttachStates(ctx);
}

template <typename TEvRequest>
void TAttachDetachPathActor::HandleAttachDetachPathRequestUndelivered(
    const TEvRequest& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request undelivered"));
}

void TAttachDetachPathActor::HandleAttachDetachPathTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request timed out"));
}

template <typename TEvResponse>
void TAttachDetachPathActor::HandleAttachDetachPathResult(
    const TEvResponse& ev,
    const TActorContext& ctx)
{
    using TEvAttachRequest = TEvDiskAgent::TEvAttachPathResponse::TPtr;

    if constexpr (std::is_same_v<TEvResponse, TEvAttachRequest>) {
        Y_ABORT_UNLESS(IsAttach);
    } else {
        Y_ABORT_UNLESS(!IsAttach);
    }

    auto* msg = ev->Get();
    auto& record = msg->Record;
    const auto& error = record.GetError();

    if (HasError(error) && error.GetCode() != E_ARGUMENT) {
        ReplyAndDie(ctx, error);
        return;
    }
    if (error.GetCode() == E_ARGUMENT) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "Attach detach path on agent %s disabled, ignoring error",
            AgentId.Quote().c_str());
    }

    if constexpr (std::is_same_v<TEvResponse, TEvAttachRequest>) {
        UpdateDeviceStatesIfNeeded(ctx, record);
    } else {
        UpdatePathAttachStates(ctx);
    }
}

void TAttachDetachPathActor::HandleChangeDeviceStateResponse(
    const TEvDiskRegistry::TEvChangeDeviceStateResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& error = *msg->Record.MutableError();

    if (HasError(error)) {
        ReplyAndDie(ctx, std::move(error));
        return;
    }

    --PendingRequests;
    if (PendingRequests == 0) {
        UpdatePathAttachStates(ctx);
    }
}

void TAttachDetachPathActor::HandleUpdatePathAttachStateResult(
    const TEvDiskRegistryPrivate::TEvUpdatePathAttachStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& error = msg->Error;

    if (HasError(error)) {
        ReplyAndDie(ctx, error);
        return;
    }

    --PendingRequests;
    if (PendingRequests == 0) {
        ReplyAndDie(ctx, {});
    }
}

void TAttachDetachPathActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

STFUNC(TAttachDetachPathActor::StateWaitAttachDetach)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvDiskAgent::TEvAttachPathRequest,
            HandleAttachDetachPathRequestUndelivered);
        HFunc(
            TEvDiskAgent::TEvDetachPathRequest,
            HandleAttachDetachPathRequestUndelivered);

        HFunc(TEvents::TEvWakeup, HandleAttachDetachPathTimeout)

        HFunc(TEvDiskAgent::TEvAttachPathResponse, HandleAttachDetachPathResult);
        HFunc(TEvDiskAgent::TEvDetachPathResponse, HandleAttachDetachPathResult);


        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_REGISTRY_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TAttachDetachPathActor::StateMarkErrorDevices)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvDiskRegistry::TEvChangeDeviceStateResponse,
            HandleChangeDeviceStateResponse);

        IgnoreFunc(TEvents::TEvWakeup);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_REGISTRY_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TAttachDetachPathActor::StateUpdatePathAttachState)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvDiskRegistryPrivate::TEvUpdatePathAttachStateResponse,
            HandleUpdatePathAttachStateResult);

        IgnoreFunc(TEvents::TEvWakeup);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::DISK_REGISTRY_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::HandleUpdatePathAttachState(
    const TEvDiskRegistryPrivate::TEvUpdatePathAttachStateRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(UpdatePathAttachState);

    if (!Config->GetAttachDetachPathsEnabled()) {
        auto response = std::make_unique<
            TEvDiskRegistryPrivate::TEvUpdatePathAttachStateResponse>(
            MakeError(E_ARGUMENT, "path attach detach feature disabled"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Received UpdatePathAttachState request: Agent=%s, Path=%s, "
        "State=%d",
        TabletID(),
        msg->AgentId.Quote().c_str(),
        msg->Path.Quote().c_str(),
        static_cast<int>(msg->NewState));

    ExecuteTx<TUpdatePathAttachState>(
        ctx,
        std::move(requestInfo),
        std::move(msg->AgentId),
        std::move(msg->Path),
        msg->NewState,
        msg->KnownGeneration);
}

////////////////////////////////////////////////////////////////////////////////

bool TDiskRegistryActor::PrepareUpdatePathAttachState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdatePathAttachState& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteUpdatePathAttachState(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TUpdatePathAttachState& args)
{
    Y_UNUSED(ctx);
    TDiskRegistryDatabase db(tx.DB);
    args.Error = State->UpdatePathAttachState(
        db,
        args.AgentId,
        args.Path,
        args.NewState,
        args.KnownGeneration);
}

void TDiskRegistryActor::CompleteUpdatePathAttachState(
    const TActorContext& ctx,
    TTxDiskRegistry::TUpdatePathAttachState& args)
{
    LOG_LOG(
        ctx,
        HasError(args.Error) ? NLog::PRI_ERROR : NLog::PRI_INFO,
        TBlockStoreComponents::DISK_REGISTRY,
        "UpdatePathAttachState result: Agent=%s, Path=%s, Error=%s",
        args.AgentId.c_str(),
        args.Path.c_str(),
        FormatError(args.Error).c_str());

    auto response = std::make_unique<
        TEvDiskRegistryPrivate::TEvUpdatePathAttachStateResponse>(
        std::move(args.Error));
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    SecureErase(ctx);
    ProcessPathsToAttachDetach(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::ProcessPathsToAttachDetachOnAgent(
    const NActors::TActorContext& ctx,
    const NProto::TAgentConfig* agent,
    const THashSet<TString>& paths)
{
    const TString& agentId = agent->GetAgentId();
    THashMap<TString, ui64> pathToGeneration;
    std::optional<bool> isAttach;
    for (const auto& path: paths) {
        auto it = agent->GetPathAttachStates().find(path);
        Y_DEBUG_ABORT_UNLESS(it != agent->GetPathAttachStates().end());
        if (it == agent->GetPathAttachStates().end()) {
            continue;
        }

        // Skip paths with dependent disks for detach
        if (it->second == NProto::PATH_ATTACH_STATE_DETACHING &&
            State->HasDependentDisks(agentId, path))
        {
            continue;
        }

        if (!isAttach) {
            isAttach = it->second == NProto::PATH_ATTACH_STATE_ATTACHING;
        } else if (
            it->second != (*isAttach ? NProto::PATH_ATTACH_STATE_ATTACHING
                                     : NProto::PATH_ATTACH_STATE_DETACHING))
        {
            continue;
        }

        pathToGeneration[path] = State->GetPathGeneration(agentId, path);
    }

    if (!isAttach) {
        return;
    }

    auto actorId = NCloud::Register<TAttachDetachPathActor>(
        ctx,
        ctx.SelfID,
        agentId,
        agent->GetNodeId(),
        *isAttach,
        Executor()->Generation(),
        std::move(pathToGeneration),
        Config->GetAgentRequestTimeout());
    AgentsWithAttachDetachRequestsInProgress[agentId] = actorId;
}

void TDiskRegistryActor::ProcessPathsToAttachDetach(const TActorContext& ctx)
{
    if (!Config->GetAttachDetachPathsEnabled()) {
        return;
    }

    if (AgentsWithAttachDetachRequestsInProgress.size() ==
        Config->GetMaxInflightAttachDetachPathRequestsProcessing())
    {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "Max inflight of attach detach requests reached, will try later");
        return;
    }

    auto pathsNeedToProcess = State->GetPathsToAttachDetach();
    if (!pathsNeedToProcess) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "No attach detach path requests to process, will try later");
        return;
    }

    for (const auto& [agentId, paths]: pathsNeedToProcess) {
        if (AgentsWithAttachDetachRequestsInProgress.contains(agentId)) {
            continue;
        }

        const auto* agent = State->FindAgent(agentId);
        if (!agent || agent->GetState() == NProto::AGENT_STATE_UNAVAILABLE ||
            agent->GetTemporaryAgent())
        {
            continue;
        }

        ProcessPathsToAttachDetachOnAgent(ctx, agent, paths);

        if (AgentsWithAttachDetachRequestsInProgress.size() ==
            Config->GetMaxInflightAttachDetachPathRequestsProcessing())
        {
            return;
        }
    }
}

void TDiskRegistryActor::HandleAttachDetachPathOperationCompleted(
    const TEvDiskRegistryPrivate::TEvAttachDetachPathOperationCompleted::TPtr&
        ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    if (HasError(msg->Error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "Failed to %s path on agent %s: %s",
            msg->IsAttach ? "attach" : "detach",
            msg->AgentId.Quote().c_str(),
            FormatError(msg->Error).c_str());
    } else {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "Successfully %s path on agent %s: %s",
            msg->IsAttach ? "attached" : "detached",
            msg->AgentId.Quote().c_str(),
            FormatError(msg->Error).c_str());
    }

    AgentsWithAttachDetachRequestsInProgress.erase(msg->AgentId);

    ProcessPathsToAttachDetach(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
