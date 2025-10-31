#include "disk_registry_actor.h"
#include "util/string/join.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

IEventBasePtr CreateResponseByActionType(
    NProto::TAction_EType actionType,
    NProto::TError error)
{
    switch (actionType) {
        case NProto::TAction_EType_REMOVE_DEVICE:
            return std::make_unique<
                TEvDiskRegistryPrivate::TEvUpdateCmsHostDeviceStateResponse>(
                std::move(error));
        case NProto::TAction_EType_REMOVE_HOST:
            return std::make_unique<
                TEvDiskRegistryPrivate::TEvUpdateCmsHostStateResponse>(
                std::move(error));
        default:
            Y_ABORT("Unsupported action type");
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TRequestContext {
    TRequestInfoPtr RequestInfo;
    NProto::TAction_EType ActionType;
};

class TAttachDetachPathActor: public TActorBootstrapped<TAttachDetachPathActor>
{
private:
    const TActorId Owner;
    const TString AgentId;
    const ui64 NodeId;
    const bool IsAttach;
    const ui64 DrGeneration;
    const ui64 DiskAgentGeneration;
    const TDuration DiskAgentRequestTimeout;
    TVector<TString> Paths;

    ui64 PendingRequests = 0;

    std::optional<TRequestContext> RequestContext;

public:
    TAttachDetachPathActor(
        TActorId owner,
        TString agentId,
        ui64 nodeId,
        bool isAttach,
        ui64 drGeneration,
        ui64 diskAgentGeneration,
        TVector<TString> paths,
        TDuration diskAgentRequestTimeout,
        std::optional<TRequestContext> requestContext);

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
        ui64 diskAgentGeneration,
        TVector<TString> paths,
        TDuration diskAgentRequestTimeout,
        std::optional<TRequestContext> requestContext)
    : Owner(owner)
    , AgentId(std::move(agentId))
    , NodeId(nodeId)
    , IsAttach(isAttach)
    , DrGeneration(drGeneration)
    , DiskAgentGeneration(diskAgentGeneration)
    , DiskAgentRequestTimeout(diskAgentRequestTimeout)
    , Paths(std::move(paths))
    , RequestContext(std::move(requestContext))
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

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY_WORKER,
        "Sending %s paths request to disk agent[DiskAgentGeneration=%lu] with "
        "paths[%s]",
        IsAttach ? "attach" : "detach",
        DiskAgentGeneration,
        JoinSeq(",", Paths).c_str());

    Become(&TThis::StateWaitAttachDetach);
}

template <typename TEvRequest>
IEventBasePtr TAttachDetachPathActor::CreateAttachDetachRequest()
{
    auto request = std::make_unique<TEvRequest>();
    request->Record.SetDiskRegistryGeneration(DrGeneration);
    request->Record.SetDiskAgentGeneration(DiskAgentGeneration);

    auto* mutablePaths = [&]()
    {
        if constexpr (
            std::is_same_v<TEvRequest, TEvDiskAgent::TEvAttachPathRequest>)
        {
            return request->Record.MutablePathsToAttach();
        } else {
            return request->Record.MutablePathsToDetach();
        }
    }();

    mutablePaths->Add(Paths.begin(), Paths.end());

    return request;
}

void TAttachDetachPathActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (RequestContext) {
        auto response =
            CreateResponseByActionType(RequestContext->ActionType, error);
        NCloud::Reply(ctx, *RequestContext->RequestInfo, std::move(response));
    }

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
    Y_DEBUG_ABORT_UNLESS(PendingRequests == 0);
    for (auto& path : Paths) {
        auto request = std::make_unique<
            TEvDiskRegistryPrivate::TEvUpdatePathAttachStateRequest>();
        request->KnownGeneration = DiskAgentGeneration;
        request->Path = path;
        request->AgentId = AgentId;
        request->NewState = NProto::PATH_ATTACH_STATE_ATTACHED;

        NCloud::Send(ctx, Owner, std::move(request));
        ++PendingRequests;
    }

    Become(&TThis::StateUpdatePathAttachState);
}

void TAttachDetachPathActor::UpdateDeviceStatesIfNeeded(
    const TActorContext& ctx,
    const NProto::TAttachPathResponse& response)
{
    Y_DEBUG_ABORT_UNLESS(PendingRequests == 0);
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
    using TEvAttachResponse = TEvDiskAgent::TEvAttachPathResponse::TPtr;

    if constexpr (std::is_same_v<TEvResponse, TEvAttachResponse>) {
        Y_ABORT_UNLESS(IsAttach);
    } else {
        Y_ABORT_UNLESS(!IsAttach);
    }

    auto* msg = ev->Get();
    auto& record = msg->Record;
    const auto& error = record.GetError();

    if (HasError(error) && error.GetCode() != E_PRECONDITION_FAILED) {
        ReplyAndDie(ctx, error);
        return;
    }
    if (error.GetCode() == E_PRECONDITION_FAILED) {
        LOG_WARN(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "Attach detach path on agent %s disabled, ignoring error",
            AgentId.Quote().c_str());
    }

    if constexpr (std::is_same_v<TEvResponse, TEvAttachResponse>) {
        UpdateDeviceStatesIfNeeded(ctx, record);
    } else {
        ReplyAndDie(ctx, {});
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
            TEvDiskRegistryPrivate::TEvUpdatePathAttachStateResponse>(MakeError(
            E_PRECONDITION_FAILED,
            "path attach detach feature disabled"));
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

void TDiskRegistryActor::ProcessPathsToAttachOnAgent(
    const NActors::TActorContext& ctx,
    const NProto::TAgentConfig* agent,
    const THashSet<TString>& paths)
{
    const TString& agentId = agent->GetAgentId();
    TVector<TString> pathsToAttach;
    for (const auto& path: paths) {
        auto it = agent->GetPathAttachStates().find(path);
        Y_DEBUG_ABORT_UNLESS(it != agent->GetPathAttachStates().end());
        if (it == agent->GetPathAttachStates().end()) {
            continue;
        }

        if (it->second != NProto::PATH_ATTACH_STATE_ATTACHING) {
            continue;
        }

        pathsToAttach.emplace_back(path);
    }

    if (!pathsToAttach) {
        return;
    }

    auto actorId = NCloud::Register<TAttachDetachPathActor>(
        ctx,
        ctx.SelfID,
        agentId,
        agent->GetNodeId(),
        true,   // isAttach
        Executor()->Generation(),
        State->GetDiskAgentGeneration(agentId),
        std::move(pathsToAttach),
        Config->GetAttachDetachPathRequestTimeout(),
        std::nullopt   // requestContext
    );
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
        LOG_WARN(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Max inflight of attach detach requests reached, will try "
            "later",
            TabletID());
        return;
    }

    auto pathsNeedToProcess = State->GetPathsToAttachDetach();
    if (!pathsNeedToProcess) {
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] No attach detach path requests to process, will try later",
            TabletID());
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

        ProcessPathsToAttachOnAgent(ctx, agent, paths);

        if (AgentsWithAttachDetachRequestsInProgress.size() ==
            Config->GetMaxInflightAttachDetachPathRequestsProcessing())
        {
            return;
        }
    }
}

void TDiskRegistryActor::TryToDetachPaths(
    const NActors::TActorContext& ctx,
    const TString& agentId,
    TVector<TString> paths,
    TRequestInfoPtr requestInfo,
    NProto::TAction_EType actionType)
{
    if (AgentsWithAttachDetachRequestsInProgress.contains(agentId)) {
        auto response = CreateResponseByActionType(
            actionType,
            MakeError(
                E_REJECTED,
                Sprintf(
                    "already has pending cms action request for agent[%s]",
                    agentId.Quote().c_str())));
        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    const auto* agent = State->FindAgent(agentId);
    if (!agent) {
        auto response = CreateResponseByActionType(
            actionType,
            MakeError(
                E_NOT_FOUND,
                Sprintf("agent[%s] not found", agentId.Quote().c_str())));
        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    for (const auto& path: paths) {
        auto it = agent->GetPathAttachStates().find(path);
        if (it == agent->GetPathAttachStates().end() ||
            it->second == NProto::PATH_ATTACH_STATE_DETACHED)
        {
            continue;
        }

        auto response = CreateResponseByActionType(
            actionType,
            MakeError(
                E_ARGUMENT,
                Sprintf(
                    "path[%s] is not detached on agent[%s]",
                    path.Quote().c_str(),
                    agentId.Quote().c_str())));

        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    auto actorId = NCloud::Register<TAttachDetachPathActor>(
        ctx,
        ctx.SelfID,
        agentId,
        agent->GetNodeId(),
        false,   // isAttach
        Executor()->Generation(),
        State->GetDiskAgentGeneration(agentId),
        std::move(paths),
        Config->GetAttachDetachPathRequestTimeout(),
        TRequestContext{
            .RequestInfo = std::move(requestInfo),
            .ActionType = actionType});
    AgentsWithAttachDetachRequestsInProgress[agentId] = actorId;
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
            "[%lu] Failed to %s path on agent %s: %s",
            TabletID(),
            msg->IsAttach ? "attach" : "detach",
            msg->AgentId.Quote().c_str(),
            FormatError(msg->Error).c_str());
    } else {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_REGISTRY,
            "[%lu] Successfully %s path on agent %s",
            TabletID(),
            msg->IsAttach ? "attached" : "detached",
            msg->AgentId.Quote().c_str());
    }

    AgentsWithAttachDetachRequestsInProgress.erase(msg->AgentId);

    ProcessPathsToAttachDetach(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
