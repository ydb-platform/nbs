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

struct TRequestContext
{
    TRequestInfoPtr RequestInfo;
    NProto::TAction_EType ActionType;
};

class TDetachPathActor: public TActorBootstrapped<TDetachPathActor>
{
private:
    const TActorId Owner;
    const TString AgentId;
    const ui64 NodeId;
    const TDuration DiskAgentRequestTimeout;
    TVector<TString> Paths;

    std::optional<TRequestContext> RequestContext;

public:
    TDetachPathActor(
        TActorId owner,
        TString agentId,
        ui64 nodeId,
        TVector<TString> paths,
        TDuration diskAgentRequestTimeout,
        std::optional<TRequestContext> requestContext);

    void Bootstrap(const TActorContext& ctx);

public:
    IEventBasePtr CreateDetachRequest();

    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

public:
    STFUNC(StateWaitAttachDetach);

    void HandleDetachPathsRequestUndelivered(
        const TEvDiskAgent::TEvDetachPathsRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleRequestTimeout(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandleDetachPathsResult(
        const TEvDiskAgent::TEvDetachPathsResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDetachPathActor::TDetachPathActor(
        TActorId owner,
        TString agentId,
        ui64 nodeId,
        TVector<TString> paths,
        TDuration diskAgentRequestTimeout,
        std::optional<TRequestContext> requestContext)
    : Owner(owner)
    , AgentId(std::move(agentId))
    , NodeId(nodeId)
    , DiskAgentRequestTimeout(diskAgentRequestTimeout)
    , Paths(std::move(paths))
    , RequestContext(std::move(requestContext))
{}

void TDetachPathActor::Bootstrap(const TActorContext& ctx)
{
    IEventBasePtr request = CreateDetachRequest();

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
        "Sending detach paths request to disk agent %s with "
        "paths[%s]",
        AgentId.Quote().c_str(),
        JoinSeq(",", Paths).c_str());

    Become(&TThis::StateWaitAttachDetach);
}

IEventBasePtr TDetachPathActor::CreateDetachRequest()
{
    auto request = std::make_unique<TEvDiskAgent::TEvDetachPathsRequest>();

    request->Record.MutablePathsToDetach()->Add(Paths.begin(), Paths.end());

    return request;
}

void TDetachPathActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (RequestContext) {
        auto response =
            CreateResponseByActionType(RequestContext->ActionType, error);
        NCloud::Reply(ctx, *RequestContext->RequestInfo, std::move(response));
    }

    auto request = std::make_unique<
        TEvDiskRegistryPrivate::TEvDetachPathsOperationCompleted>(
        std::move(error));
    request->AgentId = AgentId;
    NCloud::Send(ctx, Owner, std::move(request));
    Die(ctx);
}

void TDetachPathActor::HandleDetachPathsRequestUndelivered(
    const TEvDiskAgent::TEvDetachPathsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request undelivered"));
}

void TDetachPathActor::HandleRequestTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_TIMEOUT, "request timed out"));
}

void TDetachPathActor::HandleDetachPathsResult(
    const TEvDiskAgent::TEvDetachPathsResponse::TPtr& ev,
    const NActors::TActorContext& ctx)
{
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

    ReplyAndDie(ctx, {});
}

void TDetachPathActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

STFUNC(TDetachPathActor::StateWaitAttachDetach)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvDiskAgent::TEvDetachPathsRequest,
            HandleDetachPathsRequestUndelivered);

        HFunc(TEvents::TEvWakeup, HandleRequestTimeout);

        HFunc(TEvDiskAgent::TEvDetachPathsResponse, HandleDetachPathsResult);

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

void TDiskRegistryActor::TryToDetachPaths(
    const NActors::TActorContext& ctx,
    const TString& agentId,
    TVector<TString> paths,
    TRequestInfoPtr requestInfo,
    NProto::TAction_EType actionType)
{
    if (AgentsWithDetachRequestsInProgress.contains(agentId)) {
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

    auto actorId = NCloud::Register<TDetachPathActor>(
        ctx,
        ctx.SelfID,
        agentId,
        agent->GetNodeId(),
        std::move(paths),
        Config->GetAttachDetachPathRequestTimeout(),
        TRequestContext{
            .RequestInfo = std::move(requestInfo),
            .ActionType = actionType});
    AgentsWithDetachRequestsInProgress[agentId] = actorId;
}

void TDiskRegistryActor::HandleDetachPathsOperationCompleted(
    const TEvDiskRegistryPrivate::TEvDetachPathsOperationCompleted::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto priority = HasError(msg->Error) ? NActors::NLog::PRI_ERROR
                                         : NActors::NLog::PRI_INFO;

    TStringBuilder builder;
    builder << LogTitle.GetWithTime() << " detach request on agent "
            << msg->AgentId.Quote();

    if (HasError(msg->Error)) {
        builder << " failed: " << FormatError(msg->Error);
    } else {
        builder << " completed successfully";
    }

    LOG_LOG(ctx, priority, TBlockStoreComponents::DISK_REGISTRY, builder);

    AgentsWithDetachRequestsInProgress.erase(msg->AgentId);
}

}   // namespace NCloud::NBlockStore::NStorage
