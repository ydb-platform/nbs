#include "disk_registry_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NMonitoringUtils;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUpdatePathAttachStateActor final
    : public TActorBootstrapped<TUpdatePathAttachStateActor>
{
private:
    const TActorId Owner;
    const ui64 TabletID;
    const TRequestInfoPtr RequestInfo;
    const TString AgentID;
    const TString Path;
    const NProto::EPathAttachState NewState;
    const ui64 PathGeneration;

public:
    TUpdatePathAttachStateActor(
        const TActorId& owner,
        ui64 tabletID,
        TRequestInfoPtr requestInfo,
        TString agentId,
        TString path,
        NProto::EPathAttachState newState,
        ui64 pathGeneration);

    void Bootstrap(const TActorContext& ctx);

private:
    void Notify(
        const TActorContext& ctx,
        TString message,
        const EAlertLevel alertLevel);

    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandleUpdatePathAttachStateResponse(
        const TEvDiskRegistryPrivate::TEvUpdatePathAttachStateResponse::TPtr&
            ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TUpdatePathAttachStateActor::TUpdatePathAttachStateActor(
        const TActorId& owner,
        ui64 tabletID,
        TRequestInfoPtr requestInfo,
        TString agentId,
        TString path,
        NProto::EPathAttachState newState,
        ui64 pathGeneration)
    : Owner(owner)
    , TabletID(tabletID)
    , RequestInfo(std::move(requestInfo))
    , AgentID(std::move(agentId))
    , Path(std::move(path))
    , NewState(newState)
    , PathGeneration(pathGeneration)
{}

void TUpdatePathAttachStateActor::Bootstrap(const TActorContext& ctx)
{
    auto request = std::make_unique<
        TEvDiskRegistryPrivate::TEvUpdatePathAttachStateRequest>();

    request->AgentId = AgentID;
    request->Path = Path;
    request->NewState = NewState;
    request->KnownGeneration = PathGeneration;

    NCloud::Send(ctx, Owner, std::move(request));

    Become(&TThis::StateWork);
}

void TUpdatePathAttachStateActor::Notify(
    const TActorContext& ctx,
    TString message,
    const EAlertLevel alertLevel)
{
    TStringStream out;

    BuildNotifyPageWithRedirect(
        out,
        std::move(message),
        TStringBuilder() << "./app?action=agent&TabletID=" << TabletID
                         << "&AgentID=" << AgentID,
        alertLevel);

    auto response = std::make_unique<NMon::TEvRemoteHttpInfoRes>(out.Str());
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
}

void TUpdatePathAttachStateActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (!HasError(error)) {
        Notify(ctx, "Operation successfully completed", EAlertLevel::SUCCESS);
    } else {
        Notify(
            ctx,
            TStringBuilder()
                << "failed to change path[" << Path.Quote()
                << "] state on agent[" << AgentID.Quote() << "] to "
                << NProto::EPathAttachState_Name(NewState) << ": "
                << FormatError(error),
            EAlertLevel::DANGER);
    }

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvDiskRegistryPrivate::TEvOperationCompleted>());

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TUpdatePathAttachStateActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

void TUpdatePathAttachStateActor::HandleUpdatePathAttachStateResponse(
    const TEvDiskRegistryPrivate::TEvUpdatePathAttachStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* response = ev->Get();

    ReplyAndDie(ctx, response->GetError());
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TUpdatePathAttachStateActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvDiskRegistryPrivate::TEvUpdatePathAttachStateResponse,
            HandleUpdatePathAttachStateResponse);

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

void TDiskRegistryActor::HandleHttpInfo_UpdatePathAttachState(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    if (!Config->GetAttachDetachPathsEnabled()) {
        RejectHttpRequest(
            ctx,
            *requestInfo,
            "Attach Detach path feature disabled");
        return;
    }

    const auto& newStateRaw = params.Get("NewState");
    const auto& agentId = params.Get("AgentID");
    const auto& path = params.Get("Path");

    if (!newStateRaw) {
        RejectHttpRequest(ctx, *requestInfo, "No new state is given");
        return;
    }

    if (!agentId) {
        RejectHttpRequest(ctx, *requestInfo, "No agent id is given");
        return;
    }

    if (!path) {
        RejectHttpRequest(ctx, *requestInfo, "No path is given");
        return;
    }

    NProto::EPathAttachState newState;
    if (!EPathAttachState_Parse(newStateRaw, &newState)) {
        RejectHttpRequest(ctx, *requestInfo, "Invalid new state");
        return;
    }

    switch (newState) {
        case NProto::PATH_ATTACH_STATE_ATTACHING:
        case NProto::PATH_ATTACH_STATE_DETACHING:
            break;
        default:
            RejectHttpRequest(ctx, *requestInfo, "Invalid new state");
            return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "[%lu] Change state of path[%s] on agent[%s] on monitoring page to %s",
        TabletID(),
        path.Quote().c_str(),
        agentId.Quote().c_str(),
        newStateRaw.c_str());

    const ui64 pathGeneration = State->GetPathGeneration(agentId, path);

    auto actor = NCloud::Register<TUpdatePathAttachStateActor>(
        ctx,
        SelfId(),
        TabletID(),
        std::move(requestInfo),
        agentId,
        path,
        newState,
        pathGeneration);

    Actors.insert(actor);
}

void TDiskRegistryActor::RenderPathAttachStates(
    IOutputStream& out,
    const NProto::TAgentConfig& agent) const
{
    HTML (out) {
        TAG (TH3) {
            out << "Path attach states";
        }

        TABLE_SORTABLE_CLASS("table table-bordered")
        {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        out << "Path";
                    }
                    TABLEH () {
                        out << "Attach State";
                    }
                    TABLEH () {
                        out << "Path generation";
                    }
                    TABLEH () {
                        out << "Attach\\Detach Path";
                    }
                }
            }

            for (const auto& [path, state]: agent.GetPathAttachStates()) {
                TABLER () {
                    TABLED () {
                        out << path;
                    }
                    TABLED () {
                        switch (state) {
                            case NProto::PATH_ATTACH_STATE_ATTACHED:
                                out << "<font color=green>Attached</font>";
                                break;
                            case NProto::PATH_ATTACH_STATE_ATTACHING:
                                out << "<font "
                                       "color=lightgreen>Attaching</font>";
                                break;
                            case NProto::PATH_ATTACH_STATE_DETACHED:
                                out << "<font color=red>Detached</font>";
                                break;
                            case NProto::PATH_ATTACH_STATE_DETACHING:
                                out << "<font "
                                       "color=lightcoral>Detaching</font>";
                                break;
                            default:
                                out << "unknown state";
                                break;
                        }
                    }
                    TABLED () {
                        out << State->GetPathGeneration(
                            agent.GetAgentId(),
                            path);
                    }
                    TABLED () {
                        const auto renderAttachButton =
                            state == NProto::PATH_ATTACH_STATE_DETACHING ||
                            state == NProto::PATH_ATTACH_STATE_DETACHED;
                        const auto buttonText =
                            TString(renderAttachButton ? "Attach" : "Detach") +
                            " Path";
                        const NProto::EPathAttachState desiriableState =
                            renderAttachButton
                                ? NProto::PATH_ATTACH_STATE_ATTACHING
                                : NProto::PATH_ATTACH_STATE_DETACHING;

                        out << Sprintf(
                            R"(<form name="updatePathAttachState" method="POST">
                                <input type='hidden' name='action' value='updatePathAttachState'/>
                                <input type='hidden' name='AgentID' value='%s'/>
                                <input type='hidden' name='Path' value='%s'/>
                                <input type='hidden' name='NewState' value='%s'/>
                                <input type='hidden' name='TabletID' value='%lu'/>
                                <button type="submit">%s</button>
                            </form>)",
                            agent.GetAgentId().c_str(),
                            path.c_str(),
                            NProto::EPathAttachState_Name(desiriableState)
                                .c_str(),
                            TabletID(),
                            buttonText.c_str());
                    }
                }
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
