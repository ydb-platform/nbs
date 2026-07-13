#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/storage/core/monitoring_utils.h>

#include <util/string/cast.h>
#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NMonitoringUtils;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TSendCmsRequestActor final
    : public TActorBootstrapped<TSendCmsRequestActor>
{
private:
    const TActorId Owner;
    const ui64 TabletID;
    const TRequestInfoPtr RequestInfo;
    const TString AgentID;
    const TString DevicePath;
    const NProto::TAction_EType ActionType;
    const bool DryRun;

public:
    TSendCmsRequestActor(
        const TActorId& owner,
        ui64 tabletID,
        TRequestInfoPtr requestInfo,
        TString agentId,
        TString devicePath,
        NProto::TAction_EType actionType,
        bool dryRun);

    void Bootstrap(const TActorContext& ctx);

private:
    void Notify(
        const TActorContext& ctx,
        const TString& message,
        EAlertLevel alertLevel);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error,
        TDuration timeout = {},
        const TVector<TString>& dependentDiskIds = {});

    STFUNC(StateWork);

    void HandleUpdateCmsHostStateResponse(
        const TEvDiskRegistryPrivate::TEvUpdateCmsHostStateResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePurgeHostCmsResponse(
        const TEvDiskRegistryPrivate::TEvPurgeHostCmsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleUpdateCmsHostDeviceStateResponse(
        const TEvDiskRegistryPrivate::TEvUpdateCmsHostDeviceStateResponse::TPtr&
            ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TSendCmsRequestActor::TSendCmsRequestActor(
    const TActorId& owner,
    ui64 tabletID,
    TRequestInfoPtr requestInfo,
    TString agentId,
    TString devicePath,
    NProto::TAction_EType actionType,
    bool dryRun)
    : Owner(owner)
    , TabletID(tabletID)
    , RequestInfo(std::move(requestInfo))
    , AgentID(std::move(agentId))
    , DevicePath(std::move(devicePath))
    , ActionType(actionType)
    , DryRun(dryRun)
{}

void TSendCmsRequestActor::Bootstrap(const TActorContext& ctx)
{
    switch (ActionType) {
        case NProto::TAction::ADD_HOST: {
            auto request = std::make_unique<
                TEvDiskRegistryPrivate::TEvUpdateCmsHostStateRequest>(
                AgentID,
                NProto::AGENT_STATE_ONLINE,
                /*customMessage=*/"monpage",
                DryRun);

            NCloud::Send(ctx, Owner, std::move(request));
            break;
        }

        case NProto::TAction::REMOVE_HOST: {
            auto request = std::make_unique<
                TEvDiskRegistryPrivate::TEvUpdateCmsHostStateRequest>(
                AgentID,
                NProto::AGENT_STATE_WARNING,
                /*customMessage=*/"monpage",
                DryRun);

            NCloud::Send(ctx, Owner, std::move(request));
            break;
        }

        case NProto::TAction::PURGE_HOST: {
            auto request = std::make_unique<
                TEvDiskRegistryPrivate::TEvPurgeHostCmsRequest>(
                AgentID,
                /*customMessage=*/"monpage",
                DryRun);

            NCloud::Send(ctx, Owner, std::move(request));
            break;
        }

        case NProto::TAction::ADD_DEVICE: {
            auto request = std::make_unique<
                TEvDiskRegistryPrivate::TEvUpdateCmsHostDeviceStateRequest>(
                AgentID,
                DevicePath,
                NProto::DEVICE_STATE_ONLINE,
                /*customMessage=*/"monpage",
                /*shouldResumeDevice=*/false,
                DryRun);

            NCloud::Send(ctx, Owner, std::move(request));
            break;
        }

        case NProto::TAction::REMOVE_DEVICE: {
            auto request = std::make_unique<
                TEvDiskRegistryPrivate::TEvUpdateCmsHostDeviceStateRequest>(
                AgentID,
                DevicePath,
                NProto::DEVICE_STATE_WARNING,
                /*customMessage=*/"monpage",
                /*shouldResumeDevice=*/false,
                DryRun);

            NCloud::Send(ctx, Owner, std::move(request));
            break;
        }

        default:
            ReplyAndDie(
                ctx,
                MakeError(
                    E_ARGUMENT,
                    TStringBuilder() << "Invalid CMS request: "
                                     << static_cast<ui32>(ActionType)));
            return;
    }

    Become(&TThis::StateWork);
}

void TSendCmsRequestActor::Notify(
    const TActorContext& ctx,
    const TString& message,
    EAlertLevel alertLevel)
{
    TStringStream out;
    BuildNotifyPageWithRedirect(
        out,
        message,
        TStringBuilder() << "../tablets/app?action=agent&TabletID=" << TabletID
                         << "&AgentID=" << AgentID,
        alertLevel);

    auto response = std::make_unique<NMon::TEvRemoteHttpInfoRes>(out.Str());
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
}

void TSendCmsRequestActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error,
    TDuration timeout,
    const TVector<TString>& dependentDiskIds)
{
    const auto actionName = NProto::TAction_EType_Name(ActionType);
    if (!HasError(error)) {
        Notify(
            ctx,
            TStringBuilder()
                << "CMS request " << actionName << " successfully completed",
            EAlertLevel::SUCCESS);
    } else {
        TStringBuilder message;
        message << "failed to send CMS request " << actionName << " for agent "
                << AgentID.Quote() << ": " << FormatError(error);
        if (timeout) {
            message << ", retry timeout: " << timeout.Seconds() << "s";
        }
        if (!dependentDiskIds.empty()) {
            message << ", dependent disks: " << JoinSeq(", ", dependentDiskIds);
        }

        Notify(ctx, message, EAlertLevel::DANGER);
    }

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvDiskRegistryPrivate::TEvOperationCompleted>());

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TSendCmsRequestActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

void TSendCmsRequestActor::HandleUpdateCmsHostStateResponse(
    const TEvDiskRegistryPrivate::TEvUpdateCmsHostStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* response = ev->Get();

    ReplyAndDie(
        ctx,
        response->GetError(),
        response->Timeout,
        response->DependentDiskIds);
}

void TSendCmsRequestActor::HandlePurgeHostCmsResponse(
    const TEvDiskRegistryPrivate::TEvPurgeHostCmsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* response = ev->Get();

    ReplyAndDie(
        ctx,
        response->GetError(),
        response->Timeout,
        response->DependentDiskIds);
}

void TSendCmsRequestActor::HandleUpdateCmsHostDeviceStateResponse(
    const TEvDiskRegistryPrivate::TEvUpdateCmsHostDeviceStateResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* response = ev->Get();

    ReplyAndDie(
        ctx,
        response->GetError(),
        response->Timeout,
        response->DependentDiskIds);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TSendCmsRequestActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvDiskRegistryPrivate::TEvUpdateCmsHostStateResponse,
            HandleUpdateCmsHostStateResponse);

        HFunc(
            TEvDiskRegistryPrivate::TEvPurgeHostCmsResponse,
            HandlePurgeHostCmsResponse);

        HFunc(
            TEvDiskRegistryPrivate::TEvUpdateCmsHostDeviceStateResponse,
            HandleUpdateCmsHostDeviceStateResponse);

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

void TDiskRegistryActor::HandleHttpInfo_SendCmsHostRequest(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    if (!Config->GetEnableToChangeStatesFromDiskRegistryMonpage()) {
        RejectHttpRequest(
            ctx,
            *requestInfo,
            "Can't send CMS request from monpage");
        return;
    }

    const auto& actionRaw = params.Get("CmsAction");
    const auto& agentId = params.Get("AgentID");
    const bool dryRun = params.Has("DryRun");

    if (!actionRaw) {
        RejectHttpRequest(ctx, *requestInfo, "No CMS request is given");
        return;
    }

    if (!agentId) {
        RejectHttpRequest(ctx, *requestInfo, "No agent id is given");
        return;
    }

    ui32 actionTypeValue = static_cast<ui32>(NProto::TAction::UNKNOWN);
    if (!TryFromString(actionRaw, actionTypeValue)) {
        RejectHttpRequest(
            ctx,
            *requestInfo,
            TStringBuilder()
                << "Could not parse CMS request type: " << actionRaw);
        return;
    }
    const auto actionType = static_cast<NProto::TAction_EType>(actionTypeValue);

    switch (actionType) {
        case NProto::TAction::ADD_HOST:
        case NProto::TAction::REMOVE_HOST:
        case NProto::TAction::PURGE_HOST:
            break;

        default:
            RejectHttpRequest(
                ctx,
                *requestInfo,
                TStringBuilder() << "Invalid CMS request type: " << actionRaw);
            return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Send CMS request %s for agent[%s] from monitoring page (dryRun=%s)",
        LogTitle.GetWithTime().c_str(),
        NProto::TAction_EType_Name(actionType).c_str(),
        agentId.c_str(),
        dryRun ? "true" : "false");

    auto actor = NCloud::Register<TSendCmsRequestActor>(
        ctx,
        SelfId(),
        TabletID(),
        std::move(requestInfo),
        agentId,
        TString(),   // devicePath
        actionType,
        dryRun);

    Actors.insert(actor);
}

void TDiskRegistryActor::HandleHttpInfo_SendCmsDeviceRequest(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    if (!Config->GetEnableToChangeStatesFromDiskRegistryMonpage()) {
        RejectHttpRequest(
            ctx,
            *requestInfo,
            "Can't send CMS request from monpage");
        return;
    }

    const auto& actionRaw = params.Get("CmsAction");
    const auto& deviceName = params.Get("DeviceName");
    const auto& agentId = params.Get("AgentID");
    const bool dryRun = params.Has("DryRun");

    if (!actionRaw) {
        RejectHttpRequest(ctx, *requestInfo, "No CMS request is given");
        return;
    }

    if (!deviceName) {
        RejectHttpRequest(ctx, *requestInfo, "No device name is given");
        return;
    }

    if (!agentId) {
        RejectHttpRequest(ctx, *requestInfo, "No agent id is given");
        return;
    }

    ui32 actionTypeValue = static_cast<ui32>(NProto::TAction::UNKNOWN);
    if (!TryFromString(actionRaw, actionTypeValue)) {
        RejectHttpRequest(
            ctx,
            *requestInfo,
            TStringBuilder()
                << "Could not parse CMS request type: " << actionRaw);
        return;
    }
    const auto actionType = static_cast<NProto::TAction_EType>(actionTypeValue);

    switch (actionType) {
        case NProto::TAction::ADD_DEVICE:
        case NProto::TAction::REMOVE_DEVICE:
            break;

        default:
            RejectHttpRequest(
                ctx,
                *requestInfo,
                TStringBuilder() << "Invalid CMS request type: " << actionRaw);
            return;
    }

    LOG_INFO(
        ctx,
        TBlockStoreComponents::DISK_REGISTRY,
        "%s Send CMS request %s for path[%s] on agent[%s] from monitoring page "
        "(dryRun=%s)",
        LogTitle.GetWithTime().c_str(),
        NProto::TAction_EType_Name(actionType).c_str(),
        deviceName.c_str(),
        agentId.c_str(),
        dryRun ? "true" : "false");

    auto actor = NCloud::Register<TSendCmsRequestActor>(
        ctx,
        SelfId(),
        TabletID(),
        std::move(requestInfo),
        agentId,
        deviceName,
        actionType,
        dryRun);

    Actors.insert(actor);
}

}   // namespace NCloud::NBlockStore::NStorage
