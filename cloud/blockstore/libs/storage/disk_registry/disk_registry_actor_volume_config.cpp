#include "disk_registry_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUpdateActor final
    : public TActorBootstrapped<TUpdateActor>
{
private:
    const TActorId Owner;
    const TRequestInfoPtr RequestInfo;
    const TVolumeConfig Config;
    const ui64 SeqNo;

public:
    TUpdateActor(
        const TActorId& owner,
        TRequestInfoPtr request,
        TVolumeConfig config,
        ui64 seqNo);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeVolume(const TActorContext& ctx, const TString& diskId);
    void ReplyAndDie(const TActorContext& ctx, NProto::TError error = {});

    void UpdateVolumeConfig(
        const TActorContext& ctx,
        const TString& path,
        ui64 pathId,
        ui64 pathVersion,
        ui32 configVersion);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleDescribeVolumeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleModifySchemeResponse(
        const TEvSSProxy::TEvModifySchemeResponse::TPtr& ev,
        const TActorContext& ctx);

    STFUNC(StateWork);
};

TUpdateActor::TUpdateActor(
        const TActorId& owner,
        TRequestInfoPtr requestInfo,
        TVolumeConfig config,
        ui64 seqNo)
    : Owner(owner)
    , RequestInfo(requestInfo)
    , Config(std::move(config))
    , SeqNo(seqNo)
{}

void TUpdateActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);
    DescribeVolume(ctx, Config.GetDiskId());
}

void TUpdateActor::DescribeVolume(const TActorContext& ctx, const TString& diskId)
{
    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(diskId));
}

void TUpdateActor::HandleDescribeVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& diskId = Config.GetDiskId();
    auto& error = msg->GetError();

    if (HasError(error)) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "Failed to update " << diskId.Quote() << " volume config (DescribeVolume): " <<
            FormatError(error));

        return ReplyAndDie(ctx, error);
    }

    const auto& path = msg->Path;
    const auto& description = msg->PathDescription;
    const auto& config = description.GetBlockStoreVolumeDescription().GetVolumeConfig();

    UpdateVolumeConfig(
        ctx,
        path,
        description.GetSelf().GetPathId(),
        description.GetSelf().GetPathVersion(),
        config.GetVersion());
}

void TUpdateActor::UpdateVolumeConfig(
    const TActorContext& ctx,
    const TString& path,
    ui64 pathId,
    ui64 pathVersion,
    ui32 configVersion)
{
    TStringBuf dir;
    TStringBuf name;
    TStringBuf(path).RSplit('/', dir, name);

    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetWorkingDir(TString{dir});
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterBlockStoreVolume);

    auto* op = modifyScheme.MutableAlterBlockStoreVolume();
    op->SetName(TString{name});
    auto* config = op->MutableVolumeConfig();
    config->CopyFrom(Config);
    config->SetVersion(configVersion);

    auto* applyIf = modifyScheme.MutableApplyIf()->Add();
    applyIf->SetPathId(pathId);
    applyIf->SetPathVersion(pathVersion);

    auto& diskId = Config.GetDiskId();
    LOG_INFO_S(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER,
        "Sending new " << diskId.Quote() << " volume config");

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvModifySchemeRequest>(std::move(modifyScheme)));
}

void TUpdateActor::HandleModifySchemeResponse(
    const TEvSSProxy::TEvModifySchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& diskId = Config.GetDiskId();
    auto& error = msg->GetError();

    if (HasError(error)) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "Failed to update " << diskId.Quote() << " volume config (ModifyScheme): " <<
            FormatError(error));
    } else {
        LOG_INFO_S(ctx, TBlockStoreComponents::DISK_REGISTRY_WORKER,
            "Updated " << diskId.Quote() << " volume config");
    }

    ReplyAndDie(ctx, error);
}

void TUpdateActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeTabletIsDeadError(E_REJECTED, __LOCATION__));
}

void TUpdateActor::ReplyAndDie(const TActorContext& ctx, NProto::TError error)
{
    NCloud::Reply(ctx,
        *RequestInfo,
        std::make_unique<TEvDiskRegistryPrivate::TEvUpdateVolumeConfigResponse>(
            std::move(error),
            std::move(Config),
            SeqNo));

    NCloud::Send(
        ctx,
        Owner,
        std::make_unique<TEvDiskRegistryPrivate::TEvOperationCompleted>());

    Die(ctx);
}

STFUNC(TUpdateActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvSSProxy::TEvDescribeVolumeResponse, HandleDescribeVolumeResponse);
        HFunc(TEvSSProxy::TEvModifySchemeResponse, HandleModifySchemeResponse);

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

void TDiskRegistryActor::UpdateVolumeConfigs(
    const TActorContext& ctx,
    const TVector<TString>& diskIds,
    TPrincipalTaskId principalTaskId)
{
    Y_DEBUG_ABORT_UNLESS(!diskIds.empty());
    for (const auto& diskId: diskIds) {
        NCloud::Send(
            ctx,
            ctx.SelfID,
            std::make_unique<TEvDiskRegistryPrivate::TEvUpdateVolumeConfigRequest>(diskId),
            UpdateVolumeConfigsWaiters.StartDependentTaskAwait(principalTaskId));
    }
}

void TDiskRegistryActor::UpdateVolumeConfigs(const NActors::TActorContext& ctx)
{
    auto outdatedVolumeConfigs = State->GetOutdatedVolumeConfigs();
    if (outdatedVolumeConfigs) {
        UpdateVolumeConfigs(ctx, outdatedVolumeConfigs, INVALID_TASK_ID);
    }
}

void TDiskRegistryActor::HandleUpdateVolumeConfig(
    const TEvDiskRegistryPrivate::TEvUpdateVolumeConfigRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_REGISTRY_COUNTER(UpdateVolumeConfig);

    const TDependentTaskId dependentTaskId = ev->Cookie;
    auto* msg = ev->Get();

    auto [config, seqNo] = State->GetVolumeConfigUpdate(msg->DiskId);

    if (config.HasDiskId()) {
        auto requestInfo = CreateRequestInfo(
            ev->Sender,
            dependentTaskId,
            msg->CallContext);

        auto actor = NCloud::Register<TUpdateActor>(
            ctx,
            ctx.SelfID,
            std::move(requestInfo),
            std::move(config),
            seqNo);

        Actors.insert(actor);
    } else {
        LOG_INFO_S(ctx, TBlockStoreComponents::DISK_REGISTRY,
            "Disk " << msg->DiskId.Quote() <<
            " has been deleted, discarding volume config update");
        UpdateVolumeConfigsWaiters.FinishDependentTaskAwait(
            dependentTaskId,
            ctx);
    }
}

void TDiskRegistryActor::HandleUpdateVolumeConfigResponse(
    const TEvDiskRegistryPrivate::TEvUpdateVolumeConfigResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const TDependentTaskId dependentTaskId = ev->Cookie;
    auto* msg = ev->Get();
    auto& diskId = msg->Config.GetDiskId();
    auto& error = msg->GetError();
    auto kind = GetErrorKind(error);

    if (kind == EErrorKind::Success) {
        using google::protobuf::util::MessageDifferencer;
        auto [config, seqNo] = State->GetVolumeConfigUpdate(diskId);

        if (msg->SeqNo == seqNo && MessageDifferencer::Equals(msg->Config, config)) {
            FinishVolumeConfigUpdate(ctx, diskId);
        }
        BackoffDelayProvider.Reset();
        UpdateVolumeConfigsWaiters.FinishDependentTaskAwait(
            dependentTaskId,
            ctx);
        return;
    }

    auto code = error.GetCode();
    const auto statusPathDoesNotExist =
        MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist);

    if (kind == EErrorKind::ErrorRetriable
            || code == E_ABORTED
            || code == statusPathDoesNotExist)
    {
        auto* request = new TEvDiskRegistryPrivate::TEvUpdateVolumeConfigRequest(diskId);

        ctx.Schedule(
            BackoffDelayProvider.GetDelay(),
            std::make_unique<IEventHandle>(
                ctx.SelfID,
                ctx.SelfID,
                request,
                0,
                dependentTaskId));
        BackoffDelayProvider.IncreaseDelay();
        return;
    }

    LOG_ERROR_S(ctx, TBlockStoreComponents::DISK_REGISTRY,
        "Unable to update " << diskId.Quote() << " volume config: " << FormatError(error));
    BackoffDelayProvider.Reset();
    UpdateVolumeConfigsWaiters.FinishDependentTaskAwait(dependentTaskId, ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDiskRegistryActor::FinishVolumeConfigUpdate(
    const TActorContext& ctx,
    const TString& diskId)
{
    NCloud::Send(
        ctx,
        ctx.SelfID,
        std::make_unique<TEvDiskRegistryPrivate::TEvFinishVolumeConfigUpdateRequest>(diskId));
}

void TDiskRegistryActor::HandleFinishVolumeConfigUpdate(
    const TEvDiskRegistryPrivate::TEvFinishVolumeConfigUpdateRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    ExecuteTx<TFinishVolumeConfigUpdate>(
        ctx,
        std::move(requestInfo),
        std::move(msg->DiskId));
}

bool TDiskRegistryActor::PrepareFinishVolumeConfigUpdate(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TFinishVolumeConfigUpdate& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TDiskRegistryActor::ExecuteFinishVolumeConfigUpdate(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxDiskRegistry::TFinishVolumeConfigUpdate& args)
{
    Y_UNUSED(ctx);

    TDiskRegistryDatabase db(tx.DB);
    State->DeleteOutdatedVolumeConfig(db, args.DiskId);
}

void TDiskRegistryActor::CompleteFinishVolumeConfigUpdate(
    const TActorContext& ctx,
    TTxDiskRegistry::TFinishVolumeConfigUpdate& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(args);
}

}   // namespace NCloud::NBlockStore::NStorage
