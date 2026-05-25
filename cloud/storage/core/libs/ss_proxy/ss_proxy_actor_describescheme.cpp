#include "ss_proxy_actor.h"

#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/ss_proxy/ss_proxy_events_private.h>

#include <contrib/ydb/core/tx/scheme_cache/scheme_cache.h>
#include <contrib/ydb/core/tx/tx_proxy/proxy.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;
using namespace NKikimr::NSchemeCache;

namespace {

////////////////////////////////////////////////////////////////////////////////

std::optional<NKikimrSchemeOp::EPathType> ConvertSchemeCacheKind(
    TSchemeCacheNavigate::EKind kind)
{
    switch (kind) {
        case TSchemeCacheNavigate::KindSubdomain:
            return NKikimrSchemeOp::EPathTypeSubDomain;

        case TSchemeCacheNavigate::KindPath:
            return NKikimrSchemeOp::EPathTypeDir;

        case TSchemeCacheNavigate::KindFileStore:
            return NKikimrSchemeOp::EPathTypeFileStore;

        case TSchemeCacheNavigate::KindBlockStoreVolume:
            return NKikimrSchemeOp::EPathTypeBlockStoreVolume;

        default:
            return std::nullopt;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TDescribeSchemeActor final
    : public TActorBootstrapped<TDescribeSchemeActor>
{
private:
    const int LogComponent;
    const TSSProxyActor::TRequestInfo RequestInfo;
    const TString SchemeShardDir;
    const TString Path;
    const bool UseSchemeCache;
    TActorId PathDescriptionBackup;

public:
    TDescribeSchemeActor(
        int logComponent,
        TSSProxyActor::TRequestInfo requestInfo,
        TString schemeShardDir,
        TString path,
        bool useSchemeCache,
        TActorId pathDescriptionBackup);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeScheme(const TActorContext& ctx);

    bool HandleError(
        const TActorContext& ctx,
        const NProto::TError& error);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvSSProxy::TEvDescribeSchemeResponse> response);

private:
    STFUNC(StateWork);

    void HandleDescribeSchemeResult(
        const TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev,
        const TActorContext& ctx);

    void HandleDescribeSchemeResult(
        const TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev,
        const TActorContext& ctx);

    void HandleNavigateKeySetUndelivery(
        const TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDescribeSchemeActor::TDescribeSchemeActor(
        int logComponent,
        TSSProxyActor::TRequestInfo requestInfo,
        TString schemeShardDir,
        TString path,
        bool useSchemeCache,
        TActorId pathDescriptionBackup)
    : LogComponent(logComponent)
    , RequestInfo(std::move(requestInfo))
    , SchemeShardDir(std::move(schemeShardDir))
    , Path(std::move(path))
    , UseSchemeCache(useSchemeCache)
    , PathDescriptionBackup(std::move(pathDescriptionBackup))
{}

void TDescribeSchemeActor::Bootstrap(const TActorContext& ctx)
{
    DescribeScheme(ctx);
    Become(&TThis::StateWork);
}

void TDescribeSchemeActor::DescribeScheme(const TActorContext& ctx)
{
    if (UseSchemeCache) {
        auto navigateCacheRequest = std::make_unique<TSchemeCacheNavigate>();
        navigateCacheRequest->DatabaseName = SchemeShardDir;

        auto& entry = navigateCacheRequest->ResultSet.emplace_back();
        entry.Operation = TSchemeCacheNavigate::OpList;
        entry.SyncVersion = true;
        entry.Path = SplitPath(Path);

        auto request =
            std::make_unique<TEvTxProxySchemeCache::TEvNavigateKeySet>(
                navigateCacheRequest.release());

        auto event = std::make_unique<IEventHandle>(
            MakeSchemeCacheID(),
            ctx.SelfID,
            request.release(),
            IEventHandle::FlagForwardOnNondelivery,
            RequestInfo.Cookie,
            &ctx.SelfID);

        ctx.Send(event.release());
        return;
    }

    auto request = std::make_unique<TEvTxUserProxy::TEvNavigate>();
    request->Record.MutableDescribePath()->SetPath(Path);
    request->Record.SetDatabaseName(SchemeShardDir);

    NCloud::Send(ctx, MakeTxProxyID(), std::move(request));
}

bool TDescribeSchemeActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (FAILED(error.GetCode())) {
        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvDescribeSchemeResponse>(error));
        return true;
    }
    return false;
}

void TDescribeSchemeActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvSSProxy::TEvDescribeSchemeResponse> response)
{
    NCloud::Reply(ctx, RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDescribeSchemeActor::HandleDescribeSchemeResult(
    const TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->GetRecord();

    if (HandleError(ctx, MakeDescribeSchemeError(record))) {
        return;
    }

    if (PathDescriptionBackup) {
        auto updateRequest =
            std::make_unique<
                TEvSSProxyPrivate::TEvUpdatePathDescriptionBackupRequest>(
                    record.GetPath(),
                    record.GetPathDescription());

        NCloud::Send(ctx, PathDescriptionBackup, std::move(updateRequest));
    }

    auto response =
        std::make_unique<TEvSSProxy::TEvDescribeSchemeResponse>(
            record.GetPath(),
            record.GetPathDescription());

    ReplyAndDie(ctx, std::move(response));
}

void TDescribeSchemeActor::HandleDescribeSchemeResult(
    const TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* record = ev->Get()->Request.Get();

    Y_DEBUG_ABORT_UNLESS(record->ResultSet.size() == 1);

    if (record->ResultSet.size() != 1) {
        HandleError(
            ctx,
            MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "SchemeCache ResultSet returned unexpected "
                    << "number of entries: "
                    << record->ResultSet.size()));
        return;
    }

    const auto& entry = record->ResultSet.front();

    if (record->ErrorCount > 0) {
        switch (entry.Status) {
            case TSchemeCacheNavigate::EStatus::PathErrorUnknown:
            case TSchemeCacheNavigate::EStatus::RootUnknown: {
                auto error = MakeSchemeShardError(
                    NKikimrScheme::StatusPathDoesNotExist,
                    "Path doesn't exist");

                SetErrorProtoFlag(error, NCloud::NProto::EF_SILENT);

                HandleError(ctx, error);
                return;
            }

            default: {
                HandleError(
                    ctx,
                    MakeError(
                        E_REJECTED,
                        TStringBuilder()
                            << "SchemeCache resolve failed with uncertain "
                            << "result. Error Code: "
                            << static_cast<ui32>(entry.Status)));
                return;
            }
        }
    }

    NKikimrSchemeOp::TPathDescription pathDescription;

    if (entry.Self) {
        pathDescription.MutableSelf()->CopyFrom(entry.Self->Info);
    }

    if (entry.ListNodeEntry) {
        for (const auto& child: entry.ListNodeEntry->Children) {
            auto* childEntry = pathDescription.MutableChildren()->Add();

            auto pathType = ConvertSchemeCacheKind(child.Kind);

            if (!pathType) {
                HandleError(
                    ctx,
                    MakeError(
                        E_REJECTED,
                        TStringBuilder()
                            << "Unknown child path kind: "
                            << child.Kind));
                return;
            }

            childEntry->SetPathType(*pathType);
            childEntry->SetName(child.Name);
            childEntry->SetPathId(child.PathId.LocalPathId);
            childEntry->SetSchemeshardId(child.PathId.OwnerId);
        }
    }

    switch (entry.Kind) {
        case TSchemeCacheNavigate::KindFileStore:
            pathDescription.MutableFileStoreDescription()->CopyFrom(
                entry.FileStoreInfo->Description);

            pathDescription.MutableSelf()->SetPathType(
                NKikimrSchemeOp::EPathTypeFileStore);
            break;

        case TSchemeCacheNavigate::KindBlockStoreVolume:
            pathDescription.MutableBlockStoreVolumeDescription()->CopyFrom(
                entry.BlockStoreVolumeInfo->Description);

            pathDescription.MutableSelf()->SetPathType(
                NKikimrSchemeOp::EPathTypeBlockStoreVolume);
            break;

        case TSchemeCacheNavigate::KindSubdomain:
            pathDescription.MutableDomainDescription()->CopyFrom(
                entry.DomainDescription->Description);

            pathDescription.MutableSelf()->SetPathType(
                NKikimrSchemeOp::EPathTypeSubDomain);
            break;

        case TSchemeCacheNavigate::KindPath:
            pathDescription.MutableSelf()->SetPathType(
                NKikimrSchemeOp::EPathTypeDir);
            break;

        default:
            HandleError(
                ctx,
                MakeError(
                    E_REJECTED,
                    TStringBuilder()
                        << "Unknown path kind: "
                        << entry.Kind));
            return;
    }

    if (PathDescriptionBackup) {
        auto updateRequest =
            std::make_unique<
                TEvSSProxyPrivate::TEvUpdatePathDescriptionBackupRequest>(
                    Path,
                    pathDescription);

        NCloud::Send(ctx, PathDescriptionBackup, std::move(updateRequest));
    }

    ReplyAndDie(
        ctx,
        std::make_unique<TEvSSProxy::TEvDescribeSchemeResponse>(
            Path,
            std::move(pathDescription)));
}

void TDescribeSchemeActor::HandleNavigateKeySetUndelivery(
    const TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    HandleError(
        ctx,
        MakeError(E_REJECTED, "NavigateKeySet undelivered"));
}

STFUNC(TDescribeSchemeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvSchemeShard::TEvDescribeSchemeResult,
            HandleDescribeSchemeResult);

        HFunc(
            TEvTxProxySchemeCache::TEvNavigateKeySetResult,
            HandleDescribeSchemeResult);

        HFunc(
            TEvTxProxySchemeCache::TEvNavigateKeySet,
            HandleNavigateKeySetUndelivery);

        default:
            HandleUnexpectedEvent(ev, LogComponent, __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TSSProxyActor::HandleDescribeScheme(
    const TEvSSProxy::TEvDescribeSchemeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    NCloud::Register<TDescribeSchemeActor>(
        ctx,
        Config.LogComponent,
        TRequestInfo(ev->Sender, ev->Cookie),
        Config.SchemeShardDir,
        msg->Path,
        Config.UseSchemeCache,
        PathDescriptionBackup);
}

}   // namespace NCloud::NStorage
