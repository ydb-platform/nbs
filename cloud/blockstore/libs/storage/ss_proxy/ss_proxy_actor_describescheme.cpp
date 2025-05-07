#include "ss_proxy_actor.h"

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/ss_proxy/ss_proxy_events_private.h>

#include <cloud/storage/core/libs/common/helpers.h>

#include <contrib/ydb/core/tx/tx_proxy/proxy.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDescribeSchemeActor final
    : public TActorBootstrapped<TDescribeSchemeActor>
{
private:
    const TRequestInfoPtr RequestInfo;

    const TStorageConfigPtr Config;
    const TString Path;
    TActorId PathDescriptionBackup;

public:
    TDescribeSchemeActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TString path,
        TActorId pathDescriptionBackup);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeScheme(const TActorContext& ctx);

    bool HandleError(const TActorContext& ctx, const NProto::TError& error);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvSSProxy::TEvDescribeSchemeResponse> response);

private:
    STFUNC(StateWork);

    void HandleDescribeSchemeResult(
        const TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDescribeSchemeActor::TDescribeSchemeActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TString path,
        TActorId pathDescriptionBackup)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , Path(std::move(path))
    , PathDescriptionBackup(std::move(pathDescriptionBackup))
{}

void TDescribeSchemeActor::Bootstrap(const TActorContext& ctx)
{
    DescribeScheme(ctx);
    Become(&TThis::StateWork);
}

void TDescribeSchemeActor::DescribeScheme(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvTxUserProxy::TEvNavigate>();
    request->Record.MutableDescribePath()->SetPath(Path);
    request->Record.SetDatabaseName(Config->GetSchemeShardDir());

    LWTRACK(
        RequestSent_Proxy,
        RequestInfo->CallContext->LWOrbit,
        "Navigate",
        RequestInfo->CallContext->RequestId);

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
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDescribeSchemeActor::HandleDescribeSchemeResult(
    const TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->GetRecord();

    auto error = MakeSchemeShardError(record.GetStatus(), record.GetReason());

    LWTRACK(
        ResponseReceived_Proxy,
        RequestInfo->CallContext->LWOrbit,
        "DescribeSchemeResult",
        RequestInfo->CallContext->RequestId);

    if (HasError(error)) {
        auto status =
            static_cast<NKikimrScheme::EStatus>(STATUS_FROM_CODE(error.GetCode()));

        if (status == NKikimrScheme::StatusNotAvailable) {
            error.SetCode(E_REJECTED);
        }

        // TODO: return E_NOT_FOUND instead of StatusPathDoesNotExist
        if (status == NKikimrScheme::StatusPathDoesNotExist) {
            SetErrorProtoFlag(error, NCloud::NProto::EF_SILENT);
        }
    }

    if (HandleError(ctx, error)) {
        return;
    }

    if (PathDescriptionBackup) {
        auto updateRequest =
            std::make_unique<TEvSSProxyPrivate::TEvUpdatePathDescriptionBackupRequest>(
                record.GetPath(),
                record.GetPathDescription()
            );
        NCloud::Send(ctx, PathDescriptionBackup, std::move(updateRequest));
    }

    auto response = std::make_unique<TEvSSProxy::TEvDescribeSchemeResponse>(
        record.GetPath(),
        record.GetPathDescription());

    ReplyAndDie(ctx, std::move(response));
}

STFUNC(TDescribeSchemeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSchemeShard::TEvDescribeSchemeResult, HandleDescribeSchemeResult);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SS_PROXY,
                __PRETTY_FUNCTION__);
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

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    NCloud::Register<TDescribeSchemeActor>(
        ctx,
        std::move(requestInfo),
        Config,
        msg->Path,
        PathDescriptionBackup);
}

}   // namespace NCloud::NBlockStore::NStorage
