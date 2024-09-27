#include "ss_proxy_actor.h"

#include <ydb/core/tx/tx_proxy/proxy.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NCloud::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDescribeSchemeActor final
    : public TActorBootstrapped<TDescribeSchemeActor>
{
private:
    const int LogComponent;
    const TSSProxyActor::TRequestInfo RequestInfo;

    const TString SchemeShardDir;
    const TString Path;

public:
    TDescribeSchemeActor(
        int logComponent,
        TSSProxyActor::TRequestInfo requestInfo,
        TString schemeShardDir,
        TString path);

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
        int logComponent,
        TSSProxyActor::TRequestInfo requestInfo,
        TString schemeShardDir,
        TString path)
    : LogComponent(logComponent)
    , RequestInfo(std::move(requestInfo))
    , SchemeShardDir(std::move(schemeShardDir))
    , Path(std::move(path))
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

    if (HandleError(ctx, MakeSchemeShardError(record.GetStatus(), record.GetReason()))) {
        return;
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
            HandleUnexpectedEvent(ev, LogComponent);
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

    NCloud::Register(
        ctx,
        std::make_unique<TDescribeSchemeActor>(
            LogComponent,
            TRequestInfo(ev->Sender, ev->Cookie),
            SchemeShardDir,
            msg->Path));
}

}   // namespace NCloud::NStorage
