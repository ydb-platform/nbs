#include "service_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/private/api/protos/tablet.pb.h>

#include <ydb/core/base/tablet.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <google/protobuf/util/json_util.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TKillTabletActionActor final
    : public TActorBootstrapped<TKillTabletActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    ui64 TabletId = 0;

public:
    TKillTabletActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void GetStorageInfo(const TActorContext& ctx);
    void KillTablet(const TActorContext& ctx);

    void HandleSuccess(const TActorContext& ctx, const TString& output);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////

TKillTabletActionActor::TKillTabletActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TKillTabletActionActor::Bootstrap(const TActorContext& ctx)
{
    NPrivateProto::TKillTabletRequest request;
    if (!google::protobuf::util::JsonStringToMessage(Input, &request).ok()) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Failed to parse input"));
        return;
    }

    TabletId = request.GetTabletId();

    KillTablet(ctx);
    Become(&TThis::StateWork);
}

void TKillTabletActionActor::KillTablet(const TActorContext& ctx)
{
    ctx.Register(CreateTabletKiller(TabletId));
    HandleSuccess(ctx, TStringBuilder() << "Killing tablet");
}

void TKillTabletActionActor::HandleSuccess(
    const TActorContext& ctx,
    const TString& output)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_killtablet",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TKillTabletActionActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_killtablet",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TKillTabletActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateKillTabletActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TKillTabletActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
