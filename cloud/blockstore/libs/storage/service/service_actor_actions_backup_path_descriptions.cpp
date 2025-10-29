#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TBackupPathDescriptionsActor final
    : public TActorBootstrapped<TBackupPathDescriptionsActor>
{
private:
    const TRequestInfoPtr RequestInfo;

    NProto::TError Error;

public:
    explicit TBackupPathDescriptionsActor(TRequestInfoPtr requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleBackupPathDescriptionsResponse(
        const TEvSSProxy::TEvBackupPathDescriptionsResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TBackupPathDescriptionsActor::TBackupPathDescriptionsActor(
        TRequestInfoPtr requestInfo)
    : RequestInfo(std::move(requestInfo))
{}

void TBackupPathDescriptionsActor::Bootstrap(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvSSProxy::TEvBackupPathDescriptionsRequest>();
    Become(&TThis::StateWork);
    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TBackupPathDescriptionsActor::ReplyAndDie(const TActorContext& ctx)
{
    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(Error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_backuppathdescriptions",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TBackupPathDescriptionsActor::HandleBackupPathDescriptionsResponse(
    const TEvSSProxy::TEvBackupPathDescriptionsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        Error = msg->GetError();
    }

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TBackupPathDescriptionsActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvSSProxy::TEvBackupPathDescriptionsResponse,
            HandleBackupPathDescriptionsResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateBackupPathDescriptionsActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    Y_UNUSED(input);

    return {std::make_unique<TBackupPathDescriptionsActor>(
        std::move(requestInfo))};
}

}   // namespace NCloud::NBlockStore::NStorage
