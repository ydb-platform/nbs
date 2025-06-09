#include "service_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NStorage;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TBackupTabletBootInfosActor final
    : public TActorBootstrapped<TBackupTabletBootInfosActor>
{
private:
    const TRequestInfoPtr RequestInfo;

    NProto::TError Error;

public:
    explicit TBackupTabletBootInfosActor(TRequestInfoPtr requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleBackupTabletBootInfosResponse(
        const TEvHiveProxy::TEvBackupTabletBootInfosResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TBackupTabletBootInfosActor::TBackupTabletBootInfosActor(
        TRequestInfoPtr requestInfo)
    : RequestInfo(std::move(requestInfo))
{}

void TBackupTabletBootInfosActor::Bootstrap(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvHiveProxy::TEvBackupTabletBootInfosRequest>();
    Become(&TThis::StateWork);
    NCloud::Send(ctx, MakeHiveProxyServiceId(), std::move(request));
}

void TBackupTabletBootInfosActor::ReplyAndDie(const TActorContext& ctx)
{
    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(Error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_backuptabletbootinfos",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TBackupTabletBootInfosActor::HandleBackupTabletBootInfosResponse(
    const TEvHiveProxy::TEvBackupTabletBootInfosResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        Error = msg->GetError();
    }

    ReplyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TBackupTabletBootInfosActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvHiveProxy::TEvBackupTabletBootInfosResponse,
            HandleBackupTabletBootInfosResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateBackupTabletBootInfosActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    Y_UNUSED(input);

    return {std::make_unique<TBackupTabletBootInfosActor>(
        std::move(requestInfo))};
}

}   // namespace NCloud::NBlockStore::NStorage
