#include "service_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/hive_proxy/tablet_boot_info.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NStorage;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TGetTabletBootInfosActor final
    : public TActorBootstrapped<TGetTabletBootInfosActor>
{
private:
    const TRequestInfoPtr RequestInfo;

public:
    explicit TGetTabletBootInfosActor(TRequestInfoPtr requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvExecuteActionResponse> response);

private:
    STFUNC(StateWork);

    void HandleGetTabletBootInfosResponse(
        const TEvHiveProxy::TEvGetTabletBootInfosResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TGetTabletBootInfosActor::TGetTabletBootInfosActor(
        TRequestInfoPtr requestInfo)
    : RequestInfo(std::move(requestInfo))
{}

void TGetTabletBootInfosActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    NCloud::Send(
        ctx,
        MakeHiveProxyServiceId(),
        std::make_unique<TEvHiveProxy::TEvGetTabletBootInfosRequest>());
}

void TGetTabletBootInfosActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvExecuteActionResponse> response)
{
    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_gettabletbootinfos",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TGetTabletBootInfosActor::HandleGetTabletBootInfosResponse(
    const TEvHiveProxy::TEvGetTabletBootInfosResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(
            msg->GetError());
        ReplyAndDie(ctx, std::move(response));
        return;
    }

    NPrivateProto::TGetTabletBootInfosResponse result;
    for (const auto& info: msg->TabletBootInfos) {
        auto* entry = result.AddTabletBootInfos();
        entry->SetSerializedTabletStorageInfo(info.StorageInfoProto.SerializeAsString());
        entry->SetSuggestedGeneration(info.SuggestedGeneration);
    }

    TString output;
    google::protobuf::util::MessageToJsonString(result, &output);

    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);
    ReplyAndDie(ctx, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TGetTabletBootInfosActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvHiveProxy::TEvGetTabletBootInfosResponse,
            HandleGetTabletBootInfosResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateGetTabletBootInfosActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    Y_UNUSED(input);

    return {std::make_unique<TGetTabletBootInfosActor>(std::move(requestInfo))};
}

}   // namespace NCloud::NBlockStore::NStorage
