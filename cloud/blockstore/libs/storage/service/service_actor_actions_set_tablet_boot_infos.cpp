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

class TSetTabletBootInfosActor final
    : public TActorBootstrapped<TSetTabletBootInfosActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    TVector<TTabletBootInfo> TabletBootInfos;

public:
    TSetTabletBootInfosActor(
        TRequestInfoPtr requestInfo,
        TVector<TTabletBootInfo> tabletBootInfos);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvExecuteActionResponse> response);

private:
    STFUNC(StateWork);

    void HandleSetTabletBootInfosResponse(
        const TEvHiveProxy::TEvSetTabletBootInfosResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TSetTabletBootInfosActor::TSetTabletBootInfosActor(
        TRequestInfoPtr requestInfo,
        TVector<TTabletBootInfo> tabletBootInfos)
    : RequestInfo(std::move(requestInfo))
    , TabletBootInfos(std::move(tabletBootInfos))
{}

void TSetTabletBootInfosActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    NCloud::Send(
        ctx,
        MakeHiveProxyServiceId(),
        std::make_unique<TEvHiveProxy::TEvSetTabletBootInfosRequest>(
            std::move(TabletBootInfos)));
}

void TSetTabletBootInfosActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvExecuteActionResponse> response)
{
    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_settabletbootinfos",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TSetTabletBootInfosActor::HandleSetTabletBootInfosResponse(
    const TEvHiveProxy::TEvSetTabletBootInfosResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    NProto::TError error;
    if (HasError(msg->GetError())) {
        error = msg->GetError();
    }

    auto response =
        std::make_unique<TEvService::TEvExecuteActionResponse>(error);
    ReplyAndDie(ctx, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TSetTabletBootInfosActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvHiveProxy::TEvSetTabletBootInfosResponse,
            HandleSetTabletBootInfosResponse);

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

TResultOrError<IActorPtr> TServiceActor::CreateSetTabletBootInfosActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    NPrivateProto::TSetTabletBootInfosRequest request;
    if (!google::protobuf::util::JsonStringToMessage(input, &request).ok()) {
        return MakeError(E_ARGUMENT, "Failed to parse input");
    }

    TVector<TTabletBootInfo> tabletBootInfos;
    tabletBootInfos.reserve(request.TabletBootInfosSize());
    for (const auto& entry: request.GetTabletBootInfos()) {
        NKikimrTabletBase::TTabletStorageInfo storageInfo;
        if (!storageInfo.ParseFromString(entry.GetSerializedTabletStorageInfo())) {
            return MakeError(E_ARGUMENT, "Failed to parse StorageInfo");
        }
        if (entry.GetTabletId() != storageInfo.GetTabletID()) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "TabletId mismatch: entry contains "
                    << entry.GetTabletId() << ", while storage info contains "
                    << storageInfo.GetTabletID());
        }
        tabletBootInfos.emplace_back(
            std::move(storageInfo),
            entry.GetSuggestedGeneration());
    }

    return {std::make_unique<TSetTabletBootInfosActor>(
        std::move(requestInfo),
        std::move(tabletBootInfos))};
}

}   // namespace NCloud::NBlockStore::NStorage
