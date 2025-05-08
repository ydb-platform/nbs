#include "service_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/base/logoblob.h>
#include <contrib/ydb/core/base/tablet.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <library/cpp/json/json_reader.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NStorage;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TResetTabletActionActor final
    : public TActorBootstrapped<TResetTabletActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    ui64 TabletId = 0;
    ui32 Generation = 0;
    TTabletStorageInfoPtr StorageInfo;

public:
    TResetTabletActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void GetStorageInfo(const TActorContext& ctx);
    void ResetTablet(const TActorContext& ctx);

    void HandleSuccess(const TActorContext& ctx, const TString& output);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleGetStorageInfoResponse(
        const TEvHiveProxy::TEvGetStorageInfoResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleResetTabletResult(
        TEvTablet::TEvResetTabletResult::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TResetTabletActionActor::TResetTabletActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TResetTabletActionActor::Bootstrap(const TActorContext& ctx)
{
    NJson::TJsonValue input;
    if (!NJson::ReadJsonTree(Input, &input, false)) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Input should be in JSON format"));
        return;
    }

    if (input.Has("TabletId")) {
        TabletId = input["TabletId"].GetUIntegerRobust();
    }

    if (!TabletId) {
        HandleError(ctx, MakeError(E_ARGUMENT, "TabletId should be defined"));
        return;
    }

    if (input.Has("Generation")) {
        Generation = input["Generation"].GetUIntegerRobust();
    }

    GetStorageInfo(ctx);
    Become(&TThis::StateWork);
}

void TResetTabletActionActor::GetStorageInfo(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvHiveProxy::TEvGetStorageInfoRequest>(TabletId);

    NCloud::Send(
        ctx,
        MakeHiveProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TResetTabletActionActor::ResetTablet(const TActorContext& ctx)
{
    ctx.Register(CreateTabletReqReset(ctx.SelfID, StorageInfo, Generation));
}

void TResetTabletActionActor::HandleSuccess(
    const TActorContext& ctx,
    const TString& output)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_resettablet",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TResetTabletActionActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_resettablet",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TResetTabletActionActor::HandleGetStorageInfoResponse(
    const TEvHiveProxy::TEvGetStorageInfoResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        HandleError(ctx, error);
        return;
    }

    StorageInfo = msg->StorageInfo;
    ResetTablet(ctx);
}

void TResetTabletActionActor::HandleResetTabletResult(
    TEvTablet::TEvResetTabletResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    HandleSuccess(ctx, TStringBuilder()
        << "{ \"Status\": \""
        << NKikimrProto::EReplyStatus_Name(msg->Status)
        << "\" }");
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TResetTabletActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvHiveProxy::TEvGetStorageInfoResponse, HandleGetStorageInfoResponse);
        HFunc(TEvTablet::TEvResetTabletResult, HandleResetTabletResult);

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

TResultOrError<IActorPtr> TServiceActor::CreateResetTabletActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TResetTabletActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
