#include "service_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/logoblob.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/json/json_reader.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCheckBlobActionActor final
    : public TActorBootstrapped<TCheckBlobActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    TLogoBlobID BlobId;
    ui32 BSGroupId = 0;
    bool IndexOnly = false;

public:
    TCheckBlobActionActor(
        TRequestInfoPtr requestInfo,
        TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void CheckBlob(const TActorContext& ctx);

    void HandleSuccess(
        const TActorContext& ctx,
        NKikimrProto::EReplyStatus status,
        const TString& errorReason);

    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleGetResult(
        const TEvBlobStorage::TEvGetResult::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TCheckBlobActionActor::TCheckBlobActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TCheckBlobActionActor::Bootstrap(const TActorContext& ctx)
{
    NJson::TJsonValue input;
    if (!NJson::ReadJsonTree(Input, &input, false)) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Input should be in JSON format"));
        return;
    }

    if (input.Has("BlobId")) {
        const auto& blobId = input["BlobId"];

        ui64 rawX1 = 0, rawX2 = 0, rawX3 = 0;
        if (blobId.Has("RawX1")) {
            rawX1 = blobId["RawX1"].GetUIntegerRobust();
        }
        if (blobId.Has("RawX2")) {
            rawX2 = blobId["RawX2"].GetUIntegerRobust();
        }
        if (blobId.Has("RawX3")) {
            rawX3 = blobId["RawX3"].GetUIntegerRobust();
        }

        BlobId = TLogoBlobID(rawX1, rawX2, rawX3);
    }

    if (!BlobId) {
        HandleError(ctx, MakeError(E_ARGUMENT, "BlobId should be defined"));
        return;
    }

    if (input.Has("BSGroupId")) {
        BSGroupId = input["BSGroupId"].GetUIntegerRobust();
    }

    if (!BSGroupId) {
        HandleError(ctx, MakeError(E_ARGUMENT, "BSGroupId should be defined"));
        return;
    }

    if (input.Has("IndexOnly")) {
        IndexOnly = input["IndexOnly"].GetBooleanRobust();
    }

    CheckBlob(ctx);
    Become(&TThis::StateWork);
}

void TCheckBlobActionActor::CheckBlob(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvBlobStorage::TEvGet>(
        BlobId,
        0,
        0,
        TInstant::Max(),
        NKikimrBlobStorage::AsyncRead,
        false,
        IndexOnly
    );

    SendToBSProxy(
        ctx,
        BSGroupId,
        request.release());
}

void TCheckBlobActionActor::HandleSuccess(
    const TActorContext& ctx,
    NKikimrProto::EReplyStatus status,
    const TString& errorReason)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(TStringBuilder()
        << "{ \"Status\": \"" << NKikimrProto::EReplyStatus_Name(status) << "\""
        << ", \"Reason\": \"" << errorReason << "\""
        << "}");

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_checkblob",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TCheckBlobActionActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_checkblob",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TCheckBlobActionActor::HandleGetResult(
    const TEvBlobStorage::TEvGetResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (msg->Status != NKikimrProto::OK) {
        HandleSuccess(ctx, msg->Status, msg->ErrorReason);
        return;
    }

    if (msg->ResponseSz != 1) {
        auto error = MakeError(E_FAIL, "TEvBlobStorage::TEvGet response size is invalid");
        HandleError(ctx, error);
        return;
    }

    HandleSuccess(ctx, msg->Responses[0].Status, msg->ErrorReason);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCheckBlobActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvBlobStorage::TEvGetResult, HandleGetResult);

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

TResultOrError<IActorPtr> TServiceActor::CreateCheckBlobActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TCheckBlobActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
