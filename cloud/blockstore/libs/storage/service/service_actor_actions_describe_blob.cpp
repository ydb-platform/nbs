#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <contrib/ydb/core/base/logoblob.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <library/cpp/json/json_reader.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDescribeBlobActionActor final
    : public TActorBootstrapped<TDescribeBlobActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;

    TString DiskId;
    NKikimr::TLogoBlobID BlobId;

public:
    TDescribeBlobActionActor(TRequestInfoPtr requestInfo, TString input);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeBlob(const TActorContext& ctx);

    void HandleSuccess(const TActorContext& ctx, const TString& output);
    void HandleError(const TActorContext& ctx, const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleDescribeBlobResponse(
        const TEvVolume::TEvDescribeBlobResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDescribeBlobActionActor::TDescribeBlobActionActor(
        TRequestInfoPtr requestInfo,
        TString input)
    : RequestInfo(std::move(requestInfo))
    , Input(std::move(input))
{}

void TDescribeBlobActionActor::Bootstrap(const TActorContext& ctx)
{
    NJson::TJsonValue input;
    if (!NJson::ReadJsonTree(Input, &input, false)) {
        HandleError(ctx, MakeError(E_ARGUMENT, "Input should be in JSON format"));
        return;
    }

    if (input.Has("DiskId")) {
        DiskId = input["DiskId"].GetStringRobust();
    }

    if (!DiskId) {
        HandleError(ctx, MakeError(E_ARGUMENT, "DiskId should be defined"));
        return;
    }

    TString blobIdString;
    if (input.Has("BlobId")) {
        blobIdString = input["BlobId"].GetStringRobust();
    }

    if (!blobIdString) {
        HandleError(ctx, MakeError(E_ARGUMENT, "BlobId should be defined"));
        return;
    }

    TString errorExplanation;
    if (!NKikimr::TLogoBlobID::Parse(BlobId, blobIdString, errorExplanation)) {
        HandleError(ctx, MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "invalid BlobId: " << blobIdString.Quote()
                << " (" << errorExplanation << ")"));
        return;
    }

    DescribeBlob(ctx);
    Become(&TThis::StateWork);
}

void TDescribeBlobActionActor::DescribeBlob(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvDescribeBlobRequest>(
        RequestInfo->CallContext);
    request->Record.SetDiskId(DiskId);
    NKikimr::LogoBlobIDFromLogoBlobID(BlobId, request->Record.MutableBlobId());

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TDescribeBlobActionActor::HandleSuccess(
    const TActorContext& ctx,
    const TString& output)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    response->Record.SetOutput(output);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_describeblob",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TDescribeBlobActionActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>(error);

    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_describeblob",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDescribeBlobActionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvDescribeBlobResponse, HandleDescribeBlobResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TDescribeBlobActionActor::HandleDescribeBlobResponse(
    const TEvVolume::TEvDescribeBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (FAILED(error.GetCode())) {
        HandleError(ctx, error);
        return;
    }

    msg->Record.ClearDeprecatedTrace();
    msg->Record.MutableHeaders()->ClearTrace();

    TString response;
    google::protobuf::util::MessageToJsonString(msg->Record, &response);

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Execute action private API: describe blob response: %s",
        response.data());

    HandleSuccess(ctx, response);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateDescribeBlobActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TDescribeBlobActionActor>(
        std::move(requestInfo),
        std::move(input))};
}

}   // namespace NCloud::NBlockStore::NStorage
