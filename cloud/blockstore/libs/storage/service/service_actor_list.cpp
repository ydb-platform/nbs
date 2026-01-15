#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/model/volume_label.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDescribeActor final
    : public TActorBootstrapped<TDescribeActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Path;
    TVector<TString> Volumes;
    size_t RequestsCompleted = 0;
    size_t RequestsScheduled = 0;

public:
    TDescribeActor(
        TRequestInfoPtr requestInfo,
        TString path);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribePath(const TActorContext& ctx, const TString& path);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvListVolumesResponse> response);

private:
    STFUNC(StateWork);

    void HandleDescribeResponse(
        const TEvSSProxy::TEvDescribeSchemeResponse::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDescribeActor::TDescribeActor(
        TRequestInfoPtr requestInfo,
        TString path)
    : RequestInfo(std::move(requestInfo))
    , Path(std::move(path))
{}

void TDescribeActor::Bootstrap(const TActorContext& ctx)
{
    DescribePath(ctx, Path);
    Become(&TThis::StateWork);
}

void TDescribeActor::DescribePath(const TActorContext& ctx, const TString& path)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Sending describe request for path %s",
        path.Quote().data());

    auto request = std::make_unique<TEvSSProxy::TEvDescribeSchemeRequest>(path);
    RequestsScheduled++;

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TDescribeActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvListVolumesResponse> response)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDescribeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSSProxy::TEvDescribeSchemeResponse, HandleDescribeResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TDescribeActor::HandleDescribeResponse(
    const TEvSSProxy::TEvDescribeSchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    RequestsCompleted++;

    const auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
            "Path %s: describe failed: %s",
            Path.Quote().data(),
            FormatError(error).data());

        ReplyAndDie(
            ctx,
            std::make_unique<TEvService::TEvListVolumesResponse>(error));
        return;
    }

    const auto& pathDescription = msg->PathDescription;

    for (ui32 i = 0; i < pathDescription.ChildrenSize(); ++i) {
        const auto& descr = pathDescription.GetChildren(i);

        if (descr.GetPathType() == NKikimrSchemeOp::EPathTypeDir) {
            DescribePath(ctx, msg->Path + "/" + descr.GetName());
            continue;
        }

        if (descr.GetPathType() == NKikimrSchemeOp::EPathTypeBlockStoreVolume) {
            Volumes.emplace_back(PathNameToDiskId(descr.GetName()));
        }
    }

    if (RequestsCompleted != RequestsScheduled) {
        Y_DEBUG_ABORT_UNLESS(RequestsCompleted < RequestsScheduled);
        return;
    }

    auto response = std::make_unique<TEvService::TEvListVolumesResponse>();

    for (const auto& volume : Volumes) {
        *response->Record.MutableVolumes()->Add() = volume;
    }

    ReplyAndDie(ctx, std::move(response));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleListVolumes(
    const TEvService::TEvListVolumesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& request = msg->Record;

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    // TODO: filter?
    Y_UNUSED(request);

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Listing volumes: %s",
        Config->GetSchemeShardDir().Quote().data());

    NCloud::Register<TDescribeActor>(
        ctx,
        std::move(requestInfo),
        Config->GetSchemeShardDir());
}

}   // namespace NCloud::NBlockStore::NStorage
