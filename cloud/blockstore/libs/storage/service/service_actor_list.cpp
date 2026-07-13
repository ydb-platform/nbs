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
    const size_t MaxConcurrency;
    TVector<TString> Volumes;
    TDeque<TString> PendingPaths;
    size_t RequestsInFlight = 0;
    size_t TotalRequestsScheduled = 0;

public:
    TDescribeActor(
        TRequestInfoPtr requestInfo,
        TString rootPath,
        size_t maxConcurrency);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribePath(const TActorContext& ctx, const TString& path);
    void ContinueDescribing(const TActorContext& ctx);

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
    TString rootPath,
    size_t maxConcurrency)
    : RequestInfo(std::move(requestInfo))
    , MaxConcurrency(maxConcurrency)
{
    PendingPaths.emplace_back(std::move(rootPath));
}

void TDescribeActor::Bootstrap(const TActorContext& ctx)
{
    ContinueDescribing(ctx);
    Become(&TThis::StateWork);
}

void TDescribeActor::ContinueDescribing(const TActorContext& ctx)
{
    while (RequestsInFlight < MaxConcurrency && !PendingPaths.empty()) {
        DescribePath(ctx, PendingPaths.front());
        PendingPaths.pop_front();
    }
}

void TDescribeActor::DescribePath(const TActorContext& ctx, const TString& path)
{
    ++TotalRequestsScheduled;
    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Sending describe request #%zu for path %s",
        TotalRequestsScheduled,
        path.Quote().data());

    auto request = std::make_unique<TEvSSProxy::TEvDescribeSchemeRequest>(path);
    ++RequestsInFlight;

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
    --RequestsInFlight;

    const auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Path %s: describe failed: %s",
            msg->Path.Quote().data(),
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
            PendingPaths.emplace_back(msg->Path + "/" + descr.GetName());
            continue;
        }

        if (descr.GetPathType() == NKikimrSchemeOp::EPathTypeBlockStoreVolume) {
            Volumes.emplace_back(PathNameToDiskId(descr.GetName()));
        }
    }

    ContinueDescribing(ctx);

    if (RequestsInFlight == 0) {
        auto response = std::make_unique<TEvService::TEvListVolumesResponse>();
        for (const auto& volume: Volumes) {
            *response->Record.MutableVolumes()->Add() = volume;
        }
        ReplyAndDie(ctx, std::move(response));
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleListVolumes(
    const TEvService::TEvListVolumesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& request = msg->Record;

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    const size_t maxConcurrency =
        request.GetMaxConcurrency() > 0 ? request.GetMaxConcurrency() : 1;

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Listing volumes: %s",
        Config->GetSchemeShardDir().Quote().data());

    NCloud::Register<TDescribeActor>(
        ctx,
        std::move(requestInfo),
        Config->GetSchemeShardDir(),
        maxConcurrency);
}

}   // namespace NCloud::NBlockStore::NStorage
