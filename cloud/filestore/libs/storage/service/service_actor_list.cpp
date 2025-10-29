#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/core/config.h>
#include <cloud/filestore/libs/storage/ss_proxy/path.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/string_utils/quote/quote.h>

namespace NCloud::NFileStore::NStorage {

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

    TVector<TString> FileStores;

    size_t RequestsCompleted = 0;
    size_t RequestsScheduled = 0;

public:
    TDescribeActor(TRequestInfoPtr requestInfo, TString path)
        : RequestInfo(std::move(requestInfo))
        , Path(std::move(path))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        DescribePath(ctx, Path);
        Become(&TThis::StateWork);
    }


private:
    void DescribePath(const TActorContext& ctx, const TString& path)
    {
        LOG_DEBUG(ctx, TFileStoreComponents::SERVICE,
            "Sending describe request for path %s",
            path.Quote().c_str());

        auto request =
            std::make_unique<TEvStorageSSProxy::TEvDescribeSchemeRequest>(path);

        RequestsScheduled++;

        NCloud::Send(
            ctx,
            MakeSSProxyServiceId(),
            std::move(request),
            RequestInfo->Cookie);
    }

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvListFileStoresResponse> response)
    {
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
        Die(ctx);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvStorageSSProxy::TEvDescribeSchemeResponse, HandleDescribeResponse);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TFileStoreComponents::SERVICE,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

    void HandleDescribeResponse(
        const TEvStorageSSProxy::TEvDescribeSchemeResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        RequestsCompleted++;

        const auto* msg = ev->Get();
        const auto& error = msg->GetError();
        if (FAILED(error.GetCode())) {
            LOG_DEBUG(ctx, TFileStoreComponents::SERVICE,
                "Path %s: describe failed: %s",
                Path.Quote().c_str(),
                FormatError(error).c_str());

            ReplyAndDie(
                ctx,
                std::make_unique<TEvService::TEvListFileStoresResponse>(error));
            return;
        }

        const auto& pathDescription = msg->PathDescription;
        for (ui32 i = 0; i < pathDescription.ChildrenSize(); ++i) {
            const auto& descr = pathDescription.GetChildren(i);

            if (descr.GetPathType() == NKikimrSchemeOp::EPathTypeDir) {
                DescribePath(ctx, msg->Path + "/" + descr.GetName());
                continue;
            }

            if (descr.GetPathType() == NKikimrSchemeOp::EPathTypeFileStore) {
                TString name(descr.GetName());
                ::CGIUnescape(name);

                FileStores.emplace_back(name);
            }
        }

        if (RequestsCompleted != RequestsScheduled) {
            Y_DEBUG_ABORT_UNLESS(RequestsCompleted < RequestsScheduled);
            return;
        }

        auto response = std::make_unique<TEvService::TEvListFileStoresResponse>();
        for (const auto& fs: FileStores) {
            *response->Record.MutableFileStores()->Add() = fs;
        }

        ReplyAndDie(ctx, std::move(response));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleListFileStores(
    const TEvService::TEvListFileStoresRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto [cookie, inflight] = CreateInFlightRequest(
        TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT,
        StatsRegistry->GetRequestStats(),
        ctx.Now());

    InitProfileLogRequestInfo(inflight->ProfileLogRequest, msg->Record);

    auto requestInfo = CreateRequestInfo(
        SelfId(),
        cookie,
        msg->CallContext);

    LOG_DEBUG(ctx, TFileStoreComponents::SERVICE,
        "Listing filestores: %s",
        StorageConfig->GetSchemeShardDir().Quote().c_str());

    NCloud::Register<TDescribeActor>(
        ctx,
        std::move(requestInfo),
        StorageConfig->GetSchemeShardDir());
}

}   // namespace NCloud::NFileStore::NStorage
