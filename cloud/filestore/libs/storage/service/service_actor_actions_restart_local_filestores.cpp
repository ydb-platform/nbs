#include "service_actor.h"

#include "util/string/join.h"

#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/random_provider/random_provider.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRestartLocalFileStoresActionActor final
    : public TActorBootstrapped<TRestartLocalFileStoresActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;
    const TVector<TString> FileSystemIds;
    ui32 RemainingRestarts = 0;

public:
    TRestartLocalFileStoresActionActor(
            TRequestInfoPtr requestInfo,
            TString input,
            TVector<TString> fileSystemIds)
        : RequestInfo(std::move(requestInfo))
        , Input(std::move(input))
        , FileSystemIds(std::move(fileSystemIds))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        Y_UNUSED(ctx);
        NProtoPrivate::TRestartLocalFileStoresRequest request;
        if (!google::protobuf::util::JsonStringToMessage(Input, &request).ok())
        {
            ReplyAndDie(
                ctx,
                TErrorResponse(E_ARGUMENT, "Failed to parse input"));
            return;
        }

        auto rng = CreateDeterministicRandomProvider(request.GetSeed());

        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE_WORKER,
            "Restarting local file stores: seed: %lu",
            request.GetSeed());

        ui32 cookie = 0;

        for (const auto& fileSystemId: FileSystemIds) {
            if (rng->GenRand() % 2 == 0) {
                auto requestToTablet =
                    std::make_unique<TEvIndexTablet::TEvWaitReadyRequest>();
                requestToTablet->Record.SetFileSystemId(fileSystemId);

                LOG_INFO(
                    ctx,
                    TFileStoreComponents::SERVICE_WORKER,
                    "Sending WaitReady to %s",
                    fileSystemId.c_str());

                NCloud::Send(
                    ctx,
                    MakeIndexTabletProxyServiceId(),
                    std::move(requestToTablet),
                    cookie);
                ++RemainingRestarts;
            }
            ++cookie;
        }

        if (RemainingRestarts == 0) {
            return ReplyAndDie(ctx, {});
        }

        Become(&TThis::StateWork);
    }

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        const NProtoPrivate::TRestartLocalFileStoresResponse& response)
    {
        auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>(
            response.GetError());

        google::protobuf::util::MessageToJsonString(
            response,
            msg->Record.MutableOutput());

        NCloud::Reply(ctx, *RequestInfo, std::move(msg));
        Die(ctx);
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(
                TEvIndexTablet::TEvWaitReadyResponse,
                HandleWaitReadyResponse);

            default:
                HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE);
                break;
        }
    }

    void HandleWaitReadyResponse(
        const TEvIndexTablet::TEvWaitReadyResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        --RemainingRestarts;
        Y_UNUSED(ev);

        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE_WORKER,
            "Sending poison pill to %s",
            FileSystemIds.at(ev->Cookie).c_str());
        NCloud::Send(
            ctx,
            ev->Sender,
            std::make_unique<TEvents::TEvPoisonPill>());

        if (RemainingRestarts == 0) {
            ReplyAndDie(ctx, {});
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace

IActorPtr TStorageServiceActor::CreateRestartLocalFileStoresActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    TVector<TString> fileSystemIds;
    for (const auto& [fs, _]: State->GetLocalFileStores()) {
        fileSystemIds.push_back(fs);
    }

    return std::make_unique<TRestartLocalFileStoresActionActor>(
        std::move(requestInfo),
        std::move(input),
        std::move(fileSystemIds));
}

}   // namespace NCloud::NFileStore::NStorage
