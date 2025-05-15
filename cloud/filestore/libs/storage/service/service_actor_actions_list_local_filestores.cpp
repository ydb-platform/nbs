#include "service_actor.h"

#include <cloud/filestore/private/api/protos/actions.pb.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TListLocalFileStoresActionActor final
    : public TActorBootstrapped<TListLocalFileStoresActionActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString Input;
    const TVector<TString> FileSystemIds;

public:
    TListLocalFileStoresActionActor(
            TRequestInfoPtr requestInfo,
            TString input,
            TVector<TString> fileSystemIds)
        : RequestInfo(std::move(requestInfo))
        , Input(std::move(input))
        , FileSystemIds(std::move(fileSystemIds))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        NProtoPrivate::TListLocalFileStoresRequest request;
        if (!google::protobuf::util::JsonStringToMessage(Input, &request).ok())
        {
            NCloud::Reply(
                ctx,
                *RequestInfo,
                std::make_unique<TEvService::TEvExecuteActionResponse>(
                    TErrorResponse(E_ARGUMENT, "Failed to parse input")));

            Die(ctx);
        }
        NProtoPrivate::TListLocalFileStoresResponse response;

        for (const auto& fileSystemId: FileSystemIds) {
            response.AddFileSystemIds(fileSystemId);
        }

        auto msg = std::make_unique<TEvService::TEvExecuteActionResponse>();
        google::protobuf::util::MessageToJsonString(
            response,
            msg->Record.MutableOutput());
        NCloud::Reply(ctx, *RequestInfo, std::move(msg));
        Die(ctx);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            default:
                HandleUnexpectedEvent(
                    ev,
                    TFileStoreComponents::SERVICE,
                    __PRETTY_FUNCTION__);
                break;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace

IActorPtr TStorageServiceActor::CreateListLocalFileStoresActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    TVector<TString> fileSystemIds;
    for (const auto& [fs, _]: State->GetLocalFileStores()) {
        fileSystemIds.push_back(fs);
    }

    return std::make_unique<TListLocalFileStoresActionActor>(
        std::move(requestInfo),
        std::move(input),
        std::move(fileSystemIds));
}

}   // namespace NCloud::NFileStore::NStorage
