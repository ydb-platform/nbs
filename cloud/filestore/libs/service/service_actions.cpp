#include "service_actions.h"

#include "context.h"
#include "filestore.h"

#include <cloud/filestore/private/api/protos/actions.pb.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, TActionHandler> LowerCaseNames(
    THashMap<TString, TActionHandler> actions)
{
    THashMap<TString, TActionHandler> result;
    for (auto& [name, handler]: actions) {
        TString action = name;
        action.to_lower();
        result.emplace(std::move(action), std::move(handler));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TForwardingService
    : public IFileStoreService
{
protected:
    const IFileStoreServicePtr Service;

public:
    explicit TForwardingService(IFileStoreServicePtr service)
        : Service(std::move(service))
    {}

    void Start() override
    {
        Service->Start();
    }

    void Stop() override
    {
        Service->Stop();
    }

#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return Service->name(std::move(ctx), std::move(request));              \
    }                                                                          \
// FILESTORE_IMPLEMENT_METHOD

    FILESTORE_SERVICE(FILESTORE_IMPLEMENT_METHOD)

#undef FILESTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

class TActionsService final
    : public TForwardingService
{
private:
    const THashMap<TString, TActionHandler> Actions;

public:
    TActionsService(
            IFileStoreServicePtr service,
            THashMap<TString, TActionHandler> actions)
        : TForwardingService(std::move(service))
        , Actions(LowerCaseNames(std::move(actions)))
    {}

    TFuture<NProto::TExecuteActionResponse> ExecuteAction(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TExecuteActionRequest> request) override
    {
        TString action = request->GetAction();
        action.to_lower();

        const auto* handler = Actions.FindPtr(action);
        if (!handler) {
            return Service->ExecuteAction(std::move(ctx), std::move(request));
        }
        return (*handler)(request->GetInput());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileStoreServicePtr CreateActionsService(
    IFileStoreServicePtr service,
    THashMap<TString, TActionHandler> actions)
{
    return std::make_shared<TActionsService>(
        std::move(service),
        std::move(actions));
}

TActionHandler CreateUpdateCertificatesAction(
    std::function<TFuture<NProto::TError>()> updateCertificates)
{
    return [updateCertificates = std::move(updateCertificates)](
               const TString& input)
    {
        NProtoPrivate::TUpdateCertificatesRequest request;
        if (input &&
            !google::protobuf::util::JsonStringToMessage(input, &request).ok())
        {
            NProto::TExecuteActionResponse response;
            *response.MutableError() =
                MakeError(E_ARGUMENT, "Failed to parse input");
            return MakeFuture(std::move(response));
        }

        return updateCertificates().Apply(
            [](const auto& future)
            {
                NProto::TExecuteActionResponse response;
                *response.MutableError() = future.GetValue();
                if (!HasError(response)) {
                    google::protobuf::util::MessageToJsonString(
                        NProtoPrivate::TUpdateCertificatesResponse(),
                        response.MutableOutput());
                }
                return response;
            });
    };
}

}   // namespace NCloud::NFileStore
