#include "service_actions.h"

#include "context.h"
#include "endpoint.h"
#include "filestore.h"

#include <cloud/filestore/private/api/protos/actions.pb.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TActionHandlers
{
private:
    THashMap<TString, TActionHandler> Handlers;

public:
    explicit TActionHandlers(THashMap<TString, TActionHandler> handlers)
    {
        for (auto& [name, handler]: handlers) {
            TString action = name;
            action.to_lower();
            Handlers.emplace(std::move(action), std::move(handler));
        }
    }

    const TActionHandler* Find(
        const NProto::TExecuteActionRequest& request) const
    {
        TString action = request.GetAction();
        action.to_lower();
        return Handlers.FindPtr(action);
    }
};

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
    const TActionHandlers Actions;

public:
    TActionsService(
            IFileStoreServicePtr service,
            THashMap<TString, TActionHandler> actions)
        : TForwardingService(std::move(service))
        , Actions(std::move(actions))
    {}

    TFuture<NProto::TExecuteActionResponse> ExecuteAction(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TExecuteActionRequest> request) override
    {
        const auto* handler = Actions.Find(*request);
        if (!handler) {
            return Service->ExecuteAction(std::move(ctx), std::move(request));
        }
        return (*handler)(request->GetInput());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TForwardingEndpointManager
    : public IEndpointManager
{
protected:
    const IEndpointManagerPtr Service;

public:
    explicit TForwardingEndpointManager(IEndpointManagerPtr service)
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

    void Drain() override
    {
        Service->Drain();
    }

    TFuture<void> RestoreEndpoints() override
    {
        return Service->RestoreEndpoints();
    }

#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return Service->name(std::move(ctx), std::move(request));              \
    }                                                                          \
// FILESTORE_IMPLEMENT_METHOD

    FILESTORE_ENDPOINT_SERVICE(FILESTORE_IMPLEMENT_METHOD)

#undef FILESTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

class TEndpointActionsService final
    : public TForwardingEndpointManager
{
private:
    const TActionHandlers Actions;

public:
    TEndpointActionsService(
            IEndpointManagerPtr service,
            THashMap<TString, TActionHandler> actions)
        : TForwardingEndpointManager(std::move(service))
        , Actions(std::move(actions))
    {}

    TFuture<NProto::TExecuteActionResponse> ExecuteAction(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TExecuteActionRequest> request) override
    {
        const auto* handler = Actions.Find(*request);
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

IEndpointManagerPtr CreateActionsService(
    IEndpointManagerPtr service,
    THashMap<TString, TActionHandler> actions)
{
    return std::make_shared<TEndpointActionsService>(
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
