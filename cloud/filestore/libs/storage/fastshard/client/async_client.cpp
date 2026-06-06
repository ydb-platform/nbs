#include "async_client.h"

#include "client.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/string/builder.h>

#include <silk/fibers/fiber.h>

#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard {

using silk::FiberScheduler;
using namespace NProtoSrv;

namespace {

////////////////////////////////////////////////////////////////////////////////
// Send fiber.
//
// TRequest doesn't fit in FIBER_PARAMETERS_SIZE so it lives on the heap
// via unique_ptr; the other two fields are pointer-sized and fit inline.

struct TSendParams
{
    std::shared_ptr<IEndpoint> Endpoint;
    std::unique_ptr<TRequest> Request;
    NThreading::TPromise<TResponse> Promise;
};
static_assert(sizeof(TSendParams) <= silk::FIBER_PARAMETERS_SIZE);

int SendFiberMain(TSendParams* params) noexcept
{
    TResponse resp = params->Endpoint->Send(*params->Request);
    params->Promise.SetValue(std::move(resp));
    return 0;
}

////////////////////////////////////////////////////////////////////////////////
// Async endpoint.

struct TAsyncEndpoint: IAsyncEndpoint
{
    std::shared_ptr<IEndpoint> Endpoint;

    explicit TAsyncEndpoint(std::unique_ptr<IEndpoint> ep)
        : Endpoint(std::move(ep))
    {}

    NThreading::TFuture<TResponse> Send(TRequest req) override
    {
        auto promise = NThreading::NewPromise<TResponse>();
        auto future = promise.GetFuture();

        int r = FiberScheduler::run(
            SendFiberMain,
            TSendParams{
                .Endpoint = Endpoint,
                .Request = std::make_unique<TRequest>(std::move(req)),
                .Promise = promise,
            },
            nullptr);
        if (r) {
            TResponse resp;
            *resp.MutableError() = MakeError(E_FAIL, TStringBuilder()
                << "Failed to run fiber, code: " << r
                << ", message: " << ::strerror(r));
            promise.SetValue(std::move(resp));
        }

        return future;
    }
};

////////////////////////////////////////////////////////////////////////////////
// Connect fiber.
//
// TString is a refcounted pointer (~8 bytes) and TPromise is an intrusive
// pointer (~8 bytes), so all three fields fit in FIBER_PARAMETERS_SIZE.

struct TConnectParams
{
    std::weak_ptr<TClient> Client;
    TString Host;
    ui16 Port;
    NThreading::TPromise<IAsyncEndpointPtr> Promise;
};
static_assert(sizeof(TConnectParams) <= silk::FIBER_PARAMETERS_SIZE);

int ConnectFiberMain(TConnectParams* params) noexcept
{
    auto client = params->Client.lock();
    if (!client) {
        params->Promise.SetValue(nullptr);
        return ECANCELED;
    }

    auto ep = client->Connect(params->Host, params->Port);
    if (ep) {
        params->Promise.SetValue(
            std::make_unique<TAsyncEndpoint>(std::move(ep)));
    } else {
        params->Promise.SetValue(nullptr);
    }

    return 0;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TAsyncClient::TAsyncClient()
    : Client(std::make_shared<TClient>())
{}

NThreading::TFuture<IAsyncEndpointPtr> TAsyncClient::Connect(
    const TString& host,
    ui16 port)
{
    auto promise = NThreading::NewPromise<IAsyncEndpointPtr>();
    auto future = promise.GetFuture();

    int r = FiberScheduler::run(
        ConnectFiberMain,
        TConnectParams{
            .Client = Client,
            .Host = host,
            .Port = port,
            .Promise = promise,
        },
        nullptr);
    if (r) {
        promise.SetValue(nullptr);
    }

    return future;
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
