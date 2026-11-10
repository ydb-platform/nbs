#include "data_endpoint_factory_test.h"

#include <cloud/blockstore/libs/service/service_method.h>

#include <util/generic/deque.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore::NIcV2 {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestDataEndpoint final
    : public TBlockStoreImpl<TTestDataEndpoint, IBlockStore>
    , public std::enable_shared_from_this<TTestDataEndpoint>
{
    class TRequestGuard
    {
        const std::shared_ptr<TTestDataEndpoint> Owner;

    public:
        explicit TRequestGuard(std::shared_ptr<TTestDataEndpoint> owner)
            : Owner(std::move(owner))
        {}

        ~TRequestGuard()
        {
            Owner->FinishRequest();
        }

        TRequestGuard(const TRequestGuard&) = delete;
        TRequestGuard& operator=(const TRequestGuard&) = delete;
    };

    ////////////////////////////////////////////////////////////////////////////////

    const IBlockStorePtr Service;
    const IHostConnectionHandlerPtr Handler;

    mutable TMutex Mutex;
    THostChannelStatus Status;
    bool Stopped = false;
    ui64 Requests = 0;
    NThreading::TPromise<void> Drained = NThreading::NewPromise();

public:
    TTestDataEndpoint(IBlockStorePtr service, IHostConnectionHandlerPtr handler)
        : Service(std::move(service))
        , Handler(std::move(handler))
    {}

    void Start() override
    {}

    void Stop() override
    {
        bool publish = false;
        {
            TGuard<TMutex> guard(Mutex);
            if (!Stopped) {
                Stopped = true;
                Status.State = EHostChannelState::Disconnected;
                publish = !Requests;
            }
        }
        if (publish) {
            Drained.SetValue();
        }
        Drained.GetFuture().GetValueSync();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Service->AllocateBuffer(bytesCount);
    }

    template <typename TMethod>
    NThreading::TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        using TResponse = typename TMethod::TResponse;
        if constexpr (
            !TMethod::IsLocalRequest() &&
            TMethod::BlockStoreRequest != EBlockStoreRequest::ZeroBlocks)
        {
            Y_UNUSED(callContext);
            Y_UNUSED(request);

            return NThreading::MakeFuture<TResponse>(TErrorResponse(
                E_NOT_IMPLEMENTED,
                "unsupported IC V2 data operation"));
        } else {
            {
                TGuard<TMutex> guard(Mutex);
                if (Stopped) {
                    return NThreading::MakeFuture<TResponse>(TErrorResponse(
                        E_CANCELLED,
                        "IC V2 test endpoint is stopped"));
                }
                ++Requests;
            }
            auto requestGuard =
                std::make_shared<TRequestGuard>(shared_from_this());
            return TMethod::Execute(
                       Service.get(),
                       std::move(callContext),
                       std::move(request))
                .Apply(
                    [requestGuard = std::move(requestGuard)](const auto& future)
                    {
                        // Retain admission until the result's continuations
                        // finish.
                        Y_UNUSED(requestGuard);

                        return future.GetValue();
                    });
        }
    }

    THostChannelStatus GetStatus() const
    {
        TGuard<TMutex> guard(Mutex);
        return Status;
    }

    void SetStatus(const THostChannelStatus& status)
    {
        TGuard<TMutex> guard(Mutex);
        if (!Stopped) {
            Status = status;
        }
    }

    void NotifyDisconnected()
    {
        IHostConnectionHandlerPtr handler;
        {
            TGuard<TMutex> guard(Mutex);
            if (Status.State != EHostChannelState::Connected) {
                return;
            }
            Status.State = EHostChannelState::Disconnected;
            handler = Handler;
        }
        if (handler) {
            handler->HandleDisconnected();
        }
    }

private:
    void FinishRequest()
    {
        bool publish = false;
        {
            TGuard<TMutex> guard(Mutex);
            Y_DEBUG_ABORT_UNLESS(Requests);

            --Requests;
            publish = Stopped && !Requests;
        }
        if (publish) {
            Drained.SetValue();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TTestDataEndpoint> GetTestEndpoint(
    const IBlockStorePtr& endpoint)
{
    auto result = std::dynamic_pointer_cast<TTestDataEndpoint>(endpoint);
    Y_DEBUG_ABORT_UNLESS(result);

    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TTestDataEndpointFactory::TImpl
{
    TMutex Mutex;
    TDeque<TResultOrError<IBlockStorePtr>> Endpoints;
    TVector<TDataEndpointRequest> Requests;
};

////////////////////////////////////////////////////////////////////////////////

TTestDataEndpointFactory::TTestDataEndpointFactory()
    : Impl(std::make_unique<TImpl>())
{}

TTestDataEndpointFactory::~TTestDataEndpointFactory() = default;

void TTestDataEndpointFactory::EnqueueEndpoint(IBlockStorePtr service)
{
    Y_DEBUG_ABORT_UNLESS(service);

    TGuard<TMutex> guard(Impl->Mutex);
    Impl->Endpoints.emplace_back(std::move(service));
}

void TTestDataEndpointFactory::EnqueueError(NProto::TError error)
{
    Y_DEBUG_ABORT_UNLESS(HasError(error));

    TGuard<TMutex> guard(Impl->Mutex);
    Impl->Endpoints.emplace_back(std::move(error));
}

TVector<TDataEndpointRequest> TTestDataEndpointFactory::GetRequests() const
{
    TGuard<TMutex> guard(Impl->Mutex);
    return Impl->Requests;
}

TResultOrError<IBlockStorePtr> TTestDataEndpointFactory::CreateDataEndpoint(
    const TString& cellId,
    const TString& host,
    IHostConnectionHandlerPtr handler)
{
    IBlockStorePtr service;
    {
        TGuard<TMutex> guard(Impl->Mutex);
        Impl->Requests.push_back({.CellId = cellId, .Host = host});
        if (Impl->Endpoints.empty()) {
            return MakeError(
                E_NOT_IMPLEMENTED,
                "No test endpoint was prepared");
        }
        auto result = std::move(Impl->Endpoints.front());
        Impl->Endpoints.pop_front();
        if (HasError(result.GetError())) {
            return result.GetError();
        }
        service = result.ExtractResult();
    }
    return IBlockStorePtr(
        std::make_shared<TTestDataEndpoint>(
            std::move(service),
            std::move(handler)));
}

THostChannelStatus TTestDataEndpointFactory::GetDataEndpointStatus(
    const IBlockStorePtr& endpoint)
{
    return GetTestEndpoint(endpoint)->GetStatus();
}

void TTestDataEndpointFactory::SetStatus(
    const IBlockStorePtr& endpoint,
    const THostChannelStatus& status)
{
    GetTestEndpoint(endpoint)->SetStatus(status);
}

void TTestDataEndpointFactory::NotifyDisconnected(
    const IBlockStorePtr& endpoint)
{
    GetTestEndpoint(endpoint)->NotifyDisconnected();
}

}   // namespace NCloud::NBlockStore::NIcV2
