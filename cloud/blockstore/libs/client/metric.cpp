#include "metric.h"

#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    struct T##name##Method                                                     \
    {                                                                          \
        static constexpr EBlockStoreRequest Request = EBlockStoreRequest::name;\
                                                                               \
        using TRequest = NProto::T##name##Request;                             \
        using TResponse = NProto::T##name##Response;                           \
    };
// BLOCKSTORE_DECLARE_METHOD

BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

struct TRequestHandler
    : public TIntrusiveListItem<TRequestHandler>
{
    TCallContextPtr CallContext;
    TMetricRequest MetricRequest;

    TRequestHandler(EBlockStoreRequest requestType, TCallContextPtr callContext)
        : CallContext(std::move(callContext))
        , MetricRequest(requestType)
    {}

    virtual void Cancel(const NProto::TError& error) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TAppContext
{
    TLog Log;
    IServerStatsPtr ServerStats;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest>
constexpr bool IsControlRequest()
{
    return IsControlRequest(GetBlockStoreRequest<TRequest>());
}

template <typename T, typename = void>
struct TConverter;

template <typename T>
struct TConverter<T, std::enable_if_t<IsControlRequest<T>()>>
{
    static TString ToString(const T& request)
    {
        return TStringBuilder() << request;
    }
};

template <typename T>
struct TConverter<T, std::enable_if_t<!IsControlRequest<T>()>>
{
    static TString ToString(const T& request)
    {
        Y_UNUSED(request);
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
struct TRequestHandlerImpl final
    : public TRequestHandler
{
    using TRequest = typename TMethod::TRequest;
    using TResponse = typename TMethod::TResponse;

private:
    TAppContext& AppCtx;

    TPromise<TResponse> Promise = NewPromise<TResponse>();
    TAtomic Completed = 0;

public:
    TRequestHandlerImpl(
            TAppContext& appCtx,
            TCallContextPtr callContext,
            const TRequest& request)
        : TRequestHandler(TMethod::Request, std::move(callContext))
        , AppCtx(appCtx)
    {
        auto calculateBytesCount = [](const TRequest& request,
                                      ui32 blockSize) -> ui64 {
            if constexpr (requires { CalculateBytesCount(request, blockSize); })
            {
                return CalculateBytesCount(request, blockSize);
            } else {
                return 0;
            }
        };
        auto getStartIndex = [](const TRequest& request) -> ui64 {
            if constexpr (requires { GetStartIndex(request); }) {
                return GetStartIndex(request);
            } else {
                return 0;
            }
        };

        AppCtx.ServerStats->PrepareMetricRequest(
            MetricRequest,
            GetClientId(request),
            GetDiskId(request),
            getStartIndex(request),
            calculateBytesCount(
                request,
                AppCtx.ServerStats->GetBlockSize(GetDiskId(request))),
            false   // unaligned
        );

        AppCtx.ServerStats->RequestStarted(
            AppCtx.Log,
            MetricRequest,
            *CallContext,
            TConverter<TRequest>::ToString(request));
    }

    void CompleteRequest(const TResponse& response)
    {
        if (AtomicSwap(&Completed, 1) == 1) {
            return;
        }

        AppCtx.ServerStats->RequestCompleted(
            AppCtx.Log,
            MetricRequest,
            *CallContext,
            response.GetError());

        Promise.SetValue(response);
    }

    void Cancel(const NProto::TError& error) override
    {
        CompleteRequest(TErrorResponse(error));
    }

    TFuture<TResponse> GetFuture()
    {
        return Promise.GetFuture();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMetricClient
    : public TAppContext
    , public IMetricClient
    , public std::enable_shared_from_this<TMetricClient>
{
protected:
    const IBlockStorePtr Client;

    TIntrusiveList<TRequestHandler> Requests;
    TAdaptiveLock RequestsLock;

public:
    TMetricClient(
            IBlockStorePtr client,
            ILoggingServicePtr logging,
            IServerStatsPtr serverStats)
        : Client(std::move(client))
    {
        Log = logging->CreateLog("BLOCKSTORE_SERVER");
        ServerStats = std::move(serverStats);
    }

    ~TMetricClient()
    {
        CancelAll();
    }

    void Start() override
    {
        Client->Start();
    }

    void Stop() override
    {
        CancelAll();
        Client->Stop();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Client->AllocateBuffer(bytesCount);
    }

    size_t CollectRequests(
        const TIncompleteRequestsCollector& collector) override
    {
        with_lock (RequestsLock) {
            ui64 now = GetCycleCount();
            size_t count = 0;
            for (auto& request: Requests) {
                ++count;
                auto requestTime = request.CallContext->CalcRequestTime(now);
                if (requestTime) {
                    collector(
                        *request.CallContext,
                        request.MetricRequest.VolumeInfo,
                        request.MetricRequest.MediaKind,
                        request.MetricRequest.RequestType,
                        requestTime);
                }
            }
            return count;
        }
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        auto handler = RegisterRequest<T##name##Method>(                       \
            callContext,                                                       \
            *request);                                                         \
        auto future = Client->name(                                            \
            std::move(callContext),                                            \
            std::move(request));                                               \
        return SubscribeUnregisterRequest<T##name##Method>(                    \
            std::move(future),                                                 \
            std::move(handler));                                               \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

private:
    template <typename TMethod>
    std::shared_ptr<TRequestHandlerImpl<TMethod>> RegisterRequest(
        TCallContextPtr callContext,
        const typename TMethod::TRequest& request)
    {
        auto handler = std::make_shared<TRequestHandlerImpl<TMethod>>(
            *this,
            std::move(callContext),
            request);

        RegisterRequestHandler(handler.get());

        return handler;
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> SubscribeUnregisterRequest(
        TFuture<typename TMethod::TResponse> result,
        std::shared_ptr<TRequestHandlerImpl<TMethod>> handler)
    {
        auto future = handler->GetFuture();

        auto weak_ptr = weak_from_this();

        result.Subscribe([h = std::move(handler), weak_ptr = std::move(weak_ptr)] (const auto& f) {
            if (auto p = weak_ptr.lock()) {
                h->CompleteRequest(f.GetValue());
                p->UnregisterRequestHandler(h.get());
            }
        });

        return future;
    }

    void RegisterRequestHandler(TRequestHandler* handler)
    {
        with_lock (RequestsLock) {
            Requests.PushBack(handler);
        }
    }

    void UnregisterRequestHandler(TRequestHandler* handler)
    {
        with_lock (RequestsLock) {
            handler->Unlink();
        }
    }

    void CancelAll()
    {
        auto error = MakeError(E_CANCELLED, "metric client was stopped");
        with_lock (RequestsLock) {
            for (auto& request: Requests) {
                request.Cancel(error);
            }
            Requests.Clear();
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IMetricClientPtr CreateMetricClient(
    IBlockStorePtr client,
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats)
{
    return std::make_shared<TMetricClient>(
        std::move(client),
        std::move(logging),
        std::move(serverStats));
}

}   // namespace NCloud::NBlockStore::NClient
