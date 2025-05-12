#include "service_su.h"

#include <cloud/blockstore/config/server.pb.h>

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/server/config.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <cloud/blockstore/libs/rdma/iface/client.h>

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/client/throttling.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/encryption/encryption_client.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NServer {

using namespace NActors;
using namespace NThreading;

using namespace NCloud::NBlockStore::NStorage;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    struct T##name##Method                                                     \
    {                                                                          \
        using TRequest = TEvService::TEv##name##Request;                       \
        using TRequestProto = NProto::T##name##Request;                        \
                                                                               \
        using TResponse = TEvService::TEv##name##Response;                     \
        using TResponseProto = NProto::T##name##Response;                      \
    };                                                                         \
// BLOCKSTORE_DECLARE_METHOD

BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

template<bool DataService>
class TSuProxyService final
    : public IBlockStore
{
private:
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ILoggingServicePtr Logging;
    const IMonitoringServicePtr Monitoring;
    const TString Host;
    const ui32 Port;
    NClient::TClientAppConfigPtr ClientConfig;
    NClient::IClientPtr Client;
    IBlockStorePtr Endpoint;

public:
    TSuProxyService(
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            ILoggingServicePtr logging,
            IMonitoringServicePtr monitoring,
            const TString& host,
            ui32 port,
            std::optional<TString> clientId)
        : Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
        , Host(host)
        , Port(port)
    {
        InitClientConfig(std::move(clientId));

        auto [client, error] = CreateClient(
            ClientConfig,
            Timer,
            Scheduler,
            Logging,
            Monitoring,
            CreateServerStatsStub());

        Client = std::move(client);
    }

    void Start() override {
        Client->Start();
        Endpoint = DataService ?
            Client->CreateDataEndpoint():
            Client->CreateEndpoint();
    }

    void Stop() override {
        Client->Stop();
    }

    void InitClientConfig(std::optional<TString> clientId)
    {
        NProto::TClientAppConfig appConfig;

        auto& clientConfig = *appConfig.MutableClientConfig();
        clientConfig.SetHost(Host);
        if (DataService) {
            clientConfig.SetPort(Port);
        } else {
            clientConfig.SetInsecurePort(Port);
        }
        if (clientId.has_value()) {
            clientConfig.SetClientId(*clientId);
        } else {
            clientConfig.SetClientId("xxx");
        }

        ClientConfig = std::make_shared<NClient::TClientAppConfig>(appConfig);
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    { \
        request->MutableHeaders()->ClearInternal();                                                                      \
        return Endpoint->name(std::move(ctx), std::move(request));             \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(ctx);                                                         \
        Y_UNUSED(request);                                                     \
        return MakeFuture<NProto::T##name##Response>(TErrorResponse(           \
            E_NOT_IMPLEMENTED,                                                 \
            "Method " #name " not implemeted"));                               \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_ENDPOINT_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

private:
    template <typename T>
    void ExecuteRequest(
        TCallContextPtr ctx,
        std::shared_ptr<typename T::TRequestProto> request,
        TPromise<typename T::TResponseProto> response)
    {
        Y_UNUSED(ctx);
        Y_UNUSED(request);
        Y_UNUSED(response);
        const auto& headers = request->GetHeaders();
        auto timeout = TDuration::MilliSeconds(headers.GetRequestTimeout());

        Y_UNUSED(headers);
        Y_UNUSED(timeout);

        /*ActorSystem->Register(std::make_unique<TRequestActor<T>>(
            std::move(request),
            std::move(response),
            std::move(ctx),
            timeout));*/
    }
};

template <typename TService, typename TRequest, typename TResponse>
constexpr bool HandlesMethod = requires (TService* service ,TCallContextPtr ctx,std::shared_ptr<TRequest> request)
{
    {service->Handle(std::move(ctx), std::move(request))} -> std::same_as<NThreading::TFuture<TResponse>>;
};

template <typename TRequest>
constexpr bool HasDiskId = requires (std::shared_ptr<TRequest> request)
{
    { request->GetDiskId() } -> std::same_as<TString>;
};


struct TSuDiscoveryService
    : public ISuDiscoveryService
{
    const IBlockStorePtr Service;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ILoggingServicePtr Logging;
    const IMonitoringServicePtr Monitoring;
    const TServerAppConfigPtr Config;

    TLog Log;

    TSuServiceMap SuMap;

    THashMap<TString, IBlockStorePtr> DiskCache;

    TSuDiscoveryService(
            IBlockStorePtr service,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            ILoggingServicePtr logging,
            IMonitoringServicePtr monitoring,
            const TServerAppConfigPtr& config)
        : Service(std::move(service))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
        , Config(config)
    {
        Log = Logging->CreateLog("DISCOVERY_SERVICE");
        for (const auto& item: Config->GetShardMap()) {
            auto host = item.second.GetHosts(0);

            auto suService = CreateSuService(
                Timer,
                Scheduler,
                Logging,
                Monitoring,
                host,
                item.second.GetGrpcPort());

            SuMap.emplace(
                item.first,
                suService);
        }

        SuMap.emplace(
            "",
            Service);
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }


    #define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                             \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return TryHandle<NProto::T##name##Request, NProto::T##name##Response>( \
            std::move(ctx), \
            std::move(request), \
            [] (auto srv, auto&& ctx, auto&& request) {return srv->name(std::move(ctx), std::move(request));});           \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(ctx);                                                         \
        Y_UNUSED(request);                                                     \
        return MakeFuture<NProto::T##name##Response>(TErrorResponse(           \
            E_NOT_IMPLEMENTED,                                                 \
            "Method " #name " not implemeted"));                               \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_ENDPOINT_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

    struct TDescribeResult
        : public std::enable_shared_from_this<TDescribeResult>
    {
        TDescribeResult(TVector<std::pair<TString, TFuture<NProto::TDescribeVolumeResponse>>> futures)
            : Futures(std::move(futures))
            , Promise(NewPromise<NProto::TDescribeVolumeResponse>())
        {
        }

        void Setup()
        {
            auto self = shared_from_this();
            for (auto& f: Futures) {
                f.second.Subscribe([self, suId = f.first] (const auto& future) {
                    if (self->Promise.HasValue()) {
                        return;
                    }
                    auto response = future.GetValue();
                    if (!HasError(response)) {
                        response.SetShardId(suId);
                        self->Promise.SetValue(response);
                        return;
                    }
                    auto old = self->Counter.fetch_add(1, std::memory_order_relaxed);
                    if (old == self->Futures.size() - 1) {
                        self->Promise.SetValue(response);
                    }
                });
            }
        }

        std::atomic<ui64> Counter{0};
        TVector<std::pair<TString, TFuture<NProto::TDescribeVolumeResponse>>> Futures;
        TPromise<NProto::TDescribeVolumeResponse> Promise;
    };

    TFuture<NProto::TDescribeVolumeResponse> Handle(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TDescribeVolumeRequest> request)
    {
        Y_UNUSED(callContext);
        TVector<std::pair<TString, TFuture<NProto::TDescribeVolumeResponse>>> futures(
            Reserve(SuMap.size()));

        for (auto& su: SuMap) {
            auto callContext = MakeIntrusive<TCallContext>();

            STORAGE_ERROR(TStringBuilder()
            << "Send Discovery Request");

            auto req = std::make_shared<NProto::TDescribeVolumeRequest>();
            req->CopyFrom(*request);
            req->MutableHeaders()->ClearInternal();

            auto future = su.second->DescribeVolume(
                callContext,
                std::move(req));

            futures.emplace_back(su.first, std::move(future));
        }

        auto waitResult = std::make_shared<TDescribeResult>(std::move(futures));
        waitResult->Setup();

        return waitResult->Promise.GetFuture();
    };

    TFuture<NProto::TMountVolumeResponse> Handle(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TMountVolumeRequest> request)
    {

        TPromise<NProto::TMountVolumeResponse> promise = NewPromise<NProto::TMountVolumeResponse>();

        if (auto it = DiskCache.find(request->GetDiskId()); it != DiskCache.end()) {
            request->MutableHeaders()->ClearInternal();
            it->second->MountVolume(std::move(callContext), std::move(request)).Subscribe([=] (const auto& future) mutable {
                auto response = future.GetValue();
                promise.SetValue(response);
            });
            return promise.GetFuture();
        }

        auto describeRequest = std::make_shared<NProto::TDescribeVolumeRequest>();
        //describeRequest->MutableHeaders()->CopyFrom(headers);
        describeRequest->SetDiskId(request->GetDiskId());

        DescribeVolume(callContext, describeRequest).Subscribe([=, this] (const auto& future) mutable {
            auto response = future.GetValue();
            if (HasError(response)) {
                NProto::TMountVolumeResponse response;
                *response.MutableError() = response.GetError();
                promise.SetValue(response);
                return;
            }

            IBlockStorePtr suService;

            if (!response.GetShardId().Empty()) {
                suService = CreateSuDataService(
                    Timer,
                    Scheduler,
                    Logging,
                    Monitoring,
                    Config->GetShardMap()[response.GetShardId()].GetHosts(0),
                    Config->GetShardMap()[response.GetShardId()].GetGrpcPort()
                );
                suService->Start();
            } else {
                suService = Service;
            }

            DiskCache[request->GetDiskId()] = suService;
            request->MutableHeaders()->ClearInternal();
            suService->MountVolume(std::move(callContext), std::move(request)).Subscribe([=] (const auto& future) mutable {
                auto response = future.GetValue();
                promise.SetValue(response);
            });
        });
        return promise.GetFuture();
    };

    template <typename TRequest, typename TResponse, typename F>
    TFuture<TResponse> TryHandle(TCallContextPtr ctx, std::shared_ptr<TRequest> request, F fallback) {
        constexpr bool hasOverride = HandlesMethod<TSuDiscoveryService, TRequest, TResponse>;
        if constexpr (hasOverride) {
            return Handle(std::move(ctx), std::move(request));
        }
        if constexpr (HasDiskId<TRequest>) {
            if (auto it = DiskCache.find(request->GetDiskId()); it != DiskCache.end()) {
                fallback(it->second, std::move(ctx), std::move(request));
            }
        }
        return fallback(Service, std::move(ctx), std::move(request));
    }

    IBlockStorePtr GetSuProxyService(TString suId) override
    {
        return SuMap[suId];
    }

    void Start() override
    {
        for (auto& c: SuMap) {
            c.second->Start();
        }
    }

    void Stop() override
    {
        for (auto& c: SuMap) {
            c.second->Stop();
        }
    }
};


////////////////////////////////////////////////////////////////////////////////

struct TRemoteStorage
    : public IStorage
    , std::enable_shared_from_this<TRemoteStorage>
{
    const IBlockStorePtr Endpoint;

    TRemoteStorage(
            IBlockStorePtr endpoint)
        : Endpoint(std::move(endpoint))
    {

    }

    NThreading::TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        return Endpoint->ZeroBlocks(std::move(callContext), std::move(request));
    }

    NThreading::TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        return Endpoint->ReadBlocksLocal(std::move(callContext), std::move(request));
    }

    NThreading::TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        return Endpoint->WriteBlocksLocal(std::move(callContext), std::move(request));
    }

    NThreading::TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        Y_UNUSED(method);
        return MakeFuture(MakeError(E_NOT_IMPLEMENTED));
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

   void ReportIOError() override
   {
   }
};


}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateSuService(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    const TString& host,
    ui32 port,
    std::optional<TString> clientId)
{
    return std::make_shared<TSuProxyService<false>>(
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(monitoring),
        host,
        port,
        std::move(clientId));
}

IStoragePtr CreateRemoteEndpoint(IBlockStorePtr endpoint)
{
    return std::make_shared<TRemoteStorage>(std::move(endpoint));
}

IBlockStorePtr CreateSuDataService(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    const TString& host,
    ui32 port,
    std::optional<TString> clientId)
{
    return std::make_shared<TSuProxyService<false>>(
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(monitoring),
        host,
        port,
        std::move(clientId));
}


ISuDiscoveryServicePtr CreateSuDiscoveryService(
    IBlockStorePtr service,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    const TServerAppConfigPtr& config)
{
    auto srv = std::make_shared<TSuDiscoveryService>(
        std::move(service),
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(monitoring),
        config);
    return srv;
}

IBlockStorePtr CreateRemoteGrpcService(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    const TString& host,
    ui64 port,
    std::optional<TString> clientId)
{
    return CreateSuDataService(
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(monitoring),
        host,
        port,
        std::move(clientId));
}

}   // namespace NCloud::NBlockStore::NServer
