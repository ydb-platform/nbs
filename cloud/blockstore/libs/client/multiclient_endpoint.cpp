#include "multiclient_endpoint.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/hash.h>
#include <util/system/spinlock.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest>
concept HasClientId = requires (TRequest r)
{
    { r.GetHeaders().GetClientId()} -> std::same_as<TString>;
};

template <typename TRequest>
concept HasInstanceId = requires (TRequest r)
{
    { r.GetInstanceId() } -> std::same_as<TString>;
};

////////////////////////////////////////////////////////////////////////////////

struct TMultiClientEndpoint;

////////////////////////////////////////////////////////////////////////////////

struct TClientEndpoint
    : public IBlockStore
{
    const IBlockStorePtr Client;
    const std::weak_ptr<TMultiClientEndpoint> MultiClientEndpoint;
    const TString ClientId;
    const TString InstanceId;

    TClientEndpoint(
            IBlockStorePtr client,
            std::weak_ptr<TMultiClientEndpoint> multiClientEndpoint,
            TString clientId,
            TString instanceId)
        : Client(std::move(client))
        , MultiClientEndpoint(std::move(multiClientEndpoint))
        , ClientId(std::move(clientId))
        , InstanceId(std::move(instanceId))
    {}

    ~TClientEndpoint() override;

    template <typename TRequest>
    void PrepareRequest(TRequest& request)
    {
        if constexpr (HasClientId<TRequest>) {
            request.MutableHeaders()->SetClientId(ClientId);
        }

        if constexpr (HasInstanceId<TRequest>) {
            request.MutableHeaders()->SetInstanceId(InstanceId);
        }
    }

    void Start() override
    {}

    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Client->AllocateBuffer(bytesCount);
    }

    #define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                             \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        PrepareRequest(*request);                                              \
        return Client->name(                                                   \
            std::move(callContext),                                            \
            std::move(request));                                               \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

struct TMultiClientEndpoint
    : public IMultiClientEndpoint
    , public std::enable_shared_from_this<TMultiClientEndpoint>
{
    const IBlockStorePtr Client;
    THashMap<std::pair<TString, TString>, IBlockStorePtr> Cache;
    TAdaptiveLock Lock;

    TMultiClientEndpoint(IBlockStorePtr client)
        : Client(std::move(client))
    {}

    IBlockStorePtr CreateClientEndpoint(
        const TString& clientId,
        const TString& instanceId) override
    {
        with_lock (Lock) {
            auto key = std::make_pair(clientId, instanceId);
            if (auto it = Cache.find(key);
                it != Cache.end())
            {
                return it->second;
            }
            auto client = std::make_shared<TClientEndpoint>(
                Client,
                weak_from_this(),
                clientId,
                instanceId);
            Cache.emplace(key, client);
            return client;
        }
    }

    void Start() override
    {
    }

    void Stop() override
    {
    }

    void Release(
        const TString& clientId,
        const TString& instanceId)
    {
        with_lock (Lock) {
            Cache.erase(std::make_pair(clientId, instanceId));
        }
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Client->AllocateBuffer(bytesCount);
    }

    #define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                             \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return Client->name(                                                   \
            std::move(callContext),                                            \
            std::move(request));                                               \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)
};

////////////////////////////////////////////////////////////////////////////////

TClientEndpoint::~TClientEndpoint()
{
    if (auto owner = MultiClientEndpoint.lock(); owner) {
        owner->Release(ClientId, InstanceId);
    }
}

}  // namespace

////////////////////////////////////////////////////////////////////////////////

IMultiClientEndpointPtr CreateMultiClientEndpoint(
    IClientPtr client,
    const TString& host,
    ui32 port,
    bool isSecure)
{
    auto endpoint = client->CreateEndpoint(host, port, isSecure);
    return std::make_shared<TMultiClientEndpoint>(std::move(endpoint));
}

}   // namespace NCloud::NBlockStore::NClient
