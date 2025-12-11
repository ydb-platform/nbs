#include "multiclient_endpoint.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/service_method.h>

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
concept HasClientId = requires(TRequest r) {
    {
        r.MutableHeaders().SetClientId(TString())
    };
};

template <typename TRequest>
concept HasInstanceId = requires(TRequest r) {
    {
        r.SetInstanceId(TString())
    };
};

////////////////////////////////////////////////////////////////////////////////

struct TMultiClientEndpoint;

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateClient(
    IBlockStorePtr client,
    std::weak_ptr<TMultiClientEndpoint> owner,
    TString clientId,
    TString instanceId);

////////////////////////////////////////////////////////////////////////////////

struct TMultiClientEndpoint
    : public TBlockStoreImpl<TMultiClientEndpoint, IMultiClientEndpoint>
    , public std::enable_shared_from_this<TMultiClientEndpoint>
{
    const IBlockStorePtr Client;
    // maps pair of client id and instance id to IBlockStorePtr
    THashMap<std::pair<TString, TString>, IBlockStorePtr> ClientCache;
    TAdaptiveLock Lock;

    explicit TMultiClientEndpoint(IBlockStorePtr client)
        : Client(std::move(client))
    {}

    IBlockStorePtr CreateClientEndpoint(
        const TString& clientId,
        const TString& instanceId) override
    {
        with_lock (Lock) {
            auto key = std::make_pair(clientId, instanceId);
            if (auto it = ClientCache.find(key); it != ClientCache.end()) {
                return it->second;
            }
            auto client =
                CreateClient(Client, weak_from_this(), clientId, instanceId);
            ClientCache.emplace(key, client);
            return client;
        }
    }

    void Start() override
    {}

    void Stop() override
    {}

    void ReleaseClient(const TString& clientId, const TString& instanceId)
    {
        with_lock (Lock) {
            ClientCache.erase(std::make_pair(clientId, instanceId));
        }
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Client->AllocateBuffer(bytesCount);
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        return TMethod::Execute(
            Client.get(),
            std::move(callContext),
            std::move(request));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TClientEndpoint: public TBlockStoreImpl<TClientEndpoint, IBlockStore>
{
    const IBlockStorePtr Client;
    const std::weak_ptr<TMultiClientEndpoint> Owner;
    const TString ClientId;
    const TString InstanceId;

    TClientEndpoint(
        IBlockStorePtr client,
        std::weak_ptr<TMultiClientEndpoint> owner,
        TString clientId,
        TString instanceId)
        : Client(std::move(client))
        , Owner(std::move(owner))
        , ClientId(std::move(clientId))
        , InstanceId(std::move(instanceId))
    {}

    ~TClientEndpoint() override
    {
        if (auto owner = Owner.lock(); owner) {
            owner->ReleaseClient(ClientId, InstanceId);
        }
    }

    template <typename TRequest>
    void PrepareRequest(TRequest& request)
    {
        if constexpr (HasClientId<TRequest>) {
            request.MutableHeaders()->SetClientId(ClientId);
        }

        if constexpr (HasInstanceId<TRequest>) {
            request.SetInstanceId(InstanceId);
        }

        if constexpr (std::same_as<TRequest, NProto::TMountVolumeRequest>) {
            request.MutableEncryptionSpec()->Clear();
            request.SetForceDisableEncryption(true);
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

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        PrepareRequest(*request);
        return TMethod::Execute(
            Client.get(),
            std::move(callContext),
            std::move(request));
    }
};

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateClient(
    IBlockStorePtr client,
    std::weak_ptr<TMultiClientEndpoint> owner,
    TString clientId,
    TString instanceId)
{
    return std::make_shared<TClientEndpoint>(
        std::move(client),
        std::move(owner),
        std::move(clientId),
        std::move(instanceId));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IMultiClientEndpointPtr CreateMultiClientEndpoint(
    IMultiHostClientPtr client,
    const TString& host,
    ui32 port,
    bool isSecure)
{
    auto endpoint = client->CreateEndpoint(host, port, isSecure);
    return std::make_shared<TMultiClientEndpoint>(std::move(endpoint));
}

}   // namespace NCloud::NBlockStore::NClient
