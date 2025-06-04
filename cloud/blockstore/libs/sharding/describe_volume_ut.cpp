#include "describe_volume.h"

#include "config.h"
#include "remote_storage_provider.h"
#include "remote_storage.h"

#include <cloud/blockstore/config/client.pb.h>
#include <cloud/blockstore/config/sharding.pb.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NSharding {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestRemoteStorageProvider
    : public IRemoteStorageProvider
{
    TTestRemoteStorageProvider(TShardingConfigPtr config)
        : IRemoteStorageProvider(std::move(config))
    {}

    [[nodiscard]] TShardClient GetShardClient(
        const TString& shardId,
        NClient::TClientAppConfigPtr clientConfig) override
    {
        Y_UNUSED(shardId);
        Y_UNUSED(clientConfig);
        return {};
    }

    [[nodiscard]] TShardClients GetShardsClients(
        NClient::TClientAppConfigPtr clientConfig) override
    {
        Y_UNUSED(clientConfig);
        return Clients;
    }

    void Start() override
    {}

    void Stop() override
    {}

    void SetShardClients(TShardClients clients)
    {
        Clients = std::move(clients);
    }

    TShardClients Clients;
};

////////////////////////////////////////////////////////////////////////////////

struct TTestServiceClient
    : public IBlockStore
{
    void Start() override
    {}

    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return {};
    }

    #define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                             \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(callContext);                                                 \
        Y_UNUSED(request);                                                     \
        ++name##Called;                                                        \
        return name##Promise.GetFuture();                                      \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

    #define BLOCKSTORE_IMPLEMENT_PROMISE(name, ...)                            \
    TPromise<NProto::T##name##Response> name##Promise =                        \
        NewPromise<NProto::T##name##Response>();                               \
    ui32 name##Called = 0;                                                     \
// BLOCKSTORE_IMPLEMENT_PROMISE

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_PROMISE)

#undef BLOCKSTORE_IMPLEMENT_PROMISE

};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDescribeVolumeTest)
{
    Y_UNIT_TEST(ShouldDescribeRemoteVolume)
    {
        NProto::TShardingConfig cfg;
        auto* shard1 = cfg.MutableShards()->Add();
        shard1->SetShardId("shard1");
        shard1->MutableHosts()->Add("host1");
        auto* shard2 = cfg.MutableShards()->Add();
        shard2->SetShardId("shard2");
        shard2->MutableHosts()->Add("host2");

        auto shardingConfig = std::make_shared<TShardingConfig>(std::move(cfg));

        auto clientAppConfig = std::make_shared<NClient::TClientAppConfig>();

        TShardClients clients;

        auto createShardClient = [&](const TString& shardId, const TString& host)
        {
            auto service = std::make_shared<TTestServiceClient>();
            clients[shardId].emplace_back(
                clientAppConfig,
                host,
                service,
                CreateRemoteStorage(service));
            return service;
        };

        auto s1h1Client = createShardClient("shard1", "s1h1");
        auto s2h1Client = createShardClient("shard2", "s2h1");

        auto storageProvider = std::make_shared<TTestRemoteStorageProvider>(
            shardingConfig);
        storageProvider->SetShardClients(clients);

        auto localService = std::make_shared<TTestServiceClient>();

        auto response = DescribeRemoteVolume(
            "shard1disk",
            NProto::THeaders(),
            localService,
            storageProvider,
            CreateLoggingService("console"),
            CreateScheduler(),
            NProto::TClientConfig());

        UNIT_ASSERT_C(response.has_value(), "No future is set");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            localService->DescribeVolumeCalled);

        NProto::TDescribeVolumeResponse msg;
        s1h1Client->DescribeVolumePromise.SetValue(std::move(msg));

        auto describeResponse = response->GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            "shard1",
            describeResponse.GetShardId());
    }

    Y_UNIT_TEST(ShouldDescribeLocalVolume)
    {
        NProto::TShardingConfig cfg;
        auto* shard1 = cfg.MutableShards()->Add();
        shard1->SetShardId("shard1");
        shard1->MutableHosts()->Add("host1");
        auto* shard2 = cfg.MutableShards()->Add();
        shard2->SetShardId("shard2");
        shard2->MutableHosts()->Add("host2");

        auto shardingConfig = std::make_shared<TShardingConfig>(std::move(cfg));

        auto clientAppConfig = std::make_shared<NClient::TClientAppConfig>();

        TShardClients clients;

        auto createShardClient = [&](const TString& shardId, const TString& host)
        {
            auto service = std::make_shared<TTestServiceClient>();
            clients[shardId].emplace_back(
                clientAppConfig,
                host,
                service,
                CreateRemoteStorage(service));
            return service;
        };

        auto s1h1Client = createShardClient("shard1", "s1h1");
        auto s2h1Client = createShardClient("shard2", "s2h1");

        auto storageProvider = std::make_shared<TTestRemoteStorageProvider>(
            shardingConfig);
        storageProvider->SetShardClients(clients);

        auto localService = std::make_shared<TTestServiceClient>();

        auto response = DescribeRemoteVolume(
            "shard1disk",
            NProto::THeaders(),
            localService,
            storageProvider,
            CreateLoggingService("console"),
            CreateScheduler(),
            NProto::TClientConfig());

        UNIT_ASSERT_C(response.has_value(), "No future is set");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            localService->DescribeVolumeCalled);

        NProto::TDescribeVolumeResponse msg;
        s1h1Client->DescribeVolumePromise.SetValue(std::move(msg));

        auto describeResponse = response->GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            "shard1",
            describeResponse.GetShardId());
    }

    Y_UNIT_TEST(ShouldReturnFatalErrorIfVolumeIsAbsent)
    {
        NProto::TShardingConfig cfg;
        auto* shard1 = cfg.MutableShards()->Add();
        shard1->SetShardId("shard1");
        shard1->MutableHosts()->Add("host1");
        auto* shard2 = cfg.MutableShards()->Add();
        shard2->SetShardId("shard2");
        shard2->MutableHosts()->Add("host2");

        auto shardingConfig = std::make_shared<TShardingConfig>(std::move(cfg));

        auto clientAppConfig = std::make_shared<NClient::TClientAppConfig>();

        TShardClients clients;

        auto createShardClient = [&](const TString& shardId, const TString& host)
        {
            auto service = std::make_shared<TTestServiceClient>();
            clients[shardId].emplace_back(
                clientAppConfig,
                host,
                service,
                CreateRemoteStorage(service));
            return service;
        };

        auto s1h1Client = createShardClient("shard1", "s1h1");
        auto s2h1Client = createShardClient("shard2", "s2h1");

        auto storageProvider = std::make_shared<TTestRemoteStorageProvider>(
            shardingConfig);
        storageProvider->SetShardClients(clients);

        auto localService = std::make_shared<TTestServiceClient>();

        auto response = DescribeRemoteVolume(
            "shard1disk",
            NProto::THeaders(),
            localService,
            storageProvider,
            CreateLoggingService("console"),
            CreateScheduler(),
            NProto::TClientConfig());

        UNIT_ASSERT_C(response.has_value(), "No future is set");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            localService->DescribeVolumeCalled);

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            s1h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            s2h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            localService->DescribeVolumePromise.SetValue(std::move(msg));
        }

        auto describeResponse = response->GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_FOUND,
            describeResponse.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "lost",
            describeResponse.GetError().GetMessage());
    }

    Y_UNIT_TEST(ShouldReturnRetribleErrorIfAtLeastOneShardIsNotReachable)
    {
        NProto::TShardingConfig cfg;
        auto* shard1 = cfg.MutableShards()->Add();
        shard1->SetShardId("shard1");
        shard1->MutableHosts()->Add("host1");
        auto* shard2 = cfg.MutableShards()->Add();
        shard2->SetShardId("shard2");
        shard2->MutableHosts()->Add("host2");

        auto shardingConfig = std::make_shared<TShardingConfig>(std::move(cfg));

        auto clientAppConfig = std::make_shared<NClient::TClientAppConfig>();

        TShardClients clients;

        auto createShardClient = [&](const TString& shardId, const TString& host)
        {
            auto service = std::make_shared<TTestServiceClient>();
            clients[shardId].emplace_back(
                clientAppConfig,
                host,
                service,
                CreateRemoteStorage(service));
            return service;
        };

        auto s1h1Client = createShardClient("shard1", "s1h1");
        auto s2h1Client = createShardClient("shard2", "s2h1");

        auto storageProvider = std::make_shared<TTestRemoteStorageProvider>(
            shardingConfig);
        storageProvider->SetShardClients(clients);

        auto localService = std::make_shared<TTestServiceClient>();

        auto response = DescribeRemoteVolume(
            "shard1disk",
            NProto::THeaders(),
            localService,
            storageProvider,
            CreateLoggingService("console"),
            CreateScheduler(),
            NProto::TClientConfig());

        UNIT_ASSERT_C(response.has_value(), "No future is set");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s2h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            localService->DescribeVolumeCalled);

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            s1h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(
                E_GRPC_UNAVAILABLE,
                "connection lost"));
            s2h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            localService->DescribeVolumePromise.SetValue(std::move(msg));
        }

        auto describeResponse = response->GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            E_GRPC_UNAVAILABLE,
            describeResponse.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "connection lost",
            describeResponse.GetError().GetMessage());
    }

    Y_UNIT_TEST(ShouldReturnRetribleErrorIfAtLeastOneShardUnavailable)
    {
        NProto::TShardingConfig cfg;
        auto* shard1 = cfg.MutableShards()->Add();
        shard1->SetShardId("shard1");
        shard1->MutableHosts()->Add("host1");
        auto* shard2 = cfg.MutableShards()->Add();
        shard2->SetShardId("shard2");
        shard2->MutableHosts()->Add("host2");

        auto shardingConfig = std::make_shared<TShardingConfig>(std::move(cfg));

        auto clientAppConfig = std::make_shared<NClient::TClientAppConfig>();

        TShardClients clients;

        auto createShardClient = [&](const TString& shardId, const TString& host)
        {
            auto service = std::make_shared<TTestServiceClient>();
            clients[shardId].emplace_back(
                clientAppConfig,
                host,
                service,
                CreateRemoteStorage(service));
            return service;
        };

        auto s1h1Client = createShardClient("shard1", "s1h1");

        auto storageProvider = std::make_shared<TTestRemoteStorageProvider>(
            shardingConfig);
        storageProvider->SetShardClients(clients);

        auto localService = std::make_shared<TTestServiceClient>();

        auto response = DescribeRemoteVolume(
            "shard1disk",
            NProto::THeaders(),
            localService,
            storageProvider,
            CreateLoggingService("console"),
            CreateScheduler(),
            NProto::TClientConfig());

        UNIT_ASSERT_C(response.has_value(), "No future is set");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            s1h1Client->DescribeVolumeCalled);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            localService->DescribeVolumeCalled);

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            s1h1Client->DescribeVolumePromise.SetValue(std::move(msg));
        }

        {
            NProto::TDescribeVolumeResponse msg;
            *msg.MutableError() = std::move(MakeError(E_NOT_FOUND, "lost"));
            localService->DescribeVolumePromise.SetValue(std::move(msg));
        }

        auto describeResponse = response->GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            describeResponse.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            "Not all shards available",
            describeResponse.GetError().GetMessage());
    }

    Y_UNIT_TEST(ShouldNotReturnFutureIfShardsAreNotConfigured)
    {
        NProto::TShardingConfig cfg;

        auto shardingConfig = std::make_shared<TShardingConfig>(std::move(cfg));

        auto clientAppConfig = std::make_shared<NClient::TClientAppConfig>();

        auto storageProvider = std::make_shared<TTestRemoteStorageProvider>(
            shardingConfig);

        auto localService = std::make_shared<TTestServiceClient>();

        auto response = DescribeRemoteVolume(
            "shard1disk",
            NProto::THeaders(),
            localService,
            storageProvider,
            CreateLoggingService("console"),
            CreateScheduler(),
            NProto::TClientConfig());

        UNIT_ASSERT_C(!response.has_value(), "No future should be returned");
    }
}

}   // namespace NCloud::NBlockStore::NSharding
