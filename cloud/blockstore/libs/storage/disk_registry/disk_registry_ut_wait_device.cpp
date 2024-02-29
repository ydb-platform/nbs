#include "disk_registry.h"
#include "disk_registry_actor.h"

#include <cloud/blockstore/config/disk.pb.h>

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_logbroker.h>
#include <cloud/blockstore/libs/storage/testlib/ss_proxy_client.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <contrib/ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>

#include <chrono>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NDiskRegistryTest;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFixture
    : public NUnitTest::TBaseFixture
{
    const TString PoolName = "chunk-10G";

    NProto::TAgentConfig AgentConfig;
    std::unique_ptr<NActors::TTestActorRuntime> Runtime;
    std::unique_ptr<TDiskRegistryClient> DiskRegistryClient;

    auto QueryAvailableStorage()
    {
        auto response = DiskRegistryClient->QueryAvailableStorage(
            TVector {AgentConfig.GetAgentId()},
            PoolName,
            NProto::STORAGE_POOL_KIND_LOCAL);

        auto& record = response->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, record.AvailableStorageSize());

        return record.GetAvailableStorage(0);
    }

    void SendAddHost()
    {
        NProto::TAction action;
        action.SetHost(AgentConfig.GetAgentId());
        action.SetType(NProto::TAction::ADD_HOST);

        DiskRegistryClient->SendCmsActionRequest(TVector { action });
    }

    auto RecvAddHostResponse()
    {
        auto response = DiskRegistryClient->RecvCmsActionResponse();
        return response->Record;
    }

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        auto createDevice = [&] (TString id, TString path) {
            auto d = Device(std::move(path), std::move(id), "rack-1", 10_GB);
            d.SetPoolKind(NProto::DEVICE_POOL_KIND_LOCAL);
            d.SetPoolName(PoolName);
            return d;
        };

        AgentConfig = CreateAgentConfig("agent-1", {
            createDevice("uuid-1", "NVMENBS01"),
            createDevice("uuid-2", "NVMENBS01"),
            createDevice("uuid-3", "NVMENBS01"),
            createDevice("uuid-4", "NVMENBS02"),
            createDevice("uuid-5", "NVMENBS02"),
            createDevice("uuid-6", "NVMENBS02")
        });

        NProto::TStorageServiceConfig config = CreateDefaultStorageConfig();
        config.SetNonReplicatedDontSuspendDevices(true);

        Runtime = TTestRuntimeBuilder()
            .WithAgents({AgentConfig})
            .With(config)
            .Build();

        DiskRegistryClient = std::make_unique<TDiskRegistryClient>(*Runtime);
        DiskRegistryClient->SetWritableState(true);
        DiskRegistryClient->UpdateConfig([&] {
            auto config = CreateRegistryConfig(0, {});
            auto& pool = *config.MutableDevicePoolConfigs()->Add();
            pool.SetName(PoolName);
            pool.SetKind(NProto::DEVICE_POOL_KIND_LOCAL);
            pool.SetAllocationUnit(10_GB);

            return config;
        }());
        DiskRegistryClient->WaitReady();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskRegistryWaitDeviceTest)
{
    Y_UNIT_TEST_F(ShouldWaitForDeviceCleanup, TFixture)
    {
        std::unique_ptr<IEventHandle> cleanupRequest;

        bool cmsActionResponseSeen = false;

        Runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvDiskRegistryPrivate::EvCleanupDevicesRequest: {
                    event->DropRewrite();
                    auto* msg = event->Get<TEvDiskRegistryPrivate::TEvCleanupDevicesRequest>();
                    UNIT_ASSERT_VALUES_EQUAL(
                        AgentConfig.DevicesSize(),
                        msg->Devices.size());
                    cleanupRequest.reset(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                case TEvService::EvCmsActionResponse:
                    cmsActionResponseSeen = true;
                    break;
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        RegisterAgents(*Runtime, 1);
        UNIT_ASSERT(!cmsActionResponseSeen);

        {
            auto response = QueryAvailableStorage();
            UNIT_ASSERT_VALUES_EQUAL_C(0, response.GetChunkCount(), response);
        }

        SendAddHost();

        UNIT_ASSERT(!cmsActionResponseSeen);

        Runtime->AdvanceCurrentTime(5min);

        UNIT_ASSERT(Runtime->DispatchEvents({
            .CustomFinalCondition = [&] { return !!cleanupRequest; }
        }));

        UNIT_ASSERT(!cmsActionResponseSeen);
        UNIT_ASSERT(cleanupRequest);

        Runtime->SetObserverFunc(&TTestActorRuntimeBase::DefaultObserverFunc);
        Runtime->Send(cleanupRequest.release());

        {
            auto response = RecvAddHostResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(1, response.ActionResultsSize(), response);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetActionResults(0).GetResult().GetCode(),
                response);
        }

        {
            auto response = QueryAvailableStorage();
            UNIT_ASSERT_VALUES_EQUAL_C(
                AgentConfig.DevicesSize(),
                response.GetChunkCount(), response);
        }

        SendAddHost();
        {
            auto response = RecvAddHostResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(1, response.ActionResultsSize(), response);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetActionResults(0).GetResult().GetCode(),
                response);
        }
    }

    Y_UNIT_TEST_F(ShouldRejectPendingRequestsOnTabletReboot, TFixture)
    {
        RegisterAgents(*Runtime, 1);

        std::unique_ptr<IEventHandle> cleanupRequest;
        Runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvDiskRegistryPrivate::EvCleanupDevicesRequest: {
                    event->DropRewrite();
                    cleanupRequest.reset(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        SendAddHost();

        Runtime->AdvanceCurrentTime(5min);

        UNIT_ASSERT(Runtime->DispatchEvents({
            .CustomFinalCondition = [&] { return !!cleanupRequest; }
        }));

        Runtime->SetObserverFunc(&TTestActorRuntimeBase::DefaultObserverFunc);
        cleanupRequest.reset();

        DiskRegistryClient->RebootTablet();

        {
            auto response = RecvAddHostResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(0, response.ActionResultsSize(), response);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response.GetError().GetCode(),
                response);
        }

        SendAddHost();

        {
            auto response = RecvAddHostResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(1, response.ActionResultsSize(), response);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetActionResults(0).GetResult().GetCode(),
                response);
        }
    }

    Y_UNIT_TEST_F(ShouldRejectPendingRequestsOnDeviceError, TFixture)
    {
        RegisterAgents(*Runtime, 1);

        std::unique_ptr<IEventHandle> cleanupRequest;
        Runtime->SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvDiskRegistryPrivate::EvCleanupDevicesRequest: {
                    event->DropRewrite();
                    cleanupRequest.reset(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        SendAddHost();

        Runtime->AdvanceCurrentTime(5min);

        UNIT_ASSERT(Runtime->DispatchEvents({
            .CustomFinalCondition = [&] { return !!cleanupRequest; }
        }));

        {
            NProto::TAgentStats stats;
            stats.SetNodeId(1);
            auto* d = stats.MutableDeviceStats()->Add();
            d->SetDeviceUUID("uuid-1");
            d->SetDeviceName("NVMENBS01");
            d->SetErrors(1);

            DiskRegistryClient->UpdateAgentStats(stats);
        }

        {
            auto response = RecvAddHostResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(1, response.ActionResultsSize(), response);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                response.GetActionResults(0).GetResult().GetCode(),
                response);
        }

        SendAddHost();

        {
            auto response = RecvAddHostResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(1, response.ActionResultsSize(), response);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TRY_AGAIN,
                response.GetActionResults(0).GetResult().GetCode(),
                response);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
