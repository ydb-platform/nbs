#include "spdk_initializer.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/common/caching_allocator.h>
#include <cloud/blockstore/libs/spdk/iface/env_stub.h>
#include <cloud/blockstore/libs/spdk/iface/env_test.h>
#include <cloud/blockstore/libs/spdk/iface/target.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TBrokenTarget final
    : public NSpdk::TTestSpdkTarget
{
public:
    TFuture<void> StartAsync() override
    {
        return MakeErrorFuture<void>(std::make_exception_ptr(
            yexception() << "can't start target"));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestEnv final
    : public NSpdk::TTestSpdkEnv
{
private:
    NSpdk::ISpdkEnvPtr Stub = NSpdk::CreateEnvStub();
    TSimpleThreadPool Queue;

public:
    TTestEnv()
    {
        Queue.Start(1);
    }

    TFuture<TString> RegisterMemoryDevice(
        const TString& name,
        ui64 blockCount,
        ui32 blockSize) override
    {
        if (name.Contains("broken")) {
            return MakeErrorFuture<TString>(std::make_exception_ptr(
                yexception() << "can't register " << name.Quote()));
        }

        return Async([=] {
            return Stub->RegisterMemoryDevice(name, blockCount, blockSize);
        }, Queue);
    }

    TFuture<NSpdk::TDeviceStats> QueryDeviceStats(const TString& name) override
    {
        return Async([=] {
            return Stub->QueryDeviceStats(name);
        }, Queue);
    }

    TFuture<void> EnableHistogram(const TString& name, bool enable) override
    {
        return Async([=] {
            return Stub->EnableHistogram(name, enable);
        }, Queue);
    }

    TFuture<void> AddTransport(const TString& transportId) override
    {
        return Async([=] {
            return Stub->AddTransport(transportId);
        }, Queue);
    }

    TFuture<void> StartListen(const TString& transportId) override
    {
        return Async([=] {
            if (transportId.Contains("broken")) {
                return MakeErrorFuture<void>(std::make_exception_ptr(
                    yexception() << "can't start listen " << transportId.Quote()));
            }

            return Stub->StartListen(transportId);
        }, Queue);
    }

    TFuture<NSpdk::ISpdkTargetPtr> CreateNVMeTarget(
        const TString& nqn,
        const TVector<TString>& devices,
        const TVector<TString>& transportIds) override
    {
        return Async([=] {
            if (nqn.Contains("broken")) {
                return MakeFuture<NSpdk::ISpdkTargetPtr>(
                    std::make_shared<TBrokenTarget>());
            }

            return Stub->CreateNVMeTarget(nqn, devices, transportIds);
        }, Queue);
    }

    TFuture<void> UnregisterDevice(const TString& name) override
    {
        return Async([=] {
            return Stub->UnregisterDevice(name);
        }, Queue);
    }
};

TInitializeSpdkResult InitializeSpdkSync(
    NProto::TDiskAgentConfig config)
{
    return InitializeSpdk(
               std::make_shared<NStorage::TDiskAgentConfig>(
                   std::move(config),
                   "rack",
                   25000),
               std::make_shared<TTestEnv>(),
               ICachingAllocatorPtr())
        .GetValueSync();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSpdkInitializerTest)
{
    Y_UNIT_TEST(ShouldInitialize)
    {
        NProto::TDiskAgentConfig config;

        {
            auto& mem = *config.AddMemoryDevices();
            mem.SetName("foo");
            mem.SetDeviceId("uuid-1");
        }

        {
            auto& mem = *config.AddMemoryDevices();
            mem.SetName("bar");
            mem.SetDeviceId("uuid-2");
        }

        {
            auto& mem = *config.AddMemoryDevices();
            mem.SetName("baz");
            mem.SetDeviceId("uuid-3");
        }

        auto [target, configs, devices, errors] = InitializeSpdkSync(
            std::move(config));

        UNIT_ASSERT(target);
        UNIT_ASSERT_VALUES_EQUAL(3, configs.size());
        UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
        UNIT_ASSERT_VALUES_EQUAL(0, errors.size());
    }

    Y_UNIT_TEST(ShouldIgnoreBadDevice)
    {
        NProto::TDiskAgentConfig config;

        {
            auto& mem = *config.AddMemoryDevices();
            mem.SetName("foo");
            mem.SetDeviceId("uuid-1");
        }

        {
            auto& mem = *config.AddMemoryDevices();
            mem.SetName("broken");
            mem.SetDeviceId("uuid-2");
        }

        {
            auto& mem = *config.AddMemoryDevices();
            mem.SetName("baz");
            mem.SetDeviceId("uuid-3");
        }

        {
            auto& target = *config.MutableNvmeTarget();
            target.SetNqn("nqn");
            target.AddTransportIds("transport-id");
        }

        auto [target, configs, devices, errors] = InitializeSpdkSync(
            std::move(config));

        UNIT_ASSERT(target);
        UNIT_ASSERT_VALUES_EQUAL(2, configs.size());
        UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
        UNIT_ASSERT_VALUES_EQUAL(1, errors.size());
    }

    Y_UNIT_TEST(ShouldHandleBadTarget)
    {
        NProto::TDiskAgentConfig config;

        {
            auto& mem = *config.AddMemoryDevices();
            mem.SetName("foo");
            mem.SetDeviceId("uuid-1");
        }

        {
            auto& mem = *config.AddMemoryDevices();
            mem.SetName("broken");
            mem.SetDeviceId("uuid-2");
        }

        {
            auto& mem = *config.AddMemoryDevices();
            mem.SetName("baz");
            mem.SetDeviceId("uuid-3");
        }

        {
            auto& target = *config.MutableNvmeTarget();
            target.SetNqn("broken");
            target.AddTransportIds("transport-id");
        }

        auto [target, configs, devices, errors] = InitializeSpdkSync(
            std::move(config));

        UNIT_ASSERT(!target);
        UNIT_ASSERT_VALUES_EQUAL(2, configs.size());
        UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
        UNIT_ASSERT_VALUES_EQUAL(2, errors.size());

        UNIT_ASSERT_EQUAL(configs[0].GetState(), NProto::DEVICE_STATE_ERROR);
        UNIT_ASSERT_EQUAL(configs[1].GetState(), NProto::DEVICE_STATE_ERROR);
    }

    Y_UNIT_TEST(ShouldHandleBadTransport)
    {
        NProto::TDiskAgentConfig config;

        {
            auto& mem = *config.AddMemoryDevices();
            mem.SetName("foo");
            mem.SetDeviceId("uuid-1");
        }

        {
            auto& mem = *config.AddMemoryDevices();
            mem.SetName("bar");
            mem.SetDeviceId("uuid-2");
        }

        {
            auto& target = *config.MutableNvmeTarget();
            target.SetNqn("nqn");
            target.AddTransportIds("broken");
        }

        auto [target, configs, devices, errors] = InitializeSpdkSync(
            std::move(config));

        UNIT_ASSERT(target);
        UNIT_ASSERT_VALUES_EQUAL(2, configs.size());
        UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
        UNIT_ASSERT_VALUES_EQUAL(1, errors.size());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
