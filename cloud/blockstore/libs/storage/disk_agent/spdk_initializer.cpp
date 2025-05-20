#include "spdk_initializer.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/common/caching_allocator.h>
#include <cloud/blockstore/libs/service_local/broken_storage.h>
#include <cloud/blockstore/libs/service_local/storage_spdk.h>
#include <cloud/blockstore/libs/spdk/iface/device.h>
#include <cloud/blockstore/libs/spdk/iface/env.h>
#include <cloud/blockstore/libs/spdk/iface/target.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>

#include <library/cpp/iterator/zip.h>

#include <util/string/builder.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TDeviceConfig BrokenDeviceConfig()
{
    NProto::TDeviceConfig config;
    config.SetState(NProto::DEVICE_STATE_ERROR);
    config.SetStateMessage(CurrentExceptionMessage());
    config.SetBlocksCount(1);
    return config;
}

TString BuildBaseName(const TString& agentId, const TString& baseName)
{
    if (baseName) {
        return TStringBuilder() << agentId << ":" << baseName << ":";
    }
    return TStringBuilder() << agentId << ":";
}

TString BuildNVMeDeviceName(const TString& baseName, ui32 nsid)
{
    return TStringBuilder() << baseName << "n" << nsid;
}

////////////////////////////////////////////////////////////////////////////////

class TSpdkInitializer
{
private:
    const TDiskAgentConfigPtr AgentConfig;
    const NSpdk::ISpdkEnvPtr Spdk;
    const ICachingAllocatorPtr Allocator;

    TVector<TString> RegisteredDevices;
    TVector<NProto::TDeviceConfig> RegisteredDeviceConfigs;
    TVector<TString> DeviceIds;
    TVector<TString> Errors;

    TMutex Lock;

public:
    TSpdkInitializer(
        TDiskAgentConfigPtr agentConfig,
        NSpdk::ISpdkEnvPtr spdk,
        ICachingAllocatorPtr allocator);

    TFuture<TInitializeSpdkResult> Start();

private:
    TFuture<void> RegisterMemoryDevice(const NProto::TMemoryDeviceArgs& args);
    TFuture<void> RegisterFileDevice(const NProto::TFileDeviceArgs& args);
    TFuture<void> RegisterNVMeDevices(const NProto::TNVMeDeviceArgs& args);

    TFuture<void> AddTransport(const TString& transportId);
    TFuture<void> StartListen(const TString& transportId);

    TFuture<void> ProcessDevice(
        const TString& name,
        const TString& id,
        const TString& pool);

    void ProcessDeviceConfig(NProto::TDeviceConfig);

    TFuture<void> ProcessNVMeDevice(
        const TString& baseName,
        const TString& name,
        const TString& id,
        const TString& pool);

    TFuture<void> ProcessNVMeDevices(
        const TString& baseName,
        TVector<TString> names,
        TVector<TString> ids,
        const TString& pool);

    TFuture<NProto::TDeviceConfig> QueryDeviceStats(
        const TString& name,
        const TString& id);

    TFuture<NSpdk::ISpdkTargetPtr> CreateNVMeTarget();

    TInitializeSpdkResult Done(NSpdk::ISpdkTargetPtr target);
    void HandleCurrentException();

    TFuture<void> UnregisterDevices();
};

////////////////////////////////////////////////////////////////////////////////

TSpdkInitializer::TSpdkInitializer(
        TDiskAgentConfigPtr agentConfig,
        NSpdk::ISpdkEnvPtr spdk,
        ICachingAllocatorPtr allocator)
    : AgentConfig(std::move(agentConfig))
    , Spdk(std::move(spdk))
    , Allocator(std::move(allocator))
{}

TFuture<TInitializeSpdkResult> TSpdkInitializer::Start()
{
    TVector<TFuture<void>> futures;

    for (const auto& args: AgentConfig->GetMemoryDevices()) {
        futures.push_back(RegisterMemoryDevice(args));
    }

    for (const auto& args: AgentConfig->GetFileDevices()) {
        futures.push_back(RegisterFileDevice(args));
    }

    for (const auto& args: AgentConfig->GetNVMeDevices()) {
        futures.push_back(RegisterNVMeDevices(args));
    }

    const auto& target = AgentConfig->GetNVMeTarget();

    for (const auto& transportId: target.GetTransportIds()) {
        futures.push_back(AddTransport(transportId));
    }

    return WaitAll(futures)
        .Apply([=, this] (const auto& future) {
            Y_UNUSED(future);
            return CreateNVMeTarget();
        })
        .Apply([=, this] (const auto& future) {
            return Done(future.GetValue());
        });
}

TFuture<void> TSpdkInitializer::RegisterMemoryDevice(const NProto::TMemoryDeviceArgs& args)
{
    auto result = Spdk->RegisterMemoryDevice(
        args.GetName(),
        args.GetBlocksCount(),
        args.GetBlockSize());

    auto deviceId = args.GetDeviceId();
    Y_ABORT_UNLESS(deviceId);

    auto pool = args.GetPoolName();

    return result.Apply(
        [=, this] (const auto& future) {
            try {
                auto guard = Guard(Lock);

                return ProcessDevice(future.GetValue(), deviceId, pool);
            } catch (...) {
                HandleCurrentException();
            }

            return MakeFuture();
        });
}

TFuture<void> TSpdkInitializer::RegisterFileDevice(const NProto::TFileDeviceArgs& args)
{
    auto result = Spdk->RegisterFileDevice(
        args.GetPath(),
        args.GetPath(),
        args.GetBlockSize());

    auto deviceId = args.GetDeviceId();
    Y_ABORT_UNLESS(deviceId);

    auto pool = args.GetPoolName();

    return result.Apply(
        [=, this] (const auto& future) {
            try {
                auto guard = Guard(Lock);
                return ProcessDevice(future.GetValue(), deviceId, pool);
            } catch (...) {
                HandleCurrentException();

                auto config = BrokenDeviceConfig();
                config.SetDeviceName(args.GetPath());
                config.SetDeviceUUID(args.GetDeviceId());
                config.SetBlockSize(args.GetBlockSize());
                ProcessDeviceConfig(config);
            }
            return MakeFuture();
        });
}

TFuture<void> TSpdkInitializer::RegisterNVMeDevices(
    const NProto::TNVMeDeviceArgs& args)
{
    auto baseName = BuildBaseName(
        AgentConfig->GetAgentId(),
        args.GetBaseName());

    auto result = Spdk->RegisterNVMeDevices(
        baseName,
        args.GetTransportId());

    TVector<TString> deviceIds(
        args.GetDeviceIds().begin(),
        args.GetDeviceIds().end());

    Y_ABORT_UNLESS(!deviceIds.empty());

    auto pool = args.GetPoolName();

    return result.Apply(
        [=, this] (auto future) mutable {
            try {
                auto devices = future.ExtractValue();

                Y_ABORT_UNLESS(devices.size() == deviceIds.size());

                auto guard = Guard(Lock);

                return ProcessNVMeDevices(
                    baseName,
                    std::move(devices),
                    std::move(deviceIds),
                    pool);

            } catch (...) {
                HandleCurrentException();

                for (size_t i = 0; i < deviceIds.size(); i++) {
                    auto deviceName = BuildNVMeDeviceName(baseName, i);
                    auto config = BrokenDeviceConfig();
                    config.SetDeviceName(deviceName);
                    config.SetBlockSize(DefaultBlockSize);
                    config.SetDeviceUUID(deviceIds[i]);
                    config.SetBaseName(baseName);
                    ProcessDeviceConfig(config);
                }
            }

            return MakeFuture();
        });
}

TFuture<void> TSpdkInitializer::AddTransport(const TString& transportId)
{
    auto result = Spdk->AddTransport(transportId);

    return result.Apply(
        [=, this] (const auto& future) {
            try {
                future.TryRethrow();

                return StartListen(transportId);
            } catch (...) {
                HandleCurrentException();
            }

            return MakeFuture();
        });
}

TFuture<void> TSpdkInitializer::StartListen(const TString& transportId)
{
    return Spdk->StartListen(transportId).Apply(
        [=, this] (const auto& future) {
            try {
                future.TryRethrow();
            } catch (...) {
                HandleCurrentException();
            }
        });
}

void TSpdkInitializer::ProcessDeviceConfig(NProto::TDeviceConfig config)
{
    try {
        auto guard = Guard(Lock);

        const auto& target = AgentConfig->GetNVMeTarget();

        if (target.TransportIdsSize()) {
            size_t deviceIndex = DeviceIds.size();

            config.SetTransportId(TStringBuilder()
                << "ns:" << (deviceIndex + 1)
                << " " << target.GetTransportIds(0)
                << " subnqn:" << target.GetNqn());
        }

        if (config.GetBaseName().empty()) {
            auto baseName = BuildBaseName(AgentConfig->GetAgentId(), "");
            config.SetBaseName(baseName);
        }

        config.SetRack(AgentConfig->GetRack());

        DeviceIds.push_back(config.GetDeviceUUID());
        RegisteredDevices.push_back(config.GetDeviceName());
        RegisteredDeviceConfigs.push_back(std::move(config));

    } catch (...) {
        HandleCurrentException();
    }
}

TFuture<void> TSpdkInitializer::ProcessDevice(
    const TString& name,
    const TString& id,
    const TString& pool)
{
    auto stats = QueryDeviceStats(name, id)
        .Apply([=, this] (auto future) {
            auto config = future.ExtractValue();
            config.SetPoolName(pool);
            ProcessDeviceConfig(std::move(config));
        });
    return WaitAll(stats, Spdk->EnableHistogram(name, true));
}

TFuture<void> TSpdkInitializer::ProcessNVMeDevice(
    const TString& baseName,
    const TString& name,
    const TString& id,
    const TString& pool)
{
    auto stats = QueryDeviceStats(name, id)
        .Apply([=, this] (auto future) {
            auto config = future.ExtractValue();
            config.SetBaseName(baseName);
            config.SetPoolName(pool);
            ProcessDeviceConfig(std::move(config));
        });
    return WaitAll(stats, Spdk->EnableHistogram(name, true));
}

TFuture<void> TSpdkInitializer::ProcessNVMeDevices(
    const TString& baseName,
    TVector<TString> names,
    TVector<TString> ids,
    const TString& pool)
{
    // TODO: check secure erase capabilities

    TVector<TFuture<void>> futures(Reserve(ids.size()));

    for (const auto& [name, id]: Zip(names, ids)) {
        futures.push_back(ProcessNVMeDevice(baseName, name, id, pool));
    }

    return WaitAll(futures);
}

TFuture<NProto::TDeviceConfig> TSpdkInitializer::QueryDeviceStats(
    const TString& name,
    const TString& id)
{
    auto result = Spdk->QueryDeviceStats(name);

    return result.Apply(
        [=] (const auto& future) {
            const auto& stats = future.GetValue();

            NProto::TDeviceConfig config;

            config.SetDeviceName(name);
            config.SetDeviceUUID(id);
            config.SetBlockSize(stats.BlockSize);
            config.SetBlocksCount(stats.BlocksCount);

            return config;
        });
}

TFuture<void> TSpdkInitializer::UnregisterDevices()
{
    TVector<TFuture<void>> futures;

    for (auto& config: RegisteredDeviceConfigs) {
        if (config.GetState() == NProto::DEVICE_STATE_ONLINE) {
            auto unregister = Spdk->UnregisterDevice(config.GetDeviceName());
            futures.push_back(unregister);

            config.SetState(NProto::DEVICE_STATE_ERROR);
            config.SetStateMessage(CurrentExceptionMessage());
        }
    }

    return WaitAll(futures);
}

TFuture<NSpdk::ISpdkTargetPtr> TSpdkInitializer::CreateNVMeTarget()
{
    auto guard = Guard(Lock);

    const auto& target = AgentConfig->GetNVMeTarget();
    const auto& transportIds = target.GetTransportIds();

    auto result = Spdk->CreateNVMeTarget(
        target.GetNqn(),
        RegisteredDevices,
        TVector<TString> { transportIds.begin(), transportIds.end() });

    return result.Apply(
        [=, this] (const auto& future) {
            auto target = future.GetValue();
            return target->StartAsync().Apply([=, this] (const auto& future) {
                try {
                    future.TryRethrow();

                    return MakeFuture(target);
                } catch (...) {
                    HandleCurrentException();

                    return UnregisterDevices().Apply(
                        [=] (const auto& future) {
                            Y_UNUSED(future);
                            return NSpdk::ISpdkTargetPtr();
                        });
                }
            });
        });
}

TInitializeSpdkResult TSpdkInitializer::Done(NSpdk::ISpdkTargetPtr target)
{
    auto guard = Guard(Lock);

    TVector<IStoragePtr> devices(Reserve(RegisteredDeviceConfigs.size()));

    for (const auto& config: RegisteredDeviceConfigs) {
        const auto& deviceName = config.GetDeviceName();

        if (config.GetState() == NProto::DEVICE_STATE_ONLINE) {
            auto device = target->GetDevice(deviceName);

            devices.push_back(NServer::CreateSpdkStorage(
                device,
                Allocator,
                config.GetBlockSize()));
        } else {
            devices.push_back(CreateBrokenStorage());
        }
    }

    return TInitializeSpdkResult {
        .SpdkTarget = std::move(target),
        .Configs = std::move(RegisteredDeviceConfigs),
        .Devices = std::move(devices),
        .Errors = std::move(Errors)
    };
}

void TSpdkInitializer::HandleCurrentException()
{
    with_lock (Lock) {
        Errors.push_back(CurrentExceptionMessage());
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TFuture<TInitializeSpdkResult> InitializeSpdk(
    TDiskAgentConfigPtr agentConfig,
    NSpdk::ISpdkEnvPtr spdk,
    ICachingAllocatorPtr allocator)
{
    auto initializer = std::make_shared<TSpdkInitializer>(
        std::move(agentConfig),
        std::move(spdk),
        std::move(allocator));

    return initializer->Start().Subscribe([initializer] (const auto& future) {
        Y_UNUSED(future);
        Y_UNUSED(initializer);  // extend lifetime
    });
}

}   // namespace NCloud::NBlockStore::NStorage
