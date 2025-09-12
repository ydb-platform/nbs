#include "disk_agent_state.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/common/caching_allocator.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/service_local/storage_null.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/libs/spdk/iface/env_stub.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/algorithm.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/system/file.h>

#include <chrono>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = 5s;
constexpr ui32 DefaultDeviceBlockSize = 4_KB;
constexpr ui64 DefaultBlocksCount = 1024*1024;
constexpr bool LockingEnabled = true;

////////////////////////////////////////////////////////////////////////////////

struct TByDeviceUUID
{
    template <typename T>
    bool operator()(const T& lhs, const T& rhs) const
    {
        return lhs.GetDeviceUUID() < rhs.GetDeviceUUID();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestNvmeManager
    : NNvme::INvmeManager
{
    TFuture<NProto::TError> Format(
        const TString& path,
        NNvme::nvme_secure_erase_setting ses) override
    {
        Y_UNUSED(path);
        Y_UNUSED(ses);

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> Deallocate(
        const TString& path,
        ui64 offsetBytes,
        ui64 sizeBytes) override
    {
        Y_UNUSED(path);
        Y_UNUSED(offsetBytes);
        Y_UNUSED(sizeBytes);

        return MakeFuture<NProto::TError>();
    }

    TResultOrError<TString> GetSerialNumber(const TString& path) override
    {
        return "SN-" + path;
    }

    TResultOrError<bool> IsSsd(const TString& path) override
    {
        Y_UNUSED(path);

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

auto CreateTestAllocator()
{
    return CreateCachingAllocator(TDefaultAllocator::Instance(), 0, 0, 0);
}

auto CreateSpdkConfig()
{
    NProto::TDiskAgentConfig config;

    for (int i = 0; i != 3; ++i) {
        auto& device = *config.AddFileDevices();
        device.SetPath("/dev/nvme3n" + ToString(i + 1));
        device.SetBlockSize(DefaultDeviceBlockSize);
        device.SetDeviceId("uuid-" + ToString(i + 1));
    }

    return std::make_shared<TDiskAgentConfig>(std::move(config), "rack", 1000);
}

struct TNullConfigParams
{
    TVector<TFsPath> Files;
    bool AcquireRequired = false;
    TDuration DeviceIOTimeout;

    NProto::TStorageDiscoveryConfig DiscoveryConfig;

    TString CachedConfigPath;
    TString CachedSessionsPath;
};

auto CreateNullConfig(TNullConfigParams params)
{
    NProto::TDiskAgentConfig config;
    config.SetAcquireRequired(params.AcquireRequired);
    config.SetReleaseInactiveSessionsTimeout(10000);
    if (params.DeviceIOTimeout) {
        config.SetDeviceIOTimeout(params.DeviceIOTimeout.MilliSeconds());
    }

    for (size_t i = 0; i < params.Files.size(); ++i) {
        auto& device = *config.AddFileDevices();
        device.SetPath(params.Files[i]);
        device.SetBlockSize(DefaultDeviceBlockSize);
        device.SetDeviceId("uuid-" + ToString(i + 1));
    }

    *config.MutableStorageDiscoveryConfig() = std::move(params.DiscoveryConfig);

    config.SetCachedConfigPath(std::move(params.CachedConfigPath));
    config.SetCachedSessionsPath(std::move(params.CachedSessionsPath));

    return std::make_shared<TDiskAgentConfig>(std::move(config), "rack", 1000);
}

TStorageConfigPtr CreateStorageConfig()
{
    NProto::TStorageServiceConfig config;
    return std::make_shared<TStorageConfig>(
        std::move(config),
        std::make_shared<NFeatures::TFeaturesConfig>());
}

auto CreateDiskAgentStateSpdk(TDiskAgentConfigPtr config)
{
    return std::make_unique<TDiskAgentState>(
        CreateStorageConfig(),
        std::move(config),
        NSpdk::CreateEnvStub(),
        CreateTestAllocator(),
        nullptr,   // storageProvider
        CreateProfileLogStub(),
        CreateBlockDigestGeneratorStub(),
        CreateLoggingService("console"),
        nullptr,   // rdmaServer
        nullptr,   // nvmeManager
        nullptr,   // rdmaTargetConfig
        TOldRequestCounters(),
        nullptr   // multiAgentWriteHandler
    );
}

////////////////////////////////////////////////////////////////////////////////

struct TTestStorageState: TAtomicRefCount<TTestStorageState>
{
    bool DropRequests = false;
};

using TTestStorageStatePtr = TIntrusivePtr<TTestStorageState>;

struct TTestStorage: IStorage
{
    TTestStorageStatePtr StorageState;

    TTestStorage(TTestStorageStatePtr storageState)
        : StorageState(std::move(storageState))
    {
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        if (StorageState->DropRequests) {
            return NewPromise<NProto::TReadBlocksLocalResponse>();
        }

        return MakeFuture<NProto::TReadBlocksLocalResponse>();
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        if (StorageState->DropRequests) {
            return NewPromise<NProto::TWriteBlocksLocalResponse>();
        }

        return MakeFuture<NProto::TWriteBlocksLocalResponse>();
    }

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        if (StorageState->DropRequests) {
            return NewPromise<NProto::TZeroBlocksResponse>();
        }

        return MakeFuture<NProto::TZeroBlocksResponse>();
    }

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        Y_UNUSED(method);
        return MakeFuture(NProto::TError());
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    void ReportIOError() override
    {}
};

struct TTestStorageProvider: IStorageProvider
{
    TTestStorageStatePtr StorageState;

    TTestStorageProvider(TTestStorageStatePtr storageState)
        : StorageState(std::move(storageState))
    {
    }

    TFuture<IStoragePtr> CreateStorage(
        const NProto::TVolume& volume,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) override
    {
        Y_UNUSED(volume);
        Y_UNUSED(clientId);
        Y_UNUSED(accessMode);

        IStoragePtr storage = std::make_shared<TTestStorage>(StorageState);
        return MakeFuture(std::move(storage));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TStorageProvider: IStorageProvider
{
    THashSet<TString> Paths;

    TStorageProvider(THashSet<TString> paths)
        : Paths(std::move(paths))
    {
    }

    TFuture<IStoragePtr> CreateStorage(
        const NProto::TVolume& volume,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) override
    {
        Y_UNUSED(clientId);
        Y_UNUSED(accessMode);

        if (Paths.contains(volume.GetDiskId())) {
            return MakeFuture(NServer::CreateNullStorage());
        }

        return MakeErrorFuture<IStoragePtr>(
            std::make_exception_ptr(yexception() << "oops")
        );
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFiles
    : public NUnitTest::TBaseFixture
{
    const TTempDir TempDir;
    const TFsPath Nvme3 = TempDir.Path() / "nvme3";
    const TVector<TFsPath> Nvme3s = {
        TempDir.Path() / "nvme3n1",
        TempDir.Path() / "nvme3n2",
        TempDir.Path() / "nvme3n3"
    };

    const TString CachedConfigPath = TempDir.Path() / "nbs-disk-agent.txt";
    const TString CachedSessionsPath = TempDir.Path() / "nbs-disk-agent-sessions.txt";

    const ui64 DefaultFileSize = DefaultDeviceBlockSize * DefaultBlocksCount;

    const NNvme::INvmeManagerPtr NvmeManager =
        std::make_shared<TTestNvmeManager>();

    void PrepareFile(const TString& path, size_t size)
    {
        TFile fileData(path, EOpenModeFlag::CreateNew);
        fileData.Resize(size);
    }

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        TFile fileData(Nvme3, EOpenModeFlag::CreateNew);
        fileData.Resize(DefaultFileSize);

        for (const auto& path: Nvme3s) {
            PrepareFile(path, DefaultFileSize);
        }
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {
        Nvme3.DeleteIfExists();
        for (const auto& path: Nvme3s) {
            path.DeleteIfExists();
        }
    }

    auto CreateDiskAgentStateNull(TDiskAgentConfigPtr config)
    {
        return std::make_unique<TDiskAgentState>(
            CreateStorageConfig(),
            std::move(config),
            nullptr,   // spdk
            CreateTestAllocator(),
            NServer::CreateNullStorageProvider(),
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            CreateLoggingService("console"),
            nullptr,   // rdmaServer
            NvmeManager,
            nullptr,   // rdmaTargetConfig
            TOldRequestCounters(),
            nullptr   // multiAgentWriteHandler
        );
    }
};

////////////////////////////////////////////////////////////////////////////////

void CheckLockedPaths(
    const TVector<NProto::TDeviceConfig>& configs,
    bool lockingEnabled)
{
    if (lockingEnabled) {
        for (const auto& config: configs) {
            UNIT_ASSERT(!TDeviceGuard().Lock(config.GetDeviceName()));
        }
    } else {
        for (const auto& config: configs) {
            if (TFsPath(config.GetDeviceName()).Exists()) {
                UNIT_ASSERT(TDeviceGuard().Lock(config.GetDeviceName()));
            } else {
                UNIT_ASSERT(!TDeviceGuard().Lock(config.GetDeviceName()));
            }
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskAgentStateTest)
{
    void ShouldInitialize(TDiskAgentState& state, bool checkSerialNumbers)
    {
        auto future = state.Initialize();
        const auto& r = future.GetValue(WaitTimeout);

        UNIT_ASSERT(r.Errors.empty());
        UNIT_ASSERT_VALUES_EQUAL(3, r.Configs.size());

        auto stats = state.CollectStats().GetValue(WaitTimeout);

        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetInitErrorsCount());
        UNIT_ASSERT_VALUES_EQUAL(3, stats.GetDeviceStats().size());

        for (int i = 0; i != 3; ++i) {
            const TString expected = "uuid-" + ToString(i + 1);

            UNIT_ASSERT_VALUES_EQUAL(expected, r.Configs[i].GetDeviceUUID());

            if (checkSerialNumbers) {
                UNIT_ASSERT_VALUES_EQUAL(
                    "SN-" + r.Configs[i].GetDeviceName(),
                    r.Configs[i].GetSerialNumber());
            }

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultDeviceBlockSize,
                r.Configs[i].GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultBlocksCount,
                r.Configs[i].GetBlocksCount());
        }

        {
            NProto::TReadDeviceBlocksRequest request;
            request.SetDeviceUUID("uuid-1");
            request.SetStartIndex(1);
            request.SetBlockSize(4096);
            request.SetBlocksCount(10);

            auto response = state.Read(Now(), std::move(request))
                .GetValue(WaitTimeout);

            UNIT_ASSERT(!HasError(response));
            UNIT_ASSERT_VALUES_EQUAL(10, response.GetBlocks().BuffersSize());
            for (const auto& buffer: response.GetBlocks().GetBuffers()) {
                UNIT_ASSERT_VALUES_EQUAL(4096, buffer.size());
            }
        }

        {
            NProto::TWriteDeviceBlocksRequest request;
            request.SetDeviceUUID("uuid-1");
            request.SetStartIndex(1);
            request.SetBlockSize(4096);

            ResizeIOVector(*request.MutableBlocks(), 10, 4096);

            auto response = state.Write(Now(), std::move(request))
                .GetValue(WaitTimeout);

            UNIT_ASSERT(!HasError(response));
        }

        {
            NProto::TZeroDeviceBlocksRequest request;
            request.SetDeviceUUID("uuid-1");
            request.SetStartIndex(1);
            request.SetBlockSize(4096);
            request.SetBlocksCount(10);

            auto response = state.WriteZeroes(Now(), std::move(request))
                .GetValue(WaitTimeout);

            UNIT_ASSERT(!HasError(response));
        }

        {
            auto error = state.SecureErase("uuid-1", {}).GetValue(WaitTimeout);
            UNIT_ASSERT(!HasError(error));
        }
    }

    Y_UNIT_TEST(ShouldInitializeWithSpdk)
    {
        auto state = CreateDiskAgentStateSpdk(
            CreateSpdkConfig());

        ShouldInitialize(
            *state,
            false); // checkSerialNumbers
    }

    Y_UNIT_TEST_F(ShouldReportDeviceGeneratedConfigMismatch, TFiles)
    {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        InitCriticalEventsCounter(counters);

        auto mismatch = counters->GetCounter(
            "DiskAgentCriticalEvents/DiskAgentConfigMismatch",
            true);

        UNIT_ASSERT_EQUAL(0, *mismatch);

        NProto::TStorageDiscoveryConfig discoveryConfig;
        auto& path = *discoveryConfig.AddPathConfigs();
        path.SetPathRegExp(TempDir.Path() / "nvme3n([0-9])");
        auto& pool = *path.AddPoolConfigs();
        // the IDs of the generated devices will differ from those in the config
        pool.SetHashSuffix("random");

        auto state = CreateDiskAgentStateNull(
            CreateNullConfig({
                .Files = Nvme3s,
                .DiscoveryConfig = std::move(discoveryConfig)
            }));

        ShouldInitialize(
            *state,
            false); // checkSerialNumbers

        UNIT_ASSERT_EQUAL(1, *mismatch);
    }

    Y_UNIT_TEST_F(ShouldProperlyProcessRequestsToUninitializedDevices, TFiles)
    {
        auto config = CreateNullConfig({ .Files = Nvme3s });

        TDiskAgentState state(
            CreateStorageConfig(),
            config,
            nullptr,   // spdk
            CreateTestAllocator(),
            std::make_shared<TStorageProvider>(THashSet<TString>{
                config->GetFileDevices()[0].GetPath(),
                config->GetFileDevices()[1].GetPath(),
            }),
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            CreateLoggingService("console"),
            nullptr,   // rdmaServer
            NvmeManager,
            nullptr,   // rdmaTargetConfig
            TOldRequestCounters(),
            nullptr   // multiAgentWriteHandler
        );

        auto future = state.Initialize();
        const auto& r = future.GetValue(WaitTimeout);

        UNIT_ASSERT_VALUES_EQUAL(1, r.Errors.size());
        UNIT_ASSERT_VALUES_EQUAL(3, r.Configs.size());

        auto stats = state.CollectStats().GetValue(WaitTimeout);

        UNIT_ASSERT_VALUES_EQUAL(1, stats.GetInitErrorsCount());
        UNIT_ASSERT_VALUES_EQUAL(3, stats.GetDeviceStats().size());

        for (int i = 0; i != 3; ++i) {
            const TString expected = "uuid-" + ToString(i + 1);

            UNIT_ASSERT_VALUES_EQUAL(expected, r.Configs[i].GetDeviceUUID());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultDeviceBlockSize,
                r.Configs[i].GetBlockSize());

            UNIT_ASSERT_VALUES_EQUAL(
                DefaultBlocksCount,
                r.Configs[i].GetBlocksCount());
        }

        const TString clientId = "client";

        try {
            state.AcquireDevices(
                {"uuid-1", "uuid-2", "uuid-3"},
                clientId,
                TInstant::Seconds(1),
                NProto::VOLUME_ACCESS_READ_WRITE,
                0,
                "vol0",
                0);
        } catch (const TServiceError& e) {
            UNIT_ASSERT_C(false, e.GetMessage());
        }

        {
            NProto::TReadDeviceBlocksRequest request;
            request.SetDeviceUUID("uuid-1");
            request.SetStartIndex(1);
            request.SetBlockSize(4096);
            request.SetBlocksCount(10);
            request.MutableHeaders()->SetClientId(clientId);

            auto response = state.Read(Now(), std::move(request))
                .GetValue(WaitTimeout);

            UNIT_ASSERT(!HasError(response));
            UNIT_ASSERT_VALUES_EQUAL(10, response.GetBlocks().BuffersSize());
            for (const auto& buffer: response.GetBlocks().GetBuffers()) {
                UNIT_ASSERT_VALUES_EQUAL(4096, buffer.size());
            }
        }

        {
            NProto::TWriteDeviceBlocksRequest request;
            request.SetDeviceUUID("uuid-1");
            request.SetStartIndex(1);
            request.SetBlockSize(4096);
            request.MutableHeaders()->SetClientId(clientId);

            ResizeIOVector(*request.MutableBlocks(), 10, 4096);

            auto response = state.Write(Now(), std::move(request))
                .GetValue(WaitTimeout);

            UNIT_ASSERT(!HasError(response));
        }

        {
            NProto::TZeroDeviceBlocksRequest request;
            request.SetDeviceUUID("uuid-1");
            request.SetStartIndex(1);
            request.SetBlockSize(4096);
            request.SetBlocksCount(10);
            request.MutableHeaders()->SetClientId(clientId);

            auto response = state.WriteZeroes(Now(), std::move(request))
                .GetValue(WaitTimeout);

            UNIT_ASSERT(!HasError(response));
        }

        try {
            state.ReleaseDevices({"uuid-1"}, clientId, "vol0", 0);
        } catch (const TServiceError& e) {
            UNIT_ASSERT_C(false, e.GetMessage());
        }

        {
            auto error = state.SecureErase("uuid-1", {}).GetValue(WaitTimeout);
            UNIT_ASSERT(!HasError(error));
        }

        try {
            NProto::TReadDeviceBlocksRequest request;
            request.SetDeviceUUID("uuid-3");
            request.SetStartIndex(1);
            request.SetBlockSize(4096);
            request.SetBlocksCount(10);
            request.MutableHeaders()->SetClientId(clientId);

            state.Read(Now(), std::move(request)).GetValue(WaitTimeout);

            UNIT_ASSERT(false);
        } catch (const TServiceError& e) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_IO,
                e.GetCode(),
                e.GetMessage()
            );
        }

        try {
            NProto::TWriteDeviceBlocksRequest request;
            request.SetDeviceUUID("uuid-3");
            request.SetStartIndex(1);
            request.SetBlockSize(4096);
            request.MutableHeaders()->SetClientId(clientId);

            ResizeIOVector(*request.MutableBlocks(), 10, 4096);

            state.Write(Now(), std::move(request)).GetValue(WaitTimeout);

            UNIT_ASSERT(false);
        } catch (const TServiceError& e) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_IO,
                e.GetCode(),
                e.GetMessage()
            );
        }

        try {
            NProto::TZeroDeviceBlocksRequest request;
            request.SetDeviceUUID("uuid-3");
            request.SetStartIndex(1);
            request.SetBlockSize(4096);
            request.SetBlocksCount(10);
            request.MutableHeaders()->SetClientId(clientId);

            state.WriteZeroes(Now(), std::move(request)).GetValue(WaitTimeout);

            UNIT_ASSERT(false);
        } catch (const TServiceError& e) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_IO,
                e.GetCode(),
                e.GetMessage()
            );
        }

        try {
            state.ReleaseDevices({"uuid-2", "uuid-3"}, clientId, "vol0", 0);
        } catch (const TServiceError& e) {
            UNIT_ASSERT_C(false, e.GetMessage());
        }

        try {
            state.SecureErase("uuid-3", {}).GetValue(WaitTimeout);

            UNIT_ASSERT(false);
        } catch (const TServiceError& e) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_IO,
                e.GetCode(),
                e.GetMessage()
            );
        }
    }

    Y_UNIT_TEST_F(ShouldCorrectlyLockDevices, TFiles)
    {
        NProto::TDiskAgentConfig protoCfg;
        protoCfg.SetReleaseInactiveSessionsTimeout(10000);

        for (int i = 0; i != 3; ++i) {
            auto& device = *protoCfg.AddFileDevices();
            device.SetPath(Nvme3s[i]);
            device.SetBlockSize(DefaultDeviceBlockSize);
            device.SetDeviceId("uuid-" + ToString(i + 1));
        }

        const auto testState =
            [&](
                NProto::TDiskAgentConfig cfg,
                bool lockingEnabled,
                size_t errors,
                bool checkLockedDevices)
            {
                auto config = std::make_shared<TDiskAgentConfig>(cfg, "rack", 1000);

                TDiskAgentState state(
                    CreateStorageConfig(),
                    config,
                    nullptr,   // spdk
                    CreateTestAllocator(),
                    std::make_shared<TStorageProvider>(THashSet<TString>{
                        config->GetFileDevices()[0].GetPath(),
                        config->GetFileDevices()[1].GetPath(),
                        config->GetFileDevices()[2].GetPath()}),
                    CreateProfileLogStub(),
                    CreateBlockDigestGeneratorStub(),
                    CreateLoggingService("console"),
                    nullptr,   // rdmaServer
                    NvmeManager,
                    nullptr,   // rdmaTargetConfig
                    TOldRequestCounters(),
                    nullptr   // multiAgentWriteHandler
                );

                auto future = state.Initialize();
                const auto& r = future.GetValue(WaitTimeout);

                if (checkLockedDevices) {
                    CheckLockedPaths(r.Configs, lockingEnabled);
                }

                UNIT_ASSERT_VALUES_EQUAL(errors, r.Errors.size());
                UNIT_ASSERT_VALUES_EQUAL(3, r.Configs.size());

                auto stats = state.CollectStats().GetValue(WaitTimeout);

                UNIT_ASSERT_VALUES_EQUAL(errors, stats.GetInitErrorsCount());
                UNIT_ASSERT_VALUES_EQUAL(3, stats.GetDeviceStats().size());
            };

        testState(protoCfg, !LockingEnabled, 0, true);

        protoCfg.SetDeviceLockingEnabled(LockingEnabled);
        testState(protoCfg, LockingEnabled, 0, true);

        Nvme3s.front().DeleteIfExists();
        testState(protoCfg, LockingEnabled, 1, true);

        TFile fileData(Nvme3s.front(), EOpenModeFlag::CreateNew);
        fileData.Resize(DefaultDeviceBlockSize * DefaultBlocksCount);
        {
            auto guard = TDeviceGuard();
            guard.Lock(Nvme3);
            testState(protoCfg, LockingEnabled, 3, false);

            protoCfg.SetDeviceLockingEnabled(!LockingEnabled);
            testState(protoCfg, !LockingEnabled, 0, false);
        }

        protoCfg.SetDeviceLockingEnabled(LockingEnabled);
        Nvme3.DeleteIfExists();
        testState(protoCfg, LockingEnabled, 0, true);

        protoCfg.SetDeviceLockingEnabled(!LockingEnabled);
        testState(protoCfg, !LockingEnabled, 0, true);
    }

    Y_UNIT_TEST(ShouldCountInitErrors)
    {
        NProto::TDiskAgentConfig config;

        for (int i = 0; i != 3; ++i) {
            auto& device = *config.AddFileDevices();
            device.SetPath("/dev/not-exists-" + ToString(i + 1));
            device.SetBlockSize(DefaultDeviceBlockSize);
            device.SetDeviceId("uuid-" + ToString(i + 1));
        }

        TDiskAgentState state(
            CreateStorageConfig(),
            std::make_shared<TDiskAgentConfig>(std::move(config), "rack", 1000),
            nullptr,   // spdk
            CreateTestAllocator(),
            NServer::CreateNullStorageProvider(),
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            CreateLoggingService("console"),
            nullptr,   // rdmaServer
            std::make_shared<TTestNvmeManager>(),
            nullptr,   // rdmaTargetConfig
            TOldRequestCounters(),
            nullptr   // multiAgentWriteHandler
        );

        auto future = state.Initialize();
        const auto& r = future.GetValue(WaitTimeout);

        UNIT_ASSERT_VALUES_EQUAL(3, r.Errors.size());
        UNIT_ASSERT_VALUES_EQUAL(3, r.Configs.size());

        auto stats = state.CollectStats().GetValue(WaitTimeout);

        UNIT_ASSERT_VALUES_EQUAL(3, stats.GetInitErrorsCount());
        UNIT_ASSERT_VALUES_EQUAL(3, stats.GetDeviceStats().size());
    }

    Y_UNIT_TEST_F(ShouldAcquireDevices, TFiles)
    {
        auto state = CreateDiskAgentStateNull(
            CreateNullConfig({ .Files = Nvme3s, .AcquireRequired = true })
        );

        auto future = state->Initialize();
        const auto& r = future.GetValue(WaitTimeout);
        UNIT_ASSERT(r.Errors.empty());

#define TEST_SHOULD_READ(clientId)                                             \
        try {                                                                  \
            NProto::TReadDeviceBlocksRequest request;                          \
            request.SetDeviceUUID("uuid-1");                                   \
            request.SetStartIndex(1);                                          \
            request.SetBlockSize(4096);                                        \
            request.SetBlocksCount(10);                                        \
            request.MutableHeaders()->SetClientId(clientId);                   \
                                                                               \
            auto response =                                                    \
                state->Read(                                                   \
                    Now(),                                                     \
                    std::move(request)                                         \
                ).GetValue(WaitTimeout);                                       \
            UNIT_ASSERT_VALUES_EQUAL_C(                                        \
                S_OK,                                                          \
                response.GetError().GetCode(),                                 \
                response.GetError().GetMessage()                               \
            );                                                                 \
        } catch (const TServiceError& e) {                                     \
            UNIT_ASSERT_C(false, e.GetMessage());                              \
        }                                                                      \
// TEST_SHOULD_READ

#define TEST_SHOULD_WRITE(clientId)                                            \
        try {                                                                  \
            NProto::TWriteDeviceBlocksRequest request;                         \
            request.SetDeviceUUID("uuid-1");                                   \
            request.SetStartIndex(1);                                          \
            request.SetBlockSize(4096);                                        \
            request.MutableHeaders()->SetClientId(clientId);                   \
                                                                               \
            ResizeIOVector(*request.MutableBlocks(), 10, 4096);                \
                                                                               \
            auto response =                                                    \
                state->Write(                                                  \
                    Now(),                                                     \
                    std::move(request)                                         \
                ).GetValue(WaitTimeout);                                       \
            UNIT_ASSERT_VALUES_EQUAL_C(                                        \
                S_OK,                                                          \
                response.GetError().GetCode(),                                 \
                response.GetError().GetMessage()                               \
            );                                                                 \
        } catch (const TServiceError& e) {                                     \
            UNIT_ASSERT_C(false, e.GetMessage());                              \
        }                                                                      \
// TEST_SHOULD_WRITE

#define TEST_SHOULD_ZERO(clientId)                                             \
        try {                                                                  \
            NProto::TZeroDeviceBlocksRequest request;                          \
            request.SetDeviceUUID("uuid-1");                                   \
            request.SetStartIndex(1);                                          \
            request.SetBlockSize(4096);                                        \
            request.SetBlocksCount(10);                                        \
            request.MutableHeaders()->SetClientId(clientId);                   \
                                                                               \
            auto response =                                                    \
                state->WriteZeroes(                                            \
                    Now(),                                                     \
                    std::move(request)                                         \
                ).GetValue(WaitTimeout);                                       \
            UNIT_ASSERT_VALUES_EQUAL_C(                                        \
                S_OK,                                                          \
                response.GetError().GetCode(),                                 \
                response.GetError().GetMessage()                               \
            );                                                                 \
        } catch (const TServiceError& e) {                                     \
            UNIT_ASSERT_C(false, e.GetMessage());                              \
        }                                                                      \
// TEST_SHOULD_ZERO

#define TEST_SHOULD_NOT_READ(clientId)                                         \
        try {                                                                  \
            NProto::TReadDeviceBlocksRequest request;                          \
            request.SetDeviceUUID("uuid-1");                                   \
            request.SetStartIndex(1);                                          \
            request.SetBlockSize(4096);                                        \
            request.SetBlocksCount(10);                                        \
            request.MutableHeaders()->SetClientId(clientId);                   \
                                                                               \
            auto response =                                                    \
                state->Read(                                                   \
                    Now(),                                                     \
                    std::move(request)                                         \
                ).GetValue(WaitTimeout);                                       \
            UNIT_ASSERT(false);                                                \
        } catch (const TServiceError& e) {                                     \
            UNIT_ASSERT_VALUES_EQUAL_C(                                        \
                E_BS_INVALID_SESSION,                                          \
                e.GetCode(),                                                   \
                e.GetMessage()                                                 \
            );                                                                 \
        }                                                                      \
// TEST_SHOULD_NOT_READ

#define TEST_SHOULD_NOT_WRITE(clientId)                                        \
        try {                                                                  \
            NProto::TWriteDeviceBlocksRequest request;                         \
            request.SetDeviceUUID("uuid-1");                                   \
            request.SetStartIndex(1);                                          \
            request.SetBlockSize(4096);                                        \
            request.MutableHeaders()->SetClientId(clientId);                   \
                                                                               \
            ResizeIOVector(*request.MutableBlocks(), 10, 4096);                \
                                                                               \
            auto response =                                                    \
                state->Write(                                                  \
                    Now(),                                                     \
                    std::move(request)                                         \
                ).GetValue(WaitTimeout);                                       \
            UNIT_ASSERT(false);                                                \
        } catch (const TServiceError& e) {                                     \
            UNIT_ASSERT_VALUES_EQUAL_C(                                        \
                E_BS_INVALID_SESSION,                                          \
                e.GetCode(),                                                   \
                e.GetMessage()                                                 \
            );                                                                 \
        }                                                                      \
// TEST_SHOULD_NOT_WRITE

#define TEST_SHOULD_NOT_ZERO(clientId)                                         \
        try {                                                                  \
            NProto::TZeroDeviceBlocksRequest request;                          \
            request.SetDeviceUUID("uuid-1");                                   \
            request.SetStartIndex(1);                                          \
            request.SetBlockSize(4096);                                        \
            request.SetBlocksCount(10);                                        \
            request.MutableHeaders()->SetClientId(clientId);                   \
                                                                               \
            auto response =                                                    \
                state->WriteZeroes(                                            \
                    Now(),                                                     \
                    std::move(request)                                         \
                ).GetValue(WaitTimeout);                                       \
            UNIT_ASSERT(false);                                                \
        } catch (const TServiceError& e) {                                     \
            UNIT_ASSERT_VALUES_EQUAL_C(                                        \
                E_BS_INVALID_SESSION,                                          \
                e.GetCode(),                                                   \
                e.GetMessage()                                                 \
            );                                                                 \
        }                                                                      \
// TEST_SHOULD_NOT_READ

#define TEST_SHOULD_ACQUIRE(clientId, ts, mode, seqNo)                         \
        try {                                                                  \
            state->AcquireDevices(                                             \
                {"uuid-1"},                                                    \
                clientId,                                                      \
                ts,                                                            \
                mode,                                                          \
                seqNo,                                                         \
                "vol0",                                                        \
                0                                                              \
            );                                                                 \
        } catch (const TServiceError& e) {                                     \
            UNIT_ASSERT_C(false, e.GetMessage());                              \
        }                                                                      \
// TEST_SHOULD_ACQUIRE

#define TEST_SHOULD_NOT_ACQUIRE(clientId, ts, mode, seqNo)                     \
        try {                                                                  \
            state->AcquireDevices(                                             \
                {"uuid-1"},                                                    \
                clientId,                                                      \
                ts,                                                            \
                mode,                                                          \
                seqNo,                                                         \
                "vol0",                                                        \
                0                                                              \
            );                                                                 \
            UNIT_ASSERT(false);                                                \
        } catch (const TServiceError& e) {                                     \
            UNIT_ASSERT_VALUES_EQUAL_C(                                        \
                E_BS_MOUNT_CONFLICT,                                           \
                e.GetCode(),                                                   \
                e.GetMessage()                                                 \
            );                                                                 \
        }                                                                      \
// TEST_SHOULD_NOT_ACQUIRE

        // read requests should fail for unregistered clients
        TEST_SHOULD_NOT_READ("some-client");

        // write requests should also fail
        TEST_SHOULD_NOT_WRITE("some-client");

        // and zero requests as well
        TEST_SHOULD_NOT_ZERO("some-client");

        // secure erase should succeed as long as there are no active clients
        {
            auto error = state->SecureErase("uuid-1", {}).GetValue(WaitTimeout);
            UNIT_ASSERT(!HasError(error));
        }

        // first rw acquire should succeed
        TEST_SHOULD_ACQUIRE(
            "writer",
            TInstant::Seconds(1),
            NProto::VOLUME_ACCESS_READ_WRITE,
            0
        );

        // there is an active rw client already - second writer should be
        // rejected
        TEST_SHOULD_NOT_ACQUIRE(
            "writer2",
            TInstant::Seconds(1),
            NProto::VOLUME_ACCESS_READ_WRITE,
            0
        );

        // but readers can register
        TEST_SHOULD_ACQUIRE(
            "reader1",
            TInstant::Seconds(1),
            NProto::VOLUME_ACCESS_READ_ONLY,
            0
        );

        // the number of readers is not limited by anything
        TEST_SHOULD_ACQUIRE(
            "reader2",
            TInstant::Seconds(1),
            NProto::VOLUME_ACCESS_READ_ONLY,
            0
        );

        // all clients should be able to read
        for (auto sessId: {"writer", "reader1", "reader2"}) {
            TEST_SHOULD_READ(sessId);
        }

        // but only writer should be able to write
        TEST_SHOULD_WRITE("writer");

        // and to zero
        TEST_SHOULD_ZERO("writer");

        // readers should not be allowed to write
        TEST_SHOULD_NOT_WRITE("reader1");
        TEST_SHOULD_NOT_WRITE("reader2");

        // and to zero
        TEST_SHOULD_NOT_ZERO("reader1");
        TEST_SHOULD_NOT_ZERO("reader2");

        // secure erase should fail - there are active clients
        try {
            auto error = state->SecureErase(
                "uuid-1",
                TInstant::Seconds(1)
            ).GetValue(WaitTimeout);
            UNIT_ASSERT(false);
        } catch (const TServiceError& e) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_INVALID_STATE,
                e.GetCode(),
                e.GetMessage()
            );
        }

        // after the inactivity period secure erase should succeed - our
        // clients are no longer considered 'active'
        try {
            auto error = state->SecureErase(
                "uuid-1",
                TInstant::Seconds(111)
            ).GetValue(WaitTimeout);
        } catch (const TServiceError& e) {
            UNIT_ASSERT_C(false, e.GetMessage());
        }

        // second writer should be able to register - previous writer is no
        // longer active (due to the inactivity period)
        TEST_SHOULD_ACQUIRE(
            "writer2",
            TInstant::Seconds(11),
            NProto::VOLUME_ACCESS_READ_WRITE,
            0
        );

        // reacquire should succeed
        TEST_SHOULD_ACQUIRE(
            "writer2",
            TInstant::Seconds(16),
            NProto::VOLUME_ACCESS_READ_WRITE,
            0
        );

        // previous writer cannot register - there is a new writer already
        TEST_SHOULD_NOT_ACQUIRE(
            "writer",
            TInstant::Seconds(21),
            NProto::VOLUME_ACCESS_READ_WRITE,
            0
        );

        // new writer should be able to write
        TEST_SHOULD_WRITE("writer2");

        // readonly reacquire for a readwrite client is fine
        TEST_SHOULD_ACQUIRE(
            "writer2",
            TInstant::Seconds(21),
            NProto::VOLUME_ACCESS_READ_ONLY,
            0
        );

        // writer2 should be unable to write/zero now
        TEST_SHOULD_NOT_WRITE("writer2");
        TEST_SHOULD_NOT_ZERO("writer2");

        // now our other writer should be able to register
        TEST_SHOULD_ACQUIRE(
            "writer",
            TInstant::Seconds(22),
            NProto::VOLUME_ACCESS_READ_WRITE,
            0
        );

        // and should be able to write
        TEST_SHOULD_WRITE("writer");
        TEST_SHOULD_ZERO("writer");

        // after writer's client expires, writer2 should be able to become a
        // writer
        TEST_SHOULD_ACQUIRE(
            "writer2",
            TInstant::Seconds(32),
            NProto::VOLUME_ACCESS_READ_WRITE,
            0
        );

        // and should be able to write
        TEST_SHOULD_WRITE("writer2");
        TEST_SHOULD_ZERO("writer2");

        // whereas writer should not be able to write
        TEST_SHOULD_NOT_WRITE("writer");
        TEST_SHOULD_NOT_ZERO("writer");

        // writer3 should be able to acquire client with a greater seqno
        TEST_SHOULD_ACQUIRE(
            "writer3",
            TInstant::Seconds(33),
            NProto::VOLUME_ACCESS_READ_WRITE,
            1
        );

        // writer4 should be able to acquire client with a greater seqno
        TEST_SHOULD_ACQUIRE(
            "writer4",
            TInstant::Seconds(34),
            NProto::VOLUME_ACCESS_READ_WRITE,
            2
        );

        // writer3 should not be able to acquire client with a lower seqno
        TEST_SHOULD_NOT_ACQUIRE(
            "writer3",
            TInstant::Seconds(35),
            NProto::VOLUME_ACCESS_READ_WRITE,
            1
        );

        // writer4 should be able to reacquire client with same seqno
        TEST_SHOULD_ACQUIRE(
            "writer4",
            TInstant::Seconds(36),
            NProto::VOLUME_ACCESS_READ_WRITE,
            2
        );
    }

    Y_UNIT_TEST_F(ShouldConvertIOTimeoutsToErrors, TFiles)
    {
        // using default io timeout - expecting it to be equal to 1 minute
        auto config = CreateNullConfig({ .Files = Nvme3s });

        TTestStorageStatePtr storageState = MakeIntrusive<TTestStorageState>();

        TDiskAgentState state(
            CreateStorageConfig(),
            config,
            nullptr,   // spdk
            CreateTestAllocator(),
            std::make_shared<TTestStorageProvider>(storageState),
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            CreateLoggingService("console"),
            nullptr,   // rdmaServer
            NvmeManager,
            nullptr,   // rdmaTargetConfig
            TOldRequestCounters(),
            nullptr   // multiAgentWriteHandler
        );

        auto future = state.Initialize();
        const auto& r = future.GetValue(WaitTimeout);

        UNIT_ASSERT_VALUES_EQUAL(0, r.Errors.size());
        UNIT_ASSERT_VALUES_EQUAL(3, r.Configs.size());

        auto stats = state.CollectStats().GetValue(WaitTimeout);

        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetInitErrorsCount());
        UNIT_ASSERT_VALUES_EQUAL(3, stats.GetDeviceStats().size());

        auto now = TInstant::Seconds(1);

        {
            NProto::TReadDeviceBlocksRequest request;
            request.SetDeviceUUID("uuid-1");
            request.SetStartIndex(1);
            request.SetBlockSize(4096);
            request.SetBlocksCount(10);

            auto response = state.Read(now, std::move(request))
                .GetValue(WaitTimeout);

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }

        {
            NProto::TWriteDeviceBlocksRequest request;
            request.SetDeviceUUID("uuid-1");
            request.SetStartIndex(1);
            request.SetBlockSize(4096);

            ResizeIOVector(*request.MutableBlocks(), 10, 4096);

            auto response = state.Write(now, std::move(request))
                .GetValue(WaitTimeout);

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }

        {
            NProto::TZeroDeviceBlocksRequest request;
            request.SetDeviceUUID("uuid-1");
            request.SetStartIndex(1);
            request.SetBlockSize(4096);
            request.SetBlocksCount(10);

            auto response = state.WriteZeroes(now, std::move(request))
                .GetValue(WaitTimeout);

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }

        storageState->DropRequests = true;

        {
            NProto::TReadDeviceBlocksRequest request;
            request.SetDeviceUUID("uuid-1");
            request.SetStartIndex(1);
            request.SetBlockSize(4096);
            request.SetBlocksCount(10);

            auto future = state.Read(now, std::move(request));
            now += TDuration::Seconds(59);
            state.CheckIOTimeouts(now);
            UNIT_ASSERT(!future.HasValue() && !future.HasException());
            now += TDuration::Seconds(2);
            state.CheckIOTimeouts(now);
            auto response = future.GetValue(WaitTimeout);

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_IO,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }

        {
            NProto::TWriteDeviceBlocksRequest request;
            request.SetDeviceUUID("uuid-1");
            request.SetStartIndex(1);
            request.SetBlockSize(4096);

            ResizeIOVector(*request.MutableBlocks(), 10, 4096);

            auto future = state.Write(now, std::move(request));
            now += TDuration::Seconds(59);
            state.CheckIOTimeouts(now);
            UNIT_ASSERT(!future.HasValue() && !future.HasException());
            now += TDuration::Seconds(2);
            state.CheckIOTimeouts(now);
            auto response = future.GetValue(WaitTimeout);

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_IO,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }

        {
            NProto::TZeroDeviceBlocksRequest request;
            request.SetDeviceUUID("uuid-1");
            request.SetStartIndex(1);
            request.SetBlockSize(4096);
            request.SetBlocksCount(10);

            auto future = state.WriteZeroes(now, std::move(request));
            now += TDuration::Seconds(59);
            state.CheckIOTimeouts(now);
            UNIT_ASSERT(!future.HasValue() && !future.HasException());
            now += TDuration::Seconds(2);
            state.CheckIOTimeouts(now);
            auto response = future.GetValue(WaitTimeout);

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_IO,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }
    }

    Y_UNIT_TEST_F(ShouldStoreCachedConfigs, TFiles)
    {
        TTestStorageStatePtr storageState = MakeIntrusive<TTestStorageState>();

        auto config = CreateNullConfig({
            .DiscoveryConfig = [&] {
                NProto::TStorageDiscoveryConfig discovery;
                auto& path = *discovery.AddPathConfigs();
                path.SetPathRegExp(TempDir.Path() / "nvme3n([0-9])");
                path.AddPoolConfigs();

                return discovery;
            }(),
            .CachedConfigPath = CachedConfigPath
        });

        auto state = std::make_unique<TDiskAgentState>(
            CreateStorageConfig(),
            config,
            nullptr,   // spdk
            CreateTestAllocator(),
            std::make_shared<TTestStorageProvider>(storageState),
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            CreateLoggingService("console"),
            nullptr,   // rdmaServer
            NvmeManager,
            nullptr,   // rdmaTargetConfig
            TOldRequestCounters(),
            nullptr   // multiAgentWriteHandler
        );

        auto future = state->Initialize();
        const auto& result = future.GetValue(WaitTimeout);

        UNIT_ASSERT_VALUES_EQUAL_C(0, result.Errors.size(), JoinSeq(", ", result.Errors));
        UNIT_ASSERT_VALUES_EQUAL(3, result.Configs.size());

        NProto::TDiskAgentConfig proto;
        ParseFromTextFormat(CachedConfigPath, proto);

        UNIT_ASSERT_VALUES_EQUAL(result.Configs.size(), proto.FileDevicesSize());
        for (size_t i = 0; i != result.Configs.size(); ++i) {
            const auto& lhs = result.Configs[i];
            const auto& rhs = proto.GetFileDevices(i);
            UNIT_ASSERT_VALUES_EQUAL(lhs.GetDeviceUUID(), rhs.GetDeviceId());
            UNIT_ASSERT_VALUES_EQUAL(lhs.GetDeviceName(), rhs.GetPath());
            UNIT_ASSERT_VALUES_EQUAL(lhs.GetBlockSize(), rhs.GetBlockSize());
            UNIT_ASSERT_VALUES_EQUAL(0, rhs.GetFileSize());
            UNIT_ASSERT_VALUES_EQUAL(lhs.GetPoolName(), rhs.GetPoolName());
        }
    }

    Y_UNIT_TEST_F(ShouldPreferCachedConfigs, TFiles)
    {
        const char* ids[] = {
            "d1dd53cfe76c762edb977b68de3783f4",
            "6cf62448aa025469fc16f494461177ff",
            "b272fa66254fd0410f26a12414813f5c"
        };

        UNIT_ASSERT_VALUES_EQUAL(std::size(ids), Nvme3s.size());

        NProto::TDiskAgentConfig cachedConfig;
        for (size_t i = 0; i != Nvme3s.size(); ++i) {
            auto& device = *cachedConfig.MutableFileDevices()->Add();
            device.SetPath(Nvme3s[i].GetPath());
            device.SetFileSize(DefaultFileSize);
            device.SetBlockSize(DefaultDeviceBlockSize);
            device.SetDeviceId(ids[i]);
            device.SetPoolName("foo");
        }
        std::sort(
            cachedConfig.MutableFileDevices()->begin(),
            cachedConfig.MutableFileDevices()->end(),
            [] (const auto& lhs, const auto& rhs) {
                return lhs.GetDeviceId() < rhs.GetDeviceId();
            });

        // there are 3 devices in the cached config
        SerializeToTextFormat(cachedConfig, CachedConfigPath);

        TTestStorageStatePtr storageState = MakeIntrusive<TTestStorageState>();

        auto config = CreateNullConfig({
            .DiscoveryConfig = [&] {
                NProto::TStorageDiscoveryConfig discovery;
                auto& path = *discovery.AddPathConfigs();

                // limit the number of devices to one
                path.SetPathRegExp(TempDir.Path() / "nvme3n(1)");
                auto& pool = *path.AddPoolConfigs();
                pool.SetPoolName("foo");

                return discovery;
            }(),
            .CachedConfigPath = CachedConfigPath
        });

        auto state = std::make_unique<TDiskAgentState>(
            CreateStorageConfig(),
            config,
            nullptr,   // spdk
            CreateTestAllocator(),
            std::make_shared<TTestStorageProvider>(storageState),
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            CreateLoggingService("console"),
            nullptr,   // rdmaServer
            NvmeManager,
            nullptr,   // rdmaTargetConfig
            TOldRequestCounters(),
            nullptr   // multiAgentWriteHandler
        );

        auto future = state->Initialize();
        const auto& result = future.GetValue(WaitTimeout);

        UNIT_ASSERT_VALUES_EQUAL(1, result.Errors.size());
        UNIT_ASSERT_STRING_CONTAINS_C(
            result.Errors[0], "has been lost", result.Errors[0]);
        UNIT_ASSERT_VALUES_EQUAL(3, result.Configs.size());
        UNIT_ASSERT_VALUES_EQUAL(3, cachedConfig.FileDevicesSize());

        for (size_t i = 0; i != cachedConfig.FileDevicesSize(); ++i) {
            const auto& lhs = result.Configs[i];
            const auto& rhs = cachedConfig.GetFileDevices(i);
            UNIT_ASSERT_VALUES_EQUAL(lhs.GetDeviceUUID(), rhs.GetDeviceId());
            UNIT_ASSERT_VALUES_EQUAL(lhs.GetDeviceName(), rhs.GetPath());
            UNIT_ASSERT_VALUES_EQUAL(lhs.GetBlockSize(), rhs.GetBlockSize());
            UNIT_ASSERT_VALUES_EQUAL(
                lhs.GetBlockSize() * lhs.GetBlocksCount(),
                rhs.GetFileSize());
            UNIT_ASSERT_VALUES_EQUAL(lhs.GetPoolName(), rhs.GetPoolName());
        }
    }

    Y_UNIT_TEST_F(ShouldFailOnBrokenCachedConfigs, TFiles)
    {
        auto config = CreateNullConfig({
            .Files = Nvme3s,
            .CachedConfigPath = CachedConfigPath
        });
        auto state = CreateDiskAgentStateNull(config);

        {
            TFile file(CachedConfigPath, EOpenModeFlag::CreateNew);
            TFileOutput(file).Write("<<< garbage >>>");
        }

        auto future = state->Initialize();

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            future.GetValue(),
            yexception, "Error parsing text-format");
    }

    Y_UNIT_TEST_F(ShouldAllowNewDevice, TFiles)
    {
        PrepareFile(TempDir.Path() / "NVMENBS01", 10 * DefaultFileSize);

        TTestStorageStatePtr storageState = MakeIntrusive<TTestStorageState>();

        NProto::TStorageDiscoveryConfig discovery;
        {
            auto& path = *discovery.AddPathConfigs();
            path.SetPathRegExp(TempDir.Path() / "NVMENBS([0-9]+)");

            // limit the device count to 8
            path.SetMaxDeviceCount(8);

            auto& pool = *path.AddPoolConfigs();
            pool.SetMaxSize(10 * DefaultFileSize);

            auto& layout = *pool.MutableLayout();
            layout.SetDeviceSize(DefaultFileSize);
        }

        auto newState = [&](auto discoveryConfig)
        {
            return std::make_unique<TDiskAgentState>(
                CreateStorageConfig(),
                CreateNullConfig(
                    {.DiscoveryConfig = discoveryConfig,
                     .CachedConfigPath = CachedConfigPath}),
                nullptr,   // spdk
                CreateTestAllocator(),
                std::make_shared<TTestStorageProvider>(storageState),
                CreateProfileLogStub(),
                CreateBlockDigestGeneratorStub(),
                CreateLoggingService("console"),
                nullptr,   // rdmaServer
                NvmeManager,
                nullptr,   // rdmaTargetConfig
                TOldRequestCounters(),
                nullptr   // multiAgentWriteHandler
            );
        };

        {
            auto state = newState(discovery);

            auto future = state->Initialize();
            const auto& result = future.GetValue(WaitTimeout);

            UNIT_ASSERT_VALUES_EQUAL_C(0, result.Errors.size(), JoinSeq(", ", result.Errors));
            UNIT_ASSERT_VALUES_EQUAL(8, result.Configs.size());

            NProto::TDiskAgentConfig proto;
            ParseFromTextFormat(CachedConfigPath, proto);

            UNIT_ASSERT_VALUES_EQUAL(result.Configs.size(), proto.FileDevicesSize());
            for (size_t i = 0; i != result.Configs.size(); ++i) {
                const auto& lhs = result.Configs[i];
                const auto& rhs = proto.GetFileDevices(i);
                UNIT_ASSERT_VALUES_EQUAL(lhs.GetDeviceUUID(), rhs.GetDeviceId());
                UNIT_ASSERT_VALUES_EQUAL(lhs.GetDeviceName(), rhs.GetPath());
                UNIT_ASSERT_VALUES_EQUAL(lhs.GetBlockSize(), rhs.GetBlockSize());
                UNIT_ASSERT_VALUES_EQUAL(DefaultFileSize, rhs.GetFileSize());
                UNIT_ASSERT_VALUES_EQUAL(lhs.GetPoolName(), rhs.GetPoolName());
            }
        }

        // change the device limit to 10
        discovery.MutablePathConfigs(0)->SetMaxDeviceCount(10);

        {
            auto state = newState(discovery);

            auto future = state->Initialize();
            const auto& result = future.GetValue(WaitTimeout);

            UNIT_ASSERT_VALUES_EQUAL_C(0, result.Errors.size(), JoinSeq(", ", result.Errors));
            UNIT_ASSERT_VALUES_EQUAL(10, result.Configs.size());

            NProto::TDiskAgentConfig proto;
            ParseFromTextFormat(CachedConfigPath, proto);

            UNIT_ASSERT_VALUES_EQUAL(result.Configs.size(), proto.FileDevicesSize());
            for (size_t i = 0; i != result.Configs.size(); ++i) {
                const auto& lhs = result.Configs[i];
                const auto& rhs = proto.GetFileDevices(i);
                UNIT_ASSERT_VALUES_EQUAL(lhs.GetDeviceUUID(), rhs.GetDeviceId());
                UNIT_ASSERT_VALUES_EQUAL(lhs.GetDeviceName(), rhs.GetPath());
                UNIT_ASSERT_VALUES_EQUAL(lhs.GetBlockSize(), rhs.GetBlockSize());
                UNIT_ASSERT_VALUES_EQUAL(DefaultFileSize, rhs.GetFileSize());
                UNIT_ASSERT_VALUES_EQUAL(lhs.GetPoolName(), rhs.GetPoolName());
            }
        }
    }

    Y_UNIT_TEST_F(ShouldCacheSessions, TFiles)
    {
        PrepareFile(TempDir.Path() / "NVMENBS01", DefaultFileSize);
        PrepareFile(TempDir.Path() / "NVMENBS02", DefaultFileSize);
        PrepareFile(TempDir.Path() / "NVMENBS03", DefaultFileSize);
        PrepareFile(TempDir.Path() / "NVMENBS04", DefaultFileSize);

        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        InitCriticalEventsCounter(counters);

        auto restoreError = counters->GetCounter(
            "DiskAgentCriticalEvents/DiskAgentSessionCacheRestoreError",
            true);

        UNIT_ASSERT_EQUAL(0, *restoreError);

        auto createState = [&] {
            NProto::TStorageDiscoveryConfig discovery;
            auto& path = *discovery.AddPathConfigs();
            path.SetPathRegExp(TempDir.Path() / "NVMENBS([0-9]+)");

            path.AddPoolConfigs();

            auto state = CreateDiskAgentStateNull(
                CreateNullConfig({
                    .AcquireRequired = true,
                    .DiscoveryConfig = discovery,
                    .CachedConfigPath = CachedConfigPath,
                    .CachedSessionsPath = CachedSessionsPath,
                })
            );

            auto future = state->Initialize();
            const auto& r = future.GetValue(WaitTimeout);
            UNIT_ASSERT_VALUES_EQUAL_C(0, r.Errors.size(), r.Errors[0]);

            return state;
        };

        auto state = createState();

        UNIT_ASSERT_EQUAL(0, *restoreError);

        auto dumpSessionsToFile = [&] {
            auto sessions = state->GetSessions();
            NProto::TDiskAgentDeviceSessionCache cache;
            cache.MutableSessions()->Assign(
                std::make_move_iterator(sessions.begin()),
                std::make_move_iterator(sessions.end()));

            SerializeToTextFormat(cache, CachedSessionsPath);
        };

        TVector<TString> devices;
        for (auto& d: state->GetDevices()) {
            devices.push_back(d.GetDeviceUUID());
        }
        Sort(devices);

        UNIT_ASSERT_VALUES_EQUAL(4, devices.size());

        // acquire a bunch of devices

        state->AcquireDevices(
            { devices[0], devices[1] },
            "writer-1",
            TInstant::FromValue(1),
            NProto::VOLUME_ACCESS_READ_WRITE,
            42,     // MountSeqNumber
            "vol0",
            1000);  // VolumeGeneration

        state->AcquireDevices(
            { devices[0], devices[1] },
            "reader-1",
            TInstant::FromValue(2),
            NProto::VOLUME_ACCESS_READ_ONLY,
            -1,     // MountSeqNumber
            "vol0",
            1001);  // VolumeGeneration

        state->AcquireDevices(
            { devices[2], devices[3] },
            "reader-2",
            TInstant::FromValue(3),
            NProto::VOLUME_ACCESS_READ_ONLY,
            -1,     // MountSeqNumber
            "vol1",
            2000);  // VolumeGeneration

        UNIT_ASSERT_VALUES_EQUAL(3, state->GetSessions().size());

        dumpSessionsToFile();

        // restart
        state = createState();

        UNIT_ASSERT_EQUAL(0, *restoreError);

        // [writer-1, reader-1, reader-2]
        UNIT_ASSERT_VALUES_EQUAL(3, state->GetSessions().size());

        auto write = [&] (auto clientId, auto uuid) {
            NProto::TWriteDeviceBlocksRequest request;
            request.MutableHeaders()->SetClientId(clientId);
            request.SetDeviceUUID(uuid);
            request.SetStartIndex(1);
            request.SetBlockSize(4096);

            ResizeIOVector(*request.MutableBlocks(), 10, 4096);

            return state->Write(Now(), std::move(request))
                .GetValue(WaitTimeout);
        };

        auto read = [&] (auto clientId, auto uuid) {
            NProto::TReadDeviceBlocksRequest request;
            request.MutableHeaders()->SetClientId(clientId);
            request.SetDeviceUUID(uuid);
            request.SetStartIndex(1);
            request.SetBlockSize(4096);
            request.SetBlocksCount(10);

            return state->Read(Now(), std::move(request))
                .GetValue(WaitTimeout);
        };

        // should reject a request with a wrong client id
        UNIT_ASSERT_EXCEPTION_SATISFIES(
            write("unknown", devices[1]),
            TServiceError,
            [] (auto& e) {
                return e.GetCode() == E_BS_INVALID_SESSION;
            });

        // should reject a request with a wrong client id
        UNIT_ASSERT_EXCEPTION_SATISFIES(
            write("reader-2", devices[0]),
            TServiceError,
            [] (auto& e) {
                return e.GetCode() == E_BS_INVALID_SESSION;
            });

        // should reject a request with a wrong client id
        UNIT_ASSERT_EXCEPTION_SATISFIES(
            read("reader-2", devices[1]),
            TServiceError,
            [] (auto& e) {
                return e.GetCode() == E_BS_INVALID_SESSION;
            });

        // should reject a write request for the read only session
        UNIT_ASSERT_EXCEPTION_SATISFIES(
            write("reader-1", devices[1]),
            TServiceError,
            [] (auto& e) {
                return e.GetCode() == E_BS_INVALID_SESSION;
            });

        // should be ok

        {
            auto error = write("writer-1", devices[0]).GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
        }

        {
            auto error = write("writer-1", devices[1]).GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
        }

        {
            auto error = read("reader-1", devices[0]).GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
        }

        {
            auto error = read("reader-1", devices[1]).GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
        }

        {
            auto error = read("reader-2", devices[2]).GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
        }

        {
            auto error = read("reader-2", devices[3]).GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
        }

        state->ReleaseDevices(
            { devices[0], devices[1] },
            "writer-1",
            "vol0",
            1001);  // VolumeGeneration

        // [reader-1, reader-2]
        UNIT_ASSERT_VALUES_EQUAL(2, state->GetSessions().size());

        dumpSessionsToFile();

        // remove reader-2's file
        TFsPath { state->GetDeviceName(devices[3]) }.DeleteIfExists();

        // restart
        state = createState();

        // [reader-1, reader-2]
        UNIT_ASSERT_VALUES_EQUAL(2, state->GetSessions().size());

        // reader-2 is still works
        UNIT_ASSERT_EQUAL(0, *restoreError);

        {
            UNIT_ASSERT_EXCEPTION_SATISFIES(
                write("writer-1", devices[0]),
                TServiceError,
                [] (auto& e) {
                    return e.GetCode() == E_BS_INVALID_SESSION;
                });
        }

        {
            UNIT_ASSERT_EXCEPTION_SATISFIES(
                write("writer-1", devices[1]),
                TServiceError,
                [] (auto& e) {
                    return e.GetCode() == E_BS_INVALID_SESSION;
                });
        }

        {
            UNIT_ASSERT_EXCEPTION_SATISFIES(
                read("reader-2", devices[3]),
                TServiceError,
                [] (auto& e) {
                    return e.GetCode() == E_NOT_FOUND;
                });
        }

        // should be ok

        {
            auto error = read("reader-1", devices[0]).GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
        }

        {
            auto error = read("reader-1", devices[1]).GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
        }

        {
            auto error = read("reader-2", devices[2]).GetError();
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
        }
    }

    Y_UNIT_TEST_F(ShouldDisableDevice, TFiles)
    {
        auto state = CreateDiskAgentStateNull(
            CreateNullConfig({ .Files = Nvme3s, .AcquireRequired = true })
        );

        auto read = [&]
        {
            NProto::TReadDeviceBlocksRequest request;
            request.SetDeviceUUID("uuid-1");
            request.SetStartIndex(1);
            request.SetBlockSize(4096);
            request.SetBlocksCount(10);
            request.MutableHeaders()->SetClientId("writer-1");

            return state->Read(Now(), std::move(request))
                .GetValueSync()
                .GetError();
        };

        auto write = [&]
        {
            NProto::TWriteDeviceBlocksRequest request;
            request.SetDeviceUUID("uuid-1");
            request.SetStartIndex(1);
            request.SetBlockSize(4096);
            request.MutableHeaders()->SetClientId("writer-1");

            ResizeIOVector(*request.MutableBlocks(), 10, 4096);

            return state->Write(Now(), std::move(request))
                .GetValueSync()
                .GetError();
        };

        auto zero = [&]
        {
            NProto::TZeroDeviceBlocksRequest request;
            request.SetDeviceUUID("uuid-1");
            request.SetStartIndex(1);
            request.SetBlockSize(4096);
            request.SetBlocksCount(10);
            request.MutableHeaders()->SetClientId("writer-1");

            return state->WriteZeroes(Now(), std::move(request))
                .GetValueSync()
                .GetError();
        };

        auto future = state->Initialize();
        const auto& r = future.GetValueSync();

        UNIT_ASSERT(r.Errors.empty());
        UNIT_ASSERT_VALUES_EQUAL(3, r.Configs.size());

        state->AcquireDevices(
            {"uuid-1"},
            "writer-1",
            TInstant::FromValue(1),
            NProto::VOLUME_ACCESS_READ_WRITE,
            42,   // MountSeqNumber
            "vol0",
            1000);   // VolumeGeneration

        state->DisableDevice("uuid-1");

        auto stats = state->CollectStats().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(3, stats.DeviceStatsSize());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetDeviceStats(0).GetErrors());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetDeviceStats(1).GetErrors());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetDeviceStats(2).GetErrors());

        UNIT_ASSERT_EXCEPTION_SATISFIES(
            read(),
            TServiceError,
            [](auto& e) { return e.GetCode() == E_IO; });

        UNIT_ASSERT_EXCEPTION_SATISFIES(
            write(),
            TServiceError,
            [](auto& e) { return e.GetCode() == E_IO; });

        UNIT_ASSERT_EXCEPTION_SATISFIES(
            zero(),
            TServiceError,
            [](auto& e) { return e.GetCode() == E_IO; });

        stats = state->CollectStats().GetValueSync();
        Sort(*stats.MutableDeviceStats(), TByDeviceUUID());

        UNIT_ASSERT_VALUES_EQUAL(3, stats.DeviceStatsSize());
        UNIT_ASSERT_VALUES_EQUAL(3, stats.GetDeviceStats(0).GetErrors()); // uuid-1
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetDeviceStats(1).GetErrors());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetDeviceStats(2).GetErrors());

        state->SuspendDevice("uuid-1");

        UNIT_ASSERT_EXCEPTION_SATISFIES(
            read(),
            TServiceError,
            [](auto& e) { return e.GetCode() == E_REJECTED; });

        UNIT_ASSERT_EXCEPTION_SATISFIES(
            write(),
            TServiceError,
            [](auto& e) { return e.GetCode() == E_REJECTED; });

        UNIT_ASSERT_EXCEPTION_SATISFIES(
            zero(),
            TServiceError,
            [](auto& e) { return e.GetCode() == E_REJECTED; });

        stats = state->CollectStats().GetValueSync();
        Sort(*stats.MutableDeviceStats(), TByDeviceUUID());

        UNIT_ASSERT_VALUES_EQUAL(3, stats.DeviceStatsSize());
        UNIT_ASSERT_VALUES_EQUAL(3, stats.GetDeviceStats(0).GetErrors()); // uuid-1
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetDeviceStats(1).GetErrors());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetDeviceStats(2).GetErrors());

        state->EnableDevice("uuid-1");

        {
            auto error = read();

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());
        }

        {
            auto error = write();

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());
        }

        {
            auto error = zero();

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());
        }

        stats = state->CollectStats().GetValueSync();
        Sort(*stats.MutableDeviceStats(), TByDeviceUUID());

        UNIT_ASSERT_VALUES_EQUAL(3, stats.DeviceStatsSize());
        UNIT_ASSERT_VALUES_EQUAL(3, stats.GetDeviceStats(0).GetErrors()); // uuid-1
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetDeviceStats(1).GetErrors());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetDeviceStats(2).GetErrors());
    }

    Y_UNIT_TEST_F(ShouldFilterOutDisableDevice, TFiles)
    {
        auto state = CreateDiskAgentStateNull(
            CreateNullConfig({ .Files = Nvme3s, .AcquireRequired = true })
        );

        auto future = state->Initialize();
        const auto& r = future.GetValueSync();

        UNIT_ASSERT(r.Errors.empty());
        UNIT_ASSERT_VALUES_EQUAL(3, r.Configs.size());
        {
            auto devices = state->GetEnabledDevices();
            Sort(devices, TByDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-3", devices[2].GetDeviceUUID());
        }

        state->SuspendDevice("uuid-1");
        {
            auto devices = state->GetEnabledDevices();
            Sort(devices, TByDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-3", devices[1].GetDeviceUUID());
        }

        state->DisableDevice("uuid-3");
        {
            auto devices = state->GetEnabledDevices();
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[0].GetDeviceUUID());
        }

        state->SuspendDevice("uuid-3");
        {
            auto devices = state->GetEnabledDevices();
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[0].GetDeviceUUID());
        }

        state->SuspendDevice("uuid-2");
        UNIT_ASSERT_VALUES_EQUAL(0, state->GetEnabledDevices().size());

        state->EnableDevice("uuid-1");
        {
            auto devices = state->GetEnabledDevices();
            UNIT_ASSERT_VALUES_EQUAL(1, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
        }

        state->EnableDevice("uuid-2");
        {
            auto devices = state->GetEnabledDevices();
            Sort(devices, TByDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(2, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
        }

        state->EnableDevice("uuid-3");
        {
            auto devices = state->GetEnabledDevices();
            Sort(devices, TByDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(3, devices.size());
            UNIT_ASSERT_VALUES_EQUAL("uuid-1", devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-2", devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL("uuid-3", devices[2].GetDeviceUUID());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
