#include "disk_agent_state.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/common/caching_allocator.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/service_local/storage_null.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/libs/spdk/iface/env_stub.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>
#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/algorithm.h>
#include <util/string/cast.h>
#include <util/system/file.h>

#include <array>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);
constexpr ui32 DefaultDeviceBlockSize = 4096;
constexpr ui64 DefaultBlocksCount = 1024*1024;
constexpr bool LockingEnabled = true;

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

    TResultOrError<TString> GetSerialNumber(const TString& path) override
    {
        return "SN-" + path;
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

    return std::make_shared<TDiskAgentConfig>(std::move(config), "rack");
}

auto CreateNullConfig(
    const std::array<const TFsPath, 3>& namespaces,
    bool acquireRequired)
{
    NProto::TDiskAgentConfig config;
    config.SetAcquireRequired(acquireRequired);
    config.SetReleaseInactiveSessionsTimeout(10000);

    for (size_t i = 0; i < namespaces.size(); ++i) {
        auto& device = *config.AddFileDevices();
        device.SetPath(namespaces[i]);
        device.SetBlockSize(DefaultDeviceBlockSize);
        device.SetDeviceId("uuid-" + ToString(i + 1));
    }

    return std::make_shared<TDiskAgentConfig>(std::move(config), "rack");
}

auto CreateDiskAgentStateSpdk(TDiskAgentConfigPtr config)
{
    return std::make_unique<TDiskAgentState>(
        std::move(config),
        NSpdk::CreateEnvStub(),
        CreateTestAllocator(),
        nullptr,    // storageProvider
        CreateProfileLogStub(),
        CreateBlockDigestGeneratorStub(),
        nullptr,    // logging
        nullptr,    // rdmaServer
        nullptr);   // nvmeManager
}

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
    const std::array<const TFsPath, 3> Nvme3s = {
        TempDir.Path() / "nvme3n1",
        TempDir.Path() / "nvme3n2",
        TempDir.Path() / "nvme3n3"
    };

    const NNvme::INvmeManagerPtr NvmeManager =
        std::make_shared<TTestNvmeManager>();

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        TFile fileData(Nvme3, EOpenModeFlag::CreateNew);
        fileData.Resize(DefaultDeviceBlockSize * DefaultBlocksCount);

        for (const auto& path: Nvme3s) {
            TFile fileData(path, EOpenModeFlag::CreateNew);
            fileData.Resize(DefaultDeviceBlockSize * DefaultBlocksCount);
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
            std::move(config),
            nullptr,    // spdk
            CreateTestAllocator(),
            NServer::CreateNullStorageProvider(),
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            nullptr,    // logging
            nullptr,    // rdmaServer
            NvmeManager);
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

    Y_UNIT_TEST_F(ShouldInitializeWithNull, TFiles)
    {
        auto state = CreateDiskAgentStateNull(
            CreateNullConfig(Nvme3s, false)
        );

        ShouldInitialize(
            *state,
            true); // checkSerialNumbers
    }

    Y_UNIT_TEST_F(ShouldProperlyProcessRequestsToUninitializedDevices, TFiles)
    {
        auto config = CreateNullConfig(Nvme3s, false);

        TDiskAgentState state(
            config,
            nullptr,    // spdk
            CreateTestAllocator(),
            std::make_shared<TStorageProvider>(THashSet<TString>{
                config->GetFileDevices()[0].GetPath(),
                config->GetFileDevices()[1].GetPath(),
            }),
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            nullptr,    // logging
            nullptr,    // rdmaServer
            NvmeManager);

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
                auto config = std::make_shared<TDiskAgentConfig>(cfg, "rack");

                TDiskAgentState state(
                    config,
                    nullptr,    // spdk
                    CreateTestAllocator(),
                    std::make_shared<TStorageProvider>(THashSet<TString>{
                        config->GetFileDevices()[0].GetPath(),
                        config->GetFileDevices()[1].GetPath(),
                        config->GetFileDevices()[2].GetPath()
                    }),
                    CreateProfileLogStub(),
                    CreateBlockDigestGeneratorStub(),
                    nullptr,    // logging
                    nullptr,    // rdmaServer
                    NvmeManager);

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
            std::make_shared<TDiskAgentConfig>(std::move(config), "rack"),
            nullptr,    // spdk
            CreateTestAllocator(),
            NServer::CreateNullStorageProvider(),
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            nullptr,    // logging
            nullptr,    // rdmaServer
            std::make_shared<TTestNvmeManager>());

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
            CreateNullConfig(Nvme3s, true)
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
                E_BS_INVALID_SESSION,                                          \
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
}

}   // namespace NCloud::NBlockStore::NStorage
