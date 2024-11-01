#include "storage_initializer.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/service_local/storage_null.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/features/features_config.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/system/fs.h>

#include <algorithm>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestNvmeManager
    : NNvme::INvmeManager
{
    THashMap<TString, TString> PathToSerial;

    explicit TTestNvmeManager(
            const TVector<std::pair<TString, TString>> pathToSerial)
        : PathToSerial{pathToSerial.cbegin(), pathToSerial.cend()}
    {}

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
        auto it = PathToSerial.find(TFsPath{path}.Basename());
        if (it == PathToSerial.end()) {
            return MakeError(MAKE_SYSTEM_ERROR(42));
        }

        return it->second;
    }

    TResultOrError<bool> IsSsd(const TString& path) override
    {
        Y_UNUSED(path);

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFixture
    : NUnitTest::TBaseFixture
{
    const ui32 DeviceCountPerPath = 8;
    const ui32 PathCount = 4;
    const ui64 DeviceSize = 64_KB;
    const ui64 PaddingSize = 4_KB;
    const ui64 HeaderSize = 4_KB;

    const TTempDir TempDir;
    const TFsPath DevicesPath = TempDir.Path() / "devices";
    const TFsPath CachedConfigPath = TempDir.Path() / "cache";

    NProto::TDiskAgentConfig DefaultConfig;

    const ILoggingServicePtr Logging = CreateLoggingService("console");

    TStorageConfigPtr StorageConfig;
    IStorageProviderPtr StorageProvider = NServer::CreateNullStorageProvider();

    void SetUpStorageDiscoveryConfig()
    {
        auto& discovery = *DefaultConfig.MutableStorageDiscoveryConfig();
        auto& path = *discovery.AddPathConfigs();
        path.SetPathRegExp(DevicesPath / "NVMENBS([0-9]{2})");
        auto& layout = *path.AddPoolConfigs()->MutableLayout();
        layout.SetDeviceSize(DeviceSize);
        layout.SetDevicePadding(PaddingSize);
        layout.SetHeaderSize(HeaderSize);
    }

    void SetUpStorage()
    {
        for (ui32 i = 0; i != PathCount; ++i) {
            PrepareFile("NVMENBS0" + ToString(i + 1));
        }
    }

    void PrepareFile(const TString& name)
    {
        TFile fileData(
            DevicesPath / name,
            EOpenModeFlag::CreateNew);
        fileData.Resize(
            HeaderSize + DeviceCountPerPath * DeviceSize +
            (DeviceCountPerPath - 1) * PaddingSize);
    }

    void SetUp(NUnitTest::TTestContext&) override
    {
        DevicesPath.MkDirs();
        CachedConfigPath.MkDirs();

        DefaultConfig.SetCachedConfigPath(CachedConfigPath / "config.txt");
        DefaultConfig.SetBackend(NProto::DISK_AGENT_BACKEND_AIO);
        DefaultConfig.SetAcquireRequired(true);

        SetUpStorageDiscoveryConfig();
        SetUpStorage();

        StorageConfig = std::make_shared<TStorageConfig>(
            NProto::TStorageServiceConfig{},
            std::make_shared<NFeatures::TFeaturesConfig>());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TInitializerTest)
{
    Y_UNIT_TEST_F(ShouldInitialize, TFixture)
    {
        const TVector<std::pair<TString, TString>> pathToSerial{
            {"NVMENBS01", "W"},
            {"NVMENBS02", "X"},
            {"NVMENBS03", "Y"},
            {"NVMENBS04", "Z"},
        };

        UNIT_ASSERT(!NFs::Exists(DefaultConfig.GetCachedConfigPath()));

        auto future = InitializeStorage(
            Logging->CreateLog("Test"),
            StorageConfig,
            std::make_shared<TDiskAgentConfig>(DefaultConfig, "rack"),
            StorageProvider,
            std::make_shared<TTestNvmeManager>(pathToSerial));

        const auto& r = future.GetValueSync();

        UNIT_ASSERT(NFs::Exists(DefaultConfig.GetCachedConfigPath()));

        UNIT_ASSERT_VALUES_EQUAL(
            PathCount * DeviceCountPerPath,
            r.Configs.size());

        UNIT_ASSERT_VALUES_EQUAL(r.Configs.size(), r.Devices.size());
        UNIT_ASSERT_VALUES_EQUAL(r.Configs.size(), r.Stats.size());
        UNIT_ASSERT_VALUES_EQUAL(0, r.Errors.size());
        UNIT_ASSERT_VALUES_EQUAL(0, r.ConfigMismatchErrors.size());
        UNIT_ASSERT_VALUES_EQUAL(0, r.DevicesWithNewSerialNumber.size());

        auto configs = r.Configs;
        SortBy(configs, [](const auto& d) { return d.GetDeviceName(); });
        for (ui32 i = 0; i != PathCount; ++i) {
            const auto& sn = pathToSerial[i].second;
            for (ui32 j = 0; j != DeviceCountPerPath; ++j) {
                UNIT_ASSERT_VALUES_EQUAL(
                    sn,
                    configs[i * DeviceCountPerPath + j].GetSerialNumber());
            }
        }
    }

    Y_UNIT_TEST_F(ShouldDetectChangeOfSerialNumber, TFixture)
    {
        const TVector<std::pair<TString, TString>> pathToSerial{
            {"NVMENBS01", "W"},
            {"NVMENBS02", "X"},
            {"NVMENBS03", "Y"},
            {"NVMENBS04", "Z"},
        };

        auto future1 = InitializeStorage(
            Logging->CreateLog("Test"),
            StorageConfig,
            std::make_shared<TDiskAgentConfig>(DefaultConfig, "rack"),
            StorageProvider,
            std::make_shared<TTestNvmeManager>(pathToSerial));

        const auto& r1 = future1.GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(0, r1.DevicesWithNewSerialNumber.size());

        const TVector<std::pair<TString, TString>> newPathToSerial{
            {"NVMENBS01", "W"},
            {"NVMENBS02", "new-X"},
            {"NVMENBS03", "Z"},
            {"NVMENBS04", "Y"},
            {"NVMENBS05", "A"},
        };

        PrepareFile("NVMENBS05");

        auto future2 = InitializeStorage(
            Logging->CreateLog("Test"),
            StorageConfig,
            std::make_shared<TDiskAgentConfig>(DefaultConfig, "rack"),
            StorageProvider,
            std::make_shared<TTestNvmeManager>(newPathToSerial));

        const auto& r2 = future2.GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(
            DeviceCountPerPath * (PathCount + 1),
            r2.Configs.size());

        UNIT_ASSERT_VALUES_EQUAL(0, r2.ConfigMismatchErrors.size());
        UNIT_ASSERT_VALUES_EQUAL(
            DeviceCountPerPath * 3,
            r2.DevicesWithNewSerialNumber.size());

        {
            auto configs = r2.Configs;
            SortBy(configs, [](const auto& d) { return d.GetDeviceName(); });
            for (ui32 i = 0; i != PathCount; ++i) {
                const auto& sn = newPathToSerial[i].second;
                for (ui32 j = 0; j != DeviceCountPerPath; ++j) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        sn,
                        configs[i * DeviceCountPerPath + j].GetSerialNumber());
                }
            }
        }

        auto future3 = InitializeStorage(
            Logging->CreateLog("Test"),
            StorageConfig,
            std::make_shared<TDiskAgentConfig>(DefaultConfig, "rack"),
            StorageProvider,
            std::make_shared<TTestNvmeManager>(newPathToSerial));

        const auto& r3 = future3.GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(0, r3.ConfigMismatchErrors.size());
        UNIT_ASSERT_VALUES_EQUAL(0, r3.DevicesWithNewSerialNumber.size());
        UNIT_ASSERT_VALUES_EQUAL(r2.Configs.size(), r3.Configs.size());
        UNIT_ASSERT(std::equal(
            r2.Configs.cbegin(),
            r2.Configs.cend(),
            r3.Configs.cbegin(),
            r3.Configs.cend(),
            [](const auto& lhs, const auto& rhs)
            {
                return lhs.GetDeviceName() == rhs.GetDeviceName() &&
                       lhs.GetDeviceUUID() == rhs.GetDeviceUUID() &&
                       lhs.GetSerialNumber() == rhs.GetSerialNumber() &&
                       lhs.GetBlockSize() == rhs.GetBlockSize() &&
                       lhs.GetBlocksCount() == rhs.GetBlocksCount() &&
                       lhs.GetPhysicalOffset() == rhs.GetPhysicalOffset();
            }));
    }

    Y_UNIT_TEST_F(
        ShouldUpdateSerialNumberWhenRestoringConfigFromCache,
        TFixture)
    {
        const TVector<std::pair<TString, TString>> pathToSerial{
            {"NVMENBS01", "W"},
            {"NVMENBS02", "X"},
            {"NVMENBS03", "Y"},
            {"NVMENBS04", "Z"},
        };

        UNIT_ASSERT(!NFs::Exists(DefaultConfig.GetCachedConfigPath()));

        auto future1 = InitializeStorage(
            Logging->CreateLog("Test"),
            StorageConfig,
            std::make_shared<TDiskAgentConfig>(DefaultConfig, "rack"),
            StorageProvider,
            std::make_shared<TTestNvmeManager>(pathToSerial));

        const auto& r1 = future1.GetValueSync();

        UNIT_ASSERT(NFs::Exists(DefaultConfig.GetCachedConfigPath()));

        const TVector<std::pair<TString, TString>> newPathToSerial{
            {"NVMENBS01", "W"},
            {"NVMENBS02", "A"},
            {"NVMENBS03", "Z"},
            {"NVMENBS04", "Y"},
        };

        auto newConfig = DefaultConfig;
        newConfig.MutableStorageDiscoveryConfig()
            ->MutablePathConfigs(0)
            ->MutablePoolConfigs(0)
            ->MutableLayout()
            ->SetDevicePadding(PaddingSize * 2);

        auto future2 = InitializeStorage(
            Logging->CreateLog("Test"),
            StorageConfig,
            std::make_shared<TDiskAgentConfig>(newConfig, "rack"),
            StorageProvider,
            std::make_shared<TTestNvmeManager>(newPathToSerial));

        const auto& r2 = future2.GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(1, r2.ConfigMismatchErrors.size());
        UNIT_ASSERT_VALUES_EQUAL(
            DeviceCountPerPath * 3,
            r2.DevicesWithNewSerialNumber.size());
        UNIT_ASSERT_VALUES_EQUAL(r1.Configs.size(), r2.Configs.size());
        UNIT_ASSERT(std::equal(
            r1.Configs.cbegin(),
            r1.Configs.cend(),
            r2.Configs.cbegin(),
            r2.Configs.cend(),
            [](const auto& lhs, const auto& rhs)
            {
                return lhs.GetDeviceName() == rhs.GetDeviceName() &&
                       lhs.GetDeviceUUID() == rhs.GetDeviceUUID() &&
                       lhs.GetBlockSize() == rhs.GetBlockSize() &&
                       lhs.GetBlocksCount() == rhs.GetBlocksCount() &&
                       lhs.GetPhysicalOffset() == rhs.GetPhysicalOffset();
            }));

        {
            auto configs = r2.Configs;
            SortBy(configs, [](const auto& d) { return d.GetDeviceName(); });
            for (ui32 i = 0; i != PathCount; ++i) {
                const auto& sn = newPathToSerial[i].second;
                for (ui32 j = 0; j != DeviceCountPerPath; ++j) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        sn,
                        configs[i * DeviceCountPerPath + j].GetSerialNumber());
                }
            }
        }
    }

    Y_UNIT_TEST_F(ShouldUpdateSerialNumberForMissingDevices, TFixture)
    {
        const TVector<std::pair<TString, TString>> pathToSerial{
            {"NVMENBS01", "W"},
            {"NVMENBS02", "X"},
            {"NVMENBS03", "Y"},
            {"NVMENBS04", "Z"},
        };

        auto future1 = InitializeStorage(
            Logging->CreateLog("Test"),
            StorageConfig,
            std::make_shared<TDiskAgentConfig>(DefaultConfig, "rack"),
            StorageProvider,
            std::make_shared<TTestNvmeManager>(pathToSerial));

        const auto& r1 = future1.GetValueSync();

        const TVector<std::pair<TString, TString>> newPathToSerial{
            {"NVMENBS01", "W"},
            {"NVMENBS02", "A"},
            {"NVMENBS03", "Z"},
            {"NVMENBS04", "Y"},
        };

        auto newConfig = DefaultConfig;
        newConfig.MutableStorageDiscoveryConfig()
            ->MutablePathConfigs(0)
            ->SetPathRegExp(DevicesPath / "NVMENBS0([1])");

        auto future2 = InitializeStorage(
            Logging->CreateLog("Test"),
            StorageConfig,
            std::make_shared<TDiskAgentConfig>(newConfig, "rack"),
            StorageProvider,
            std::make_shared<TTestNvmeManager>(newPathToSerial));

        const auto& r2 = future2.GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(1, r2.ConfigMismatchErrors.size());
        UNIT_ASSERT_VALUES_EQUAL(
            DeviceCountPerPath * 3,
            r2.DevicesWithNewSerialNumber.size());
        UNIT_ASSERT_VALUES_EQUAL(r1.Configs.size(), r2.Configs.size());
        UNIT_ASSERT(std::equal(
            r1.Configs.cbegin(),
            r1.Configs.cend(),
            r2.Configs.cbegin(),
            r2.Configs.cend(),
            [](const auto& lhs, const auto& rhs)
            {
                return lhs.GetDeviceName() == rhs.GetDeviceName() &&
                       lhs.GetDeviceUUID() == rhs.GetDeviceUUID() &&
                       lhs.GetBlockSize() == rhs.GetBlockSize() &&
                       lhs.GetBlocksCount() == rhs.GetBlocksCount() &&
                       lhs.GetPhysicalOffset() == rhs.GetPhysicalOffset();
            }));

        {
            auto configs = r2.Configs;
            SortBy(configs, [](const auto& d) { return d.GetDeviceName(); });
            for (ui32 i = 0; i != PathCount; ++i) {
                const auto& sn = newPathToSerial[i].second;
                for (ui32 j = 0; j != DeviceCountPerPath; ++j) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        sn,
                        configs[i * DeviceCountPerPath + j].GetSerialNumber());
                }
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
