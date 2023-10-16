#include "device_scanner.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/nvme/nvme.h>
#include <cloud/blockstore/libs/storage/core/config.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>
#include <util/system/file.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFixture
    : public NUnitTest::TBaseFixture
{
    const TTempDir TempDir;
    const TFsPath RootDir = TempDir.Path();

    TVector<TFsPath> Files;

    NProto::TStorageDiscoveryConfig Config;

    TFsPath GetPath(const TString& relPath) const
    {
        return RootDir / relPath;
    }

    void PrepareFiles(std::initializer_list<std::pair<TFsPath, size_t>> files)
    {
        for (const auto& [relPath, size]: files) {
            GetPath(relPath.Parent()).MkDirs();

            const TFsPath path = GetPath(relPath);

            TFile fileData(path, EOpenModeFlag::CreateNew);
            fileData.Resize(size);

            Files.push_back(path);
        }
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {
        for (const auto& path: Files) {
            path.DeleteIfExists();
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDeviceScannerTest)
{
    Y_UNIT_TEST_F(ShouldScanDevices, TFixture)
    {
        PrepareFiles({
            { "dev/disk/by-partlabel/NVMECOMPUTE01", 1_KB }, // v1
            { "dev/disk/by-partlabel/NVMECOMPUTE02", 1_KB }, // v1
            { "dev/disk/by-partlabel/NVMECOMPUTE03", 2_KB }, // v2
            { "dev/disk/by-partlabel/NVMECOMPUTE04", 2_KB }, // v2
            { "dev/disk/by-partlabel/NVMECOMPUTE05", 2_KB }, // v2
            { "dev/disk/by-partlabel/NVMENBS01", 1_KB },     // default
            { "dev/disk/by-partlabel/NVMENBS02", 1_KB },     // default
            { "dev/disk/by-partlabel/NVMENBS03", 1_KB },     // default
            { "dev/disk/by-partlabel/NVMENBS04", 10_KB },    // v3
            { "dev/disk/by-partlabel/NVMEKIKIMR01", 1_KB },
            { "dev/disk/by-partlabel/NVMEKIKIMR02", 2_KB },
            { "dev/disk/by-partlabel/NVMEKIKIMR03", 3_KB },
            { "dev/disk/by-partlabel/NVMEKIKIMR04", 10_KB },
            { "dev/disk/by-partlabel/ROTNBS01", 21_KB },    // rot
            { "dev/disk/by-partlabel/ROTNBS02", 22_KB },    // rot
            { "dev/disk/by-partlabel/ROTNBS03", 23_KB },    // rot
            { "dev/disk/by-partlabel/ROTNBS04", 24_KB },    // rot
            { "dev/nvme1n1", 1_KB },     // default
            { "dev/nvme2n1", 1_KB },     // default
            { "dev/nvme3n1", 1_KB },     // default
        });

        {
            auto& compute = *Config.AddPathConfigs();
            compute.SetPathRegExp(RootDir / "dev/disk/by-partlabel/NVMECOMPUTE([0-9]{2})");

            auto& v1 = *compute.AddPoolConfigs();
            v1.SetPoolName("v1");
            v1.SetMinSize(1_KB);
            v1.SetMaxSize(1_KB + 1);

            auto& v2 = *compute.AddPoolConfigs();
            v2.SetPoolName("v2");
            v2.SetMinSize(2_KB);
            v2.SetMaxSize(2_KB + 1);

            auto& nvme = *Config.AddPathConfigs();
            nvme.SetPathRegExp(RootDir / "dev/disk/by-partlabel/NVMENBS([0-9]{2})");

            auto& def = *nvme.AddPoolConfigs();
            def.SetMinSize(1_KB);
            def.SetMaxSize(1_KB + 1);

            auto& v3 = *nvme.AddPoolConfigs();
            v3.SetPoolName("v3");
            v3.SetMinSize(10_KB);
            v3.SetMaxSize(10_KB + 1);

            auto& rot = *Config.AddPathConfigs();
            rot.SetPathRegExp(RootDir / "dev/disk/by-partlabel/ROTNBS([0-9]{2})");

            auto& rotPool = *rot.AddPoolConfigs();
            rotPool.SetPoolName("rot");
            rotPool.SetMinSize(20_KB);
            rotPool.SetMaxSize(24_KB);

            auto& raw = *Config.AddPathConfigs();
            raw.SetPathRegExp(RootDir / "dev/nvme([0-9])n1");

            auto& rawPool = *raw.AddPoolConfigs();
            rawPool.SetPoolName("raw");
            rawPool.SetMinSize(1_KB);
            rawPool.SetMaxSize(1_KB);
        }

        const std::tuple<TString, TString, ui32> expected[] {
            { RootDir / "dev/disk/by-partlabel/NVMECOMPUTE01", "v1", 1 },
            { RootDir / "dev/disk/by-partlabel/NVMECOMPUTE02", "v1", 2 },
            { RootDir / "dev/disk/by-partlabel/NVMECOMPUTE03", "v2", 3 },
            { RootDir / "dev/disk/by-partlabel/NVMECOMPUTE04", "v2", 4 },
            { RootDir / "dev/disk/by-partlabel/NVMECOMPUTE05", "v2", 5 },
            { RootDir / "dev/disk/by-partlabel/NVMENBS01", "", 1 },
            { RootDir / "dev/disk/by-partlabel/NVMENBS02", "", 2 },
            { RootDir / "dev/disk/by-partlabel/NVMENBS03", "", 3 },
            { RootDir / "dev/disk/by-partlabel/NVMENBS04", "v3", 4 },
            { RootDir / "dev/disk/by-partlabel/ROTNBS01", "rot", 1 },
            { RootDir / "dev/disk/by-partlabel/ROTNBS02", "rot", 2 },
            { RootDir / "dev/disk/by-partlabel/ROTNBS03", "rot", 3 },
            { RootDir / "dev/disk/by-partlabel/ROTNBS04", "rot", 4 },
            { RootDir / "dev/nvme1n1", "raw", 1 },
            { RootDir / "dev/nvme2n1", "raw", 2 },
            { RootDir / "dev/nvme3n1", "raw", 3 },
        };

        TVector<std::pair<NProto::TFileDeviceArgs, ui32>> r;

        auto error = FindDevices(Config, [&] (
            auto& path,
            auto& pool,
            auto pathIndex,
            auto maxDeviceCount,
            auto blockSize,
            auto fileSize)
        {
            UNIT_ASSERT_VALUES_EQUAL(0, maxDeviceCount);

            NProto::TFileDeviceArgs f;
            f.SetPath(path);
            f.SetPoolName(pool.GetPoolName());
            f.SetBlockSize(blockSize);
            f.SetFileSize(fileSize);

            r.emplace_back(std::move(f), pathIndex);

            return MakeError(S_OK);
        });

        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        UNIT_ASSERT_VALUES_EQUAL(std::size(expected), r.size());
        SortBy(r, [] (const auto& p) {
            return p.first.GetPath();
        });

        for (size_t i = 0; i != r.size(); ++i) {
            auto& [path, poolName, expectedPathIndex] = expected[i];
            auto& [f, pathIndex] = r[i];
            UNIT_ASSERT_VALUES_EQUAL(path, f.GetPath());
            UNIT_ASSERT_VALUES_EQUAL_C(poolName, f.GetPoolName(), f);
            UNIT_ASSERT_VALUES_EQUAL_C(4_KB, f.GetBlockSize(), f);
            UNIT_ASSERT_VALUES_EQUAL_C(expectedPathIndex, pathIndex, f);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
