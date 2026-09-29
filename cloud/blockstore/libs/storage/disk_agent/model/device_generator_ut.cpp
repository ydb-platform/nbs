#include "device_generator.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TStorageDiscoveryConfig::TPathConfig MakePathConfig(
    NProto::TStorageDiscoveryConfig::TPoolConfig pool,
    ui32 maxDeviceCount = 0)
{
    NProto::TStorageDiscoveryConfig::TPathConfig path;
    path.SetMaxDeviceCount(maxDeviceCount);
    *path.AddPoolConfigs() = std::move(pool);

    return path;
}

struct TFixture
    : public NUnitTest::TBaseFixture
{
    ILoggingServicePtr Logging = CreateLoggingService("console");
    TLog Log = Logging->CreateLog("BLOCKSTORE_DISK_AGENT");

    const TString AgentId = "eu-north1-a-ct2-9b.infra.nemax.nebiuscloud.net";

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {}

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDeviceGeneratorTest)
{
    Y_UNIT_TEST_F(ShouldGenerateDevices, TFixture)
    {
        NProto::TStorageDiscoveryConfig::TPoolConfig def;

        {
            auto& layout = *def.MutableLayout();
            layout.SetHeaderSize(1_GB);
            layout.SetDevicePadding(32_MB);
            layout.SetDeviceSize(93_GB);
        }

        NProto::TStorageDiscoveryConfig::TPoolConfig rot;
        rot.SetPoolName("rot");
        rot.SetHashSuffix("-rot");

        {
            auto& layout = *rot.MutableLayout();
            layout.SetHeaderSize(1_GB);
            layout.SetDevicePadding(32_MB);
            layout.SetDeviceSize(93_GB);
        }

        NProto::TStorageDiscoveryConfig::TPoolConfig local;
        local.SetPoolName("local");
        local.SetHashSuffix("-local");
        local.SetBlockSize(512);

        TDeviceGenerator gen { Log, AgentId };

        {
            gen(
                "/dev/disk/by-partlabel/NVMENBS01",
                MakePathConfig(def, 63),
                1,
                4_KB,
                6401251344384);

            auto r = gen.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(63, r.size());

            for (const auto& d: r) {
                UNIT_ASSERT_VALUES_EQUAL_C("", d.GetPoolName(), d);
                UNIT_ASSERT_VALUES_EQUAL_C(4_KB, d.GetBlockSize(), d);
                UNIT_ASSERT_VALUES_EQUAL_C(93_GB, d.GetFileSize(), d);
            }

            UNIT_ASSERT_VALUES_EQUAL_C(
                "106e06a6badd67822dcc1d449e4d793e", r[0].GetDeviceId(), r[0]);
            UNIT_ASSERT_VALUES_EQUAL_C(1073741824, r[0].GetOffset(), r[0]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "ac78b0c4f1510bf7158070271dfcf58f", r[1].GetDeviceId(), r[1]);
            UNIT_ASSERT_VALUES_EQUAL_C(100965285888, r[1].GetOffset(), r[1]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "f66be94d3144d1bf333459655fdfd9d7", r[5].GetDeviceId(), r[5]);
            UNIT_ASSERT_VALUES_EQUAL_C(500531462144, r[5].GetOffset(), r[5]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "129ad5096b4f7f2dae7e1b1719f7007b", r[10].GetDeviceId(), r[10]);
            UNIT_ASSERT_VALUES_EQUAL_C(999989182464, r[10].GetOffset(), r[10]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "289f051650402174ae0d892082aa92fc", r[62].GetDeviceId(), r[62]);
            UNIT_ASSERT_VALUES_EQUAL_C(6194349473792, r[62].GetOffset(), r[62]);
        }

        {
            gen(
                "/dev/disk/by-partlabel/ROTNBS01",
                MakePathConfig(rot, 140),
                1,
                4_KB,
                16000898564096);

            auto r = gen.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(140, r.size());

            for (const auto& d: r) {
                UNIT_ASSERT_VALUES_EQUAL_C("rot", d.GetPoolName(), d);
                UNIT_ASSERT_VALUES_EQUAL_C(4_KB, d.GetBlockSize(), d);
                UNIT_ASSERT_VALUES_EQUAL_C(93_GB, d.GetFileSize(), d);
            }

            UNIT_ASSERT_VALUES_EQUAL_C(
                "2f07f2bf30e6ffe40644fe1bf08d744f", r[0].GetDeviceId(), r[0]);
            UNIT_ASSERT_VALUES_EQUAL_C(1073741824, r[0].GetOffset(), r[0]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "a5ba4c956e2f8742722c41fdf4a6e039", r[41].GetDeviceId(), r[41]);
            UNIT_ASSERT_VALUES_EQUAL_C(4096627048448, r[41].GetOffset(), r[41]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "2441bf9832c162c41dd1c268e8006d65", r[99].GetDeviceId(), r[99]);
            UNIT_ASSERT_VALUES_EQUAL_C(9890336604160, r[99].GetOffset(), r[99]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "60db58a3fa6cc2fac2169b637023eefc", r[139].GetDeviceId(), r[139]);
            UNIT_ASSERT_VALUES_EQUAL_C(13885998366720, r[139].GetOffset(), r[139]);
        }

        {
            gen(
                "/dev/disk/by-partlabel/ROTNBS02",
                MakePathConfig(rot, 140),
                2,
                4_KB,
                15999825870848);

            auto r = gen.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(140, r.size());

            for (const auto& d: r) {
                UNIT_ASSERT_VALUES_EQUAL_C("rot", d.GetPoolName(), d);
                UNIT_ASSERT_VALUES_EQUAL_C(4_KB, d.GetBlockSize(), d);
                UNIT_ASSERT_VALUES_EQUAL_C(93_GB, d.GetFileSize(), d);
            }

            UNIT_ASSERT_VALUES_EQUAL_C(
                "919aa95b16c0bfd8fc6476fde09ef645", r[0].GetDeviceId(), r[0]);
            UNIT_ASSERT_VALUES_EQUAL_C(1073741824, r[0].GetOffset(), r[0]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "7a3b642159a0de575b1e9c700dd0bd33", r[41].GetDeviceId(), r[41]);
            UNIT_ASSERT_VALUES_EQUAL_C(4096627048448, r[41].GetOffset(), r[41]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "405158327ee766c4ce45cb1a61017293", r[99].GetDeviceId(), r[99]);
            UNIT_ASSERT_VALUES_EQUAL_C(9890336604160, r[99].GetOffset(), r[99]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "eb5e46284e440d7bb5a6aafe067fc7bc", r[139].GetDeviceId(), r[139]);
            UNIT_ASSERT_VALUES_EQUAL_C(13885998366720, r[139].GetOffset(), r[139]);
        }

        {
            gen(
                "/dev/disk/by-partlabel/NVMECOMPUTE01",
                MakePathConfig(local, 1),
                1,
                local.GetBlockSize(),
                367_GB);

            auto r = gen.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(1, r.size());

            const auto& d = r[0];

            UNIT_ASSERT_VALUES_EQUAL_C("local", d.GetPoolName(), d);
            UNIT_ASSERT_VALUES_EQUAL_C(512, d.GetBlockSize(), d);
            UNIT_ASSERT_VALUES_EQUAL_C(0, d.GetFileSize(), d);  // the file size is set only when a layout is used

            UNIT_ASSERT_VALUES_EQUAL_C(
                "c7f55aef7b99489f8a47d2f94f85a88b", d.GetDeviceId(), d);
        }
    }

    Y_UNIT_TEST_F(ShouldGenerateStableUUIDs, TFixture)
    {
        NProto::TStorageDiscoveryConfig::TPoolConfig def;

        TString expectedId;

        {
            TDeviceGenerator gen { Log, AgentId };

            gen(
                "/dev/disk/by-partlabel/NVMENBS01",
                MakePathConfig(def, 42),
                1,
                4_KB,
                93_GB);
            gen(
                "/dev/disk/by-partlabel/NVMENBS02",
                MakePathConfig(def, 0),
                2,
                4_KB,
                93_GB);

            auto r = gen.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(2, r.size());
            expectedId = r[1].GetDeviceId();
        }

        {
            TDeviceGenerator gen { Log, AgentId };

            gen(
                "/dev/disk/by-partlabel/NVMENBS02",
                MakePathConfig(def, 0),
                2,
                4_KB,
                93_GB);
            auto r = gen.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(1, r.size());
            UNIT_ASSERT_VALUES_EQUAL(expectedId, r[0].GetDeviceId());
        }
    }

    Y_UNIT_TEST_F(ShouldGenerateLogicalDevices, TFixture)
    {
        const ui64 headerSize = 1_GB;
        const ui64 padding = 32_MB;
        const ui64 deviceSize = 93_GB;
        const ui64 deviceCount = 10;
        const ui64 blockSize = 4_KB;

        NProto::TStorageDiscoveryConfig::TPoolConfig compound;

        auto& layout = *compound.MutableLayout();
        layout.SetHeaderSize(headerSize);
        layout.SetDevicePadding(padding);
        layout.SetDeviceSize(deviceSize);

        TDeviceGenerator gen { Log, AgentId };

        gen(
            "/dev/disk/by-partlabel/NVMENBS01",
            MakePathConfig(compound),
            1,      // device number
            4_KB,   // block size
            headerSize + (deviceSize + padding) * deviceCount);

        auto devices = gen.ExtractResult();
        UNIT_ASSERT_VALUES_EQUAL(10, devices.size());
        SortBy(devices, [] (const NProto::TFileDeviceArgs& d) {
            return d.GetOffset();
        });

        const TString ids[] {
            "106e06a6badd67822dcc1d449e4d793e",
            "ac78b0c4f1510bf7158070271dfcf58f",
            "1f700ac364e7da8656117bfd9b53101f",
            "054955e52a0b43d83482f66f7b48feb9",
            "ed9a0a8bb15208951e80a47c29134b7b",
            "f66be94d3144d1bf333459655fdfd9d7",
            "ac634d5316c02940b9c490a07df7665c",
            "e8f0eb076142588345f94b0e6c4ffc9f",
            "77a1755b5df703cb5c8dc6c0d7ec02d7",
            "4d774f7d69227c2e281681cce660744c"
        };

        ui64 offset = headerSize;
        for (size_t i = 0; i != devices.size(); ++i) {
            const auto& d = devices[i];
            UNIT_ASSERT_VALUES_EQUAL_C(offset, d.GetOffset(), d);
            UNIT_ASSERT_VALUES_EQUAL_C(blockSize, d.GetBlockSize(), d);
            UNIT_ASSERT_VALUES_EQUAL_C(deviceSize, d.GetFileSize(), d);
            UNIT_ASSERT_VALUES_EQUAL_C(ids[i], d.GetDeviceId(), d);

            offset += padding + deviceSize;
        }
    }

    Y_UNIT_TEST_F(ShouldGenerateDevicesWithFullPathHS, TFixture)
    {
        NProto::TStorageDiscoveryConfig::TPoolConfig def;
        def.SetHashScheme(NProto::TStorageDiscoveryConfig::HS_FULL_PATH);

        {
            auto& layout = *def.MutableLayout();
            layout.SetHeaderSize(1_GB);
            layout.SetDevicePadding(32_MB);
            layout.SetDeviceSize(93_GB);
        }

        NProto::TStorageDiscoveryConfig::TPoolConfig rot;
        rot.SetPoolName("rot");
        rot.SetHashSuffix("-rot");
        rot.SetHashScheme(NProto::TStorageDiscoveryConfig::HS_FULL_PATH);

        {
            auto& layout = *rot.MutableLayout();
            layout.SetHeaderSize(1_GB);
            layout.SetDevicePadding(32_MB);
            layout.SetDeviceSize(93_GB);
        }

        NProto::TStorageDiscoveryConfig::TPoolConfig local;
        local.SetPoolName("local");
        local.SetHashSuffix("-local");
        local.SetBlockSize(512);
        local.SetHashScheme(NProto::TStorageDiscoveryConfig::HS_FULL_PATH);

        TDeviceGenerator gen { Log, AgentId };

        {
            gen(
                "/dev/disk/by-partlabel/NVMENBS0110",
                MakePathConfig(def, 63),
                1,
                4_KB,
                6401251344384);

            auto r = gen.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(63, r.size());

            for (const auto& d: r) {
                UNIT_ASSERT_VALUES_EQUAL_C("", d.GetPoolName(), d);
                UNIT_ASSERT_VALUES_EQUAL_C(4_KB, d.GetBlockSize(), d);
                UNIT_ASSERT_VALUES_EQUAL_C(93_GB, d.GetFileSize(), d);
            }

            UNIT_ASSERT_VALUES_EQUAL_C(
                "61ec8104ba81651301635ec872b46c79", r[0].GetDeviceId(), r[0]);
            UNIT_ASSERT_VALUES_EQUAL_C(1073741824, r[0].GetOffset(), r[0]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "3df48c11d7fab0b047c60d49a9ce31f9", r[1].GetDeviceId(), r[1]);
            UNIT_ASSERT_VALUES_EQUAL_C(100965285888, r[1].GetOffset(), r[1]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "2af35ec822c99af5e71f2fd324073efb", r[5].GetDeviceId(), r[5]);
            UNIT_ASSERT_VALUES_EQUAL_C(500531462144, r[5].GetOffset(), r[5]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "7511923c1aac65bf2f58f35704e7f166", r[10].GetDeviceId(), r[10]);
            UNIT_ASSERT_VALUES_EQUAL_C(999989182464, r[10].GetOffset(), r[10]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "5e7ff006cc26cfc9c7d1e15f599c1b9b", r[62].GetDeviceId(), r[62]);
            UNIT_ASSERT_VALUES_EQUAL_C(6194349473792, r[62].GetOffset(), r[62]);
        }

        {
            gen(
                "/dev/disk/by-partlabel/ROTNBS0110",
                MakePathConfig(rot, 140),
                1,
                4_KB,
                16000898564096);

            auto r = gen.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(140, r.size());

            for (const auto& d: r) {
                UNIT_ASSERT_VALUES_EQUAL_C("rot", d.GetPoolName(), d);
                UNIT_ASSERT_VALUES_EQUAL_C(4_KB, d.GetBlockSize(), d);
                UNIT_ASSERT_VALUES_EQUAL_C(93_GB, d.GetFileSize(), d);
            }

            UNIT_ASSERT_VALUES_EQUAL_C(
                "11bfc4fd3eb316fc10cca2e09e932ae4", r[0].GetDeviceId(), r[0]);
            UNIT_ASSERT_VALUES_EQUAL_C(1073741824, r[0].GetOffset(), r[0]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "1528521ed35c86e8b0a2d9395baad914", r[41].GetDeviceId(), r[41]);
            UNIT_ASSERT_VALUES_EQUAL_C(4096627048448, r[41].GetOffset(), r[41]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "9ba483b3fbca02a26e4c4c9bb7b0865e", r[99].GetDeviceId(), r[99]);
            UNIT_ASSERT_VALUES_EQUAL_C(9890336604160, r[99].GetOffset(), r[99]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "37b6fa35b9327ec540c7bea92e20e017", r[139].GetDeviceId(), r[139]);
            UNIT_ASSERT_VALUES_EQUAL_C(13885998366720, r[139].GetOffset(), r[139]);
        }

        {
            gen(
                "/dev/disk/by-partlabel/ROTNBS0210",
                MakePathConfig(rot, 140),
                2,
                4_KB,
                15999825870848);

            auto r = gen.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(140, r.size());

            for (const auto& d: r) {
                UNIT_ASSERT_VALUES_EQUAL_C("rot", d.GetPoolName(), d);
                UNIT_ASSERT_VALUES_EQUAL_C(4_KB, d.GetBlockSize(), d);
                UNIT_ASSERT_VALUES_EQUAL_C(93_GB, d.GetFileSize(), d);
            }

            UNIT_ASSERT_VALUES_EQUAL_C(
                "cb83c102ad955dde89f0407db6144bf2", r[0].GetDeviceId(), r[0]);
            UNIT_ASSERT_VALUES_EQUAL_C(1073741824, r[0].GetOffset(), r[0]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "2b2eb302bbd950dc26261078b1c84137", r[41].GetDeviceId(), r[41]);
            UNIT_ASSERT_VALUES_EQUAL_C(4096627048448, r[41].GetOffset(), r[41]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "97e4a714ea7b4df0d5941e413949df39", r[99].GetDeviceId(), r[99]);
            UNIT_ASSERT_VALUES_EQUAL_C(9890336604160, r[99].GetOffset(), r[99]);

            UNIT_ASSERT_VALUES_EQUAL_C(
                "772bfff304f8da9c9c57a1031e7c06b3", r[139].GetDeviceId(), r[139]);
            UNIT_ASSERT_VALUES_EQUAL_C(13885998366720, r[139].GetOffset(), r[139]);
        }

        {
            gen(
                "/dev/disk/by-partlabel/NVMECOMPUTE0110",
                MakePathConfig(local, 1),
                1,
                local.GetBlockSize(),
                367_GB);

            auto r = gen.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(1, r.size());

            const auto& d = r[0];

            UNIT_ASSERT_VALUES_EQUAL_C("local", d.GetPoolName(), d);
            UNIT_ASSERT_VALUES_EQUAL_C(512, d.GetBlockSize(), d);
            UNIT_ASSERT_VALUES_EQUAL_C(0, d.GetFileSize(), d);  // the file size is set only when a layout is used

            UNIT_ASSERT_VALUES_EQUAL_C(
                "bda325237cb48ea217fc4fbc48bdb640", d.GetDeviceId(), d);
        }
    }

    Y_UNIT_TEST_F(ShouldGenerateStableUUIDsWithFullPathHS, TFixture)
    {
        NProto::TStorageDiscoveryConfig::TPoolConfig def;
        def.SetHashScheme(NProto::TStorageDiscoveryConfig::HS_FULL_PATH);

        TString expectedId;

        {
            TDeviceGenerator gen { Log, AgentId };

            gen(
                "/dev/disk/by-partlabel/NVMENBS0110",
                MakePathConfig(def, 42),
                1,
                4_KB,
                93_GB);
            gen(
                "/dev/disk/by-partlabel/NVMENBS0210",
                MakePathConfig(def, 0),
                2,
                4_KB,
                93_GB);

            auto r = gen.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(2, r.size());
            expectedId = r[1].GetDeviceId();
        }

        {
            TDeviceGenerator gen { Log, AgentId };

            gen(
                "/dev/disk/by-partlabel/NVMENBS0210",
                MakePathConfig(def, 0),
                2,
                4_KB,
                93_GB);
            auto r = gen.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(1, r.size());
            UNIT_ASSERT_VALUES_EQUAL(expectedId, r[0].GetDeviceId());
        }
    }

    Y_UNIT_TEST_F(ShouldGenerateLogicalDevicesWithFullPathHS, TFixture)
    {
        const ui64 headerSize = 1_GB;
        const ui64 padding = 32_MB;
        const ui64 deviceSize = 93_GB;
        const ui64 deviceCount = 10;
        const ui64 blockSize = 4_KB;

        NProto::TStorageDiscoveryConfig::TPoolConfig compound;

        compound.SetHashScheme(NProto::TStorageDiscoveryConfig::HS_FULL_PATH);

        auto& layout = *compound.MutableLayout();
        layout.SetHeaderSize(headerSize);
        layout.SetDevicePadding(padding);
        layout.SetDeviceSize(deviceSize);

        TDeviceGenerator gen { Log, AgentId };

        gen(
            "/dev/disk/by-partlabel/NVMENBS0110",
            MakePathConfig(compound),
            1,      // device number
            4_KB,   // block size
            headerSize + (deviceSize + padding) * deviceCount);

        auto devices = gen.ExtractResult();
        UNIT_ASSERT_VALUES_EQUAL(10, devices.size());
        SortBy(devices, [] (const NProto::TFileDeviceArgs& d) {
            return d.GetOffset();
        });

        const TString ids[] {
            "61ec8104ba81651301635ec872b46c79",
            "3df48c11d7fab0b047c60d49a9ce31f9",
            "d3b11e4d64d9cd3ec2bc7e7cc47b2cd1",
            "6497306f8b6925fceee0c0f99663e65a",
            "6ed6dbfaa1774fb928d2e3ecc34fc67b",
            "2af35ec822c99af5e71f2fd324073efb",
            "6cc315ffff98f2f4fd614ff7711a832a",
            "74dae64711245fc1b0209524717dfaaf",
            "f56f46eef98bd44546bee7f0a64eeeed",
            "f08cd547a325ade47845235e50ebdb13"
        };

        ui64 offset = headerSize;
        for (size_t i = 0; i != devices.size(); ++i) {
            const auto& d = devices[i];
            UNIT_ASSERT_VALUES_EQUAL_C(offset, d.GetOffset(), d);
            UNIT_ASSERT_VALUES_EQUAL_C(blockSize, d.GetBlockSize(), d);
            UNIT_ASSERT_VALUES_EQUAL_C(deviceSize, d.GetFileSize(), d);
            UNIT_ASSERT_VALUES_EQUAL_C(ids[i], d.GetDeviceId(), d);

            offset += padding + deviceSize;
        }
    }

    Y_UNIT_TEST_F(ShouldPropagateJournalConfig, TFixture)
    {
        NProto::TStorageDiscoveryConfig::TPoolConfig journalled;
        journalled.SetPoolName("journalled");
        journalled.MutableJournalConfig()->SetEnabled(true);
        journalled.MutableJournalConfig()->SetLogMetaSize(1_GB);
        journalled.MutableJournalConfig()->SetLogDataSize(10_GB);

        NProto::TStorageDiscoveryConfig::TPoolConfig journalledWithLayout;
        journalledWithLayout.SetPoolName("journalled");
        journalledWithLayout.MutableJournalConfig()->SetEnabled(true);

        {
            auto& layout = *journalledWithLayout.MutableLayout();
            layout.SetHeaderSize(1_GB);
            layout.SetDevicePadding(32_MB);
            layout.SetDeviceSize(93_GB);
        }

        NProto::TStorageDiscoveryConfig::TPoolConfig regular;
        regular.SetPoolName("regular");

        TDeviceGenerator gen { Log, AgentId };

        {
            gen(
                "/dev/disk/by-partlabel/NVMENBS01",
                MakePathConfig(journalled, 0),
                1,
                4_KB,
                93_GB);

            auto r = gen.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(1, r.size());
            UNIT_ASSERT_C(r[0].GetJournalConfig().GetEnabled(), r[0]);
            UNIT_ASSERT_VALUES_EQUAL_C(
                1_GB,
                r[0].GetJournalConfig().GetLogMetaSize(),
                r[0]);
            UNIT_ASSERT_VALUES_EQUAL_C(
                10_GB,
                r[0].GetJournalConfig().GetLogDataSize(),
                r[0]);
        }

        {
            gen(
                "/dev/disk/by-partlabel/NVMENBS02",
                MakePathConfig(journalledWithLayout),
                2,
                4_KB,
                1_GB + (93_GB + 32_MB) * 3);

            auto r = gen.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(3, r.size());
            for (const auto& d: r) {
                UNIT_ASSERT_C(d.GetJournalConfig().GetEnabled(), d);
                UNIT_ASSERT_VALUES_EQUAL_C(
                    0,
                    d.GetJournalConfig().GetLogMetaSize(),
                    d);
            }
        }

        {
            gen(
                "/dev/disk/by-partlabel/NVMENBS03",
                MakePathConfig(regular, 0),
                3,
                4_KB,
                93_GB);

            auto r = gen.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(1, r.size());
            UNIT_ASSERT_C(!r[0].GetJournalConfig().GetEnabled(), r[0]);
        }
    }

    Y_UNIT_TEST_F(ShouldSelectPoolBySize, TFixture)
    {
        NProto::TStorageDiscoveryConfig::TPathConfig nvme;

        auto& def = *nvme.AddPoolConfigs();
        def.SetMinSize(1_KB);
        def.SetMaxSize(1_KB + 1);

        auto& v3 = *nvme.AddPoolConfigs();
        v3.SetPoolName("v3");
        v3.SetMinSize(10_KB);
        v3.SetMaxSize(10_KB + 1);

        auto& unbounded = *nvme.AddPoolConfigs();
        unbounded.SetPoolName("unbounded");
        unbounded.SetMinSize(20_KB);
        // MaxSize is not specified

        TDeviceGenerator gen { Log, AgentId };

        auto error = gen(
            "/dev/disk/by-partlabel/NVMENBS01",
            nvme,
            1,
            4_KB,
            1_KB);
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        error = gen("/dev/disk/by-partlabel/NVMENBS02", nvme, 2, 4_KB, 10_KB);
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        error = gen("/dev/disk/by-partlabel/NVMENBS03", nvme, 3, 4_KB, 1_TB);
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        // 5Kb doesn't fit into any pool
        error = gen("/dev/disk/by-partlabel/NVMENBS04", nvme, 4, 4_KB, 5_KB);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_NOT_FOUND,
            error.GetCode(),
            error.GetMessage());

        auto r = gen.ExtractResult();
        UNIT_ASSERT_VALUES_EQUAL(3, r.size());

        UNIT_ASSERT_VALUES_EQUAL_C("", r[0].GetPoolName(), r[0]);
        UNIT_ASSERT_VALUES_EQUAL_C("v3", r[1].GetPoolName(), r[1]);
        UNIT_ASSERT_VALUES_EQUAL_C("unbounded", r[2].GetPoolName(), r[2]);
    }

    Y_UNIT_TEST_F(ShouldResolveBlockSize, TFixture)
    {
        NProto::TStorageDiscoveryConfig::TPoolConfig pool;
        pool.SetPoolName("pool");

        TDeviceGenerator gen { Log, AgentId };

        // Neither the pool nor the path specifies the block size: use the
        // block size of the file.
        {
            auto error = gen(
                "/dev/disk/by-partlabel/NVMENBS01",
                MakePathConfig(pool),
                1,
                4_KB,
                93_GB);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());

            auto r = gen.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(1, r.size());
            UNIT_ASSERT_VALUES_EQUAL_C(4_KB, r[0].GetBlockSize(), r[0]);
        }

        // The path block size overrides the file block size.
        {
            auto path = MakePathConfig(pool);
            path.SetBlockSize(512);

            auto error = gen(
                "/dev/disk/by-partlabel/NVMENBS02",
                path,
                2,
                4_KB,
                93_GB);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());

            auto r = gen.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(1, r.size());
            UNIT_ASSERT_VALUES_EQUAL_C(512, r[0].GetBlockSize(), r[0]);
        }

        // The pool block size overrides the path block size.
        {
            auto path = MakePathConfig(pool);
            path.SetBlockSize(512);
            path.MutablePoolConfigs(0)->SetBlockSize(16_KB);

            auto error = gen(
                "/dev/disk/by-partlabel/NVMENBS03",
                path,
                3,
                4_KB,
                93_GB);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());

            auto r = gen.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(1, r.size());
            UNIT_ASSERT_VALUES_EQUAL_C(16_KB, r[0].GetBlockSize(), r[0]);
        }
    }

    Y_UNIT_TEST_F(ShouldResolveMaxDeviceCount, TFixture)
    {
        NProto::TStorageDiscoveryConfig::TPoolConfig def;
        def.MutableLayout()->SetDeviceSize(1_KB);

        TDeviceGenerator gen { Log, AgentId };

        // Neither the pool nor the path limits the device count.
        {
            auto error = gen(
                "/dev/disk/by-partlabel/NVMENBS01",
                MakePathConfig(def),
                1,
                4_KB,
                100_KB);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(100, gen.ExtractResult().size());
        }

        // The path limits the device count.
        {
            auto error = gen(
                "/dev/disk/by-partlabel/NVMENBS02",
                MakePathConfig(def, 42),
                2,
                4_KB,
                100_KB);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(42, gen.ExtractResult().size());
        }

        // The pool limit overrides the path limit.
        {
            auto path = MakePathConfig(def, 42);
            path.MutablePoolConfigs(0)->SetMaxDeviceCount(10);

            auto error = gen(
                "/dev/disk/by-partlabel/NVMENBS03",
                path,
                3,
                4_KB,
                100_KB);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                error.GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(10, gen.ExtractResult().size());
        }
    }

    Y_UNIT_TEST_F(ShouldInferMinSizeFromLayout, TFixture)
    {
        NProto::TStorageDiscoveryConfig::TPoolConfig def;
        auto& layout = *def.MutableLayout();
        // MinSize & MaxSize are not specified
        layout.SetHeaderSize(8_KB);
        layout.SetDeviceSize(2_KB);
        layout.SetDevicePadding(1_KB);

        TDeviceGenerator gen { Log, AgentId };

        // NVMENBS01 is accepted because of implicit MinSize = 10Kb and
        // the unlimited MaxSize.
        auto error = gen(
            "/dev/disk/by-partlabel/NVMENBS01",
            MakePathConfig(def),
            1,
            4_KB,
            100_KB);
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error.GetMessage());

        auto r = gen.ExtractResult();
        UNIT_ASSERT_VALUES_EQUAL(31, r.size());  // (100 - 8) / (2 + 1)
        for (const auto& d: r) {
            UNIT_ASSERT_VALUES_EQUAL_C("", d.GetPoolName(), d);
            UNIT_ASSERT_VALUES_EQUAL_C(2_KB, d.GetFileSize(), d);
        }

        // NVMENBS02 is rejected because of implicit MinSize = 10Kb
        error = gen(
            "/dev/disk/by-partlabel/NVMENBS02",
            MakePathConfig(def),
            2,
            4_KB,
            9_KB);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_NOT_FOUND,
            error.GetCode(),
            error.GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(0, gen.ExtractResult().size());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
