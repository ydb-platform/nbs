#include "device_generator.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

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
            gen("/dev/disk/by-partlabel/NVMENBS01", def, 1, 63, 4_KB, 6401251344384);

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
            gen("/dev/disk/by-partlabel/ROTNBS01", rot, 1, 140, 4_KB, 16000898564096);

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
            gen("/dev/disk/by-partlabel/ROTNBS02", rot, 2, 140, 4_KB, 15999825870848);

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
            gen("/dev/disk/by-partlabel/NVMECOMPUTE01", local, 1, 1, local.GetBlockSize(), 367_GB);

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

            gen("/dev/disk/by-partlabel/NVMENBS01", def, 1, 42, 4_KB, 93_GB);
            gen("/dev/disk/by-partlabel/NVMENBS02", def, 2, 0, 4_KB, 93_GB);

            auto r = gen.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(2, r.size());
            expectedId = r[1].GetDeviceId();
        }

        {
            TDeviceGenerator gen { Log, AgentId };

            gen("/dev/disk/by-partlabel/NVMENBS02", def, 2, 0, 4_KB, 93_GB);
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
            compound,
            1,      // device number
            0,      // max device count
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
}

}   // namespace NCloud::NBlockStore::NStorage
