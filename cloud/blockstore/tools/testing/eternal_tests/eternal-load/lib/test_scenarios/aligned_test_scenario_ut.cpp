#include "aligned_test_scenario.h"

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config.h>

#include <library/cpp/digest/crc32c/crc32c.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

#include <algorithm>
#include <cstddef>

namespace NCloud::NBlockStore::NTesting {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 RequestBlockCount = 4;
constexpr ui64 RequestCount = 8;

struct TTestDevice final: public ITestExecutorIOService
{
    TVector<char> Data;
    TVector<TString> Errors;

    explicit TTestDevice(ui64 size)
        : Data(size)
    {}

    void
    Read(void* buffer, ui32 count, ui64 offset, TCallback callback) override
    {
        UNIT_ASSERT(offset + count <= Data.size());
        memcpy(buffer, Data.data() + offset, count);
        callback();
    }

    void Write(const void* buffer, ui32 count, ui64 offset, TCallback callback)
        override
    {
        UNIT_ASSERT(offset + count <= Data.size());
        memcpy(Data.data() + offset, buffer, count);
        callback();
    }

    void Zero(ui32 count, ui64 offset, TCallback callback) override
    {
        UNIT_ASSERT(offset + count <= Data.size());
        memset(Data.data() + offset, 0, count);
        callback();
    }

    void Stop() override
    {}

    void Fail(const TString& message) override
    {
        Errors.push_back(message);
    }
};

struct TTestEnv
{
    const ui64 BlockSize;
    const ui64 SlotSize;
    IConfigHolderPtr ConfigHolder;
    TTestDevice Device;

    TTestEnv(ui64 blockSize, ui64 writeParts)
        : BlockSize(blockSize)
        , SlotSize(RequestBlockCount * blockSize)
        , ConfigHolder(CreateTestConfig(
              {.FilePath = "test",
               .FileSize = RequestCount * RequestBlockCount * blockSize,
               .IoDepth = 1,
               .BlockSize = blockSize,
               .WriteRate = 100,
               .RequestBlockCount = RequestBlockCount,
               .WriteParts = writeParts}))
        , Device(RequestCount * SlotSize)
    {}

    void WriteSlots(ui32 count)
    {
        ConfigHolder->GetConfig().SetWriteRate(100);
        Run(count);
    }

    void WriteAllSlots()
    {
        WriteSlots(RequestCount);
    }

    void ReadSlots(ui32 count)
    {
        ConfigHolder->GetConfig().SetWriteRate(0);
        Run(count);
    }

    char* Sector(ui64 slot, ui64 sector)
    {
        return Device.Data.data() + slot * SlotSize +
               sector * TSectorData::Size;
    }

private:
    void Run(ui32 count)
    {
        auto scenario = CreateAlignedTestScenario(ConfigHolder, "test", TLog());
        for (ui32 i = 0; i < count; ++i) {
            scenario->GetWorker(0).Run(0, Device);
        }
    }
};

ui32 SectorChecksum(const char* sector)
{
    TSectorData copy;
    memcpy(&copy, sector, sizeof(copy));
    copy.Header.Checksum = 0;
    return Crc32c(&copy, sizeof(copy));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TAlignedTestScenarioTest)
{
    Y_UNIT_TEST(ShouldWriteChecksummedSectors)
    {
        const std::pair<ui64, ui64> blockSizeAndWriteParts[] = {
            {4_KB, 1},
            {4_KB, 8},
            {32_KB, 8},
        };

        for (auto [blockSize, writeParts]: blockSizeAndWriteParts) {
            TTestEnv env(blockSize, writeParts);
            env.WriteAllSlots();

            const ui64 testId = env.ConfigHolder->GetConfig().GetTestId();
            const ui64 sectorsPerSlot = env.SlotSize / TSectorData::Size;
            const ui64 partSize = env.SlotSize / writeParts;
            for (ui64 slot = 0; slot < RequestCount; ++slot) {
                for (ui64 i = 0; i < sectorsPerSlot; ++i) {
                    TSectorData sector;
                    memcpy(&sector, env.Sector(slot, i), sizeof(sector));

                    UNIT_ASSERT_VALUES_EQUAL(
                        sector.Header.Checksum,
                        static_cast<ui64>(SectorChecksum(env.Sector(slot, i))));
                    UNIT_ASSERT_VALUES_EQUAL(sector.Header.TestId, testId);
                    UNIT_ASSERT_VALUES_EQUAL(
                        sector.Header.BlockIndex,
                        slot * RequestBlockCount);
                    UNIT_ASSERT_VALUES_EQUAL(
                        sector.Header.PartNumber,
                        i * TSectorData::Size / partSize);
                    UNIT_ASSERT_VALUES_EQUAL(
                        sector.SectorIndex,
                        slot * sectorsPerSlot + i);
                }
            }

            env.ReadSlots(8 * RequestCount);
            UNIT_ASSERT_C(env.Device.Errors.empty(), env.Device.Errors[0]);
        }
    }

    Y_UNIT_TEST(ShouldDetectCorruptedByteInsideRequest)
    {
        TTestEnv env(4_KB, 1);
        env.WriteAllSlots();

        for (ui64 slot = 0; slot < RequestCount; ++slot) {
            env.Sector(slot, 5)[offsetof(TSectorData, Payload) + 7] ^= 1;
        }

        env.ReadSlots(1);
        UNIT_ASSERT_VALUES_EQUAL(1u, env.Device.Errors.size());
        const auto& error = env.Device.Errors[0];
        UNIT_ASSERT_STRING_CONTAINS(error, "1 of 32 sectors are bad");
        UNIT_ASSERT_STRING_CONTAINS(error, "first bad sector at offset 2560");
        UNIT_ASSERT_STRING_CONTAINS(error, "checksum mismatch");
    }

    Y_UNIT_TEST(ShouldDetectStaleSector)
    {
        TTestEnv env(4_KB, 1);
        env.WriteAllSlots();
        const auto firstWrites = env.Device.Data;
        env.WriteAllSlots();

        // A sector that missed the second write keeps a valid checksum.
        const ui64 staleOffset = 7 * TSectorData::Size;
        for (ui64 slot = 0; slot < RequestCount; ++slot) {
            const ui64 offset = slot * env.SlotSize + staleOffset;
            memcpy(
                env.Device.Data.data() + offset,
                firstWrites.data() + offset,
                TSectorData::Size);
        }

        env.ReadSlots(1);
        UNIT_ASSERT_VALUES_EQUAL(1u, env.Device.Errors.size());
        UNIT_ASSERT_STRING_CONTAINS(
            env.Device.Errors[0],
            "first bad sector at offset 3584 (wrong RequestNumber)");
    }

    Y_UNIT_TEST(ShouldDetectMisplacedSector)
    {
        TTestEnv env(4_KB, 1);
        env.WriteAllSlots();

        for (ui64 slot = 0; slot < RequestCount; ++slot) {
            std::swap_ranges(
                env.Sector(slot, 1),
                env.Sector(slot, 2),
                env.Sector(slot, 2));
        }

        env.ReadSlots(1);
        UNIT_ASSERT_VALUES_EQUAL(1u, env.Device.Errors.size());
        const auto& error = env.Device.Errors[0];
        UNIT_ASSERT_STRING_CONTAINS(error, "2 of 32 sectors are bad");
        UNIT_ASSERT_STRING_CONTAINS(
            error,
            "first bad sector at offset 512 (wrong location)");
    }
}

}   // namespace NCloud::NBlockStore::NTesting
