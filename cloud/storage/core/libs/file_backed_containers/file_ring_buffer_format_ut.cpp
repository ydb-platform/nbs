#include "file_ring_buffer_format.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

TVector<EFileRingBufferVersion> GetVersionsForTest()
{
    return {
        EFileRingBufferVersion::V5,
        EFileRingBufferVersion::V6};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFileRingBufferFormatTest)
{
    Y_UNIT_TEST(ShouldCreateLayoutFromCapacities)
    {
        struct TTestCase
        {
            ui64 DataCapacity;
            ui64 MetadataCapacity;
            TFileRingBufferLayout Expected;
        };

        const ui64 reserveSize = TFileRingBufferHeader::ReserveSize;
        const ui64 maxSize = Max<i64>();
        const TVector<TTestCase> testCases = {
            {0, 0, {reserveSize, reserveSize, reserveSize}},
            {17, 1, {reserveSize + 8, reserveSize, reserveSize + 25}},
            {17, 8, {reserveSize + 8, reserveSize, reserveSize + 25}},
            {17, 9, {reserveSize + 16, reserveSize, reserveSize + 33}},
            {maxSize - reserveSize, 0, {reserveSize, reserveSize, maxSize}},
            {7, maxSize - reserveSize - 7, {maxSize - 7, reserveSize, maxSize}},
            {0, maxSize - reserveSize - 7, {maxSize - 7, reserveSize, maxSize - 7}},
            {maxSize - reserveSize + 1, 0, {}},
            {8, maxSize - reserveSize - 7, {}},
            {0, maxSize - reserveSize - 6, {}},
            {maxSize, 0, {}},
            {0, maxSize, {}},
            {0, maxSize - reserveSize, {}},
            {Max<ui64>() - reserveSize, 0, {}},
            {7, Max<ui64>() - reserveSize - 7, {}},
            {Max<ui64>(), 0, {}},
            {0, Max<ui64>(), {}},
        };

        for (const auto& testCase: testCases) {
            const auto layout = TFileRingBufferLayout::FromCapacity(
                testCase.DataCapacity,
                testCase.MetadataCapacity);
            UNIT_ASSERT_VALUES_EQUAL(
                testCase.Expected.DataOffset,
                layout.DataOffset);
            UNIT_ASSERT_VALUES_EQUAL(
                testCase.Expected.MetadataOffset,
                layout.MetadataOffset);
            UNIT_ASSERT_VALUES_EQUAL(
                testCase.Expected.StateFileSize,
                layout.StateFileSize);
        }
    }

    Y_UNIT_TEST(ShouldStoreAndReadHeaders)
    {
        for (const auto& version : GetVersionsForTest()) {
            TVector<char> data(1024);
            auto processor = CreateFileRingBufferDataProcessor(version, data);

            for (ui32 i = 0; i < 16; i++) {
                auto written = processor->WriteEntryHeader(i * sizeof(ui64), {
                    .DataSize = i,
                    .DataChecksum = i * 13,
                    .FreeFlag = (i % 2 == 0),
                });

                UNIT_ASSERT(written);
            }

            for (ui32 i = 0; i < 16; i++) {
                auto header = processor->ReadEntryHeader(i * sizeof(ui64));
                UNIT_ASSERT_VALUES_EQUAL(i, header.DataSize);
                UNIT_ASSERT_VALUES_EQUAL(i * 13, header.DataChecksum);
                UNIT_ASSERT_VALUES_EQUAL((i % 2 == 0), header.FreeFlag);
            }
        }
    }

    Y_UNIT_TEST(ShouldRespectCapabilities)
    {
        for (const auto& version : GetVersionsForTest()) {
            TVector<char> data(1024);
            auto processor = CreateFileRingBufferDataProcessor(version, data);
            auto capabilities = processor->GetCapabilities(false);

            // MaxAllocationByteCount
            if (capabilities.MaxAllocationByteCount < Max<ui32>()) {
                auto goodHeader = TFileRingBufferEntryHeader{
                    .DataSize =
                        static_cast<ui32>(capabilities.MaxAllocationByteCount),
                    .DataChecksum = 7,
                    .FreeFlag = true,
                };

                auto badHeader = TFileRingBufferEntryHeader{
                    .DataSize = static_cast<ui32>(
                        capabilities.MaxAllocationByteCount + 1),
                    .DataChecksum = 7,
                    .FreeFlag = true,
                };

                UNIT_ASSERT(processor->WriteEntryHeader(0, goodHeader));
                UNIT_ASSERT(!processor->WriteEntryHeader(0, badHeader));

                auto header = processor->ReadEntryHeader(0);
                UNIT_ASSERT_VALUES_EQUAL(goodHeader.DataSize, header.DataSize);
                UNIT_ASSERT_VALUES_EQUAL(
                    goodHeader.DataChecksum,
                    header.DataChecksum);
                UNIT_ASSERT_VALUES_EQUAL(goodHeader.Tag, header.Tag);
                UNIT_ASSERT_VALUES_EQUAL(goodHeader.FreeFlag, header.FreeFlag);
            }

            // MaxTag
            if (capabilities.MaxTag < Max<ui32>()) {
                auto goodHeader = TFileRingBufferEntryHeader{
                    .DataSize = 11,
                    .DataChecksum = 7,
                    .Tag = static_cast<ui32>(capabilities.MaxTag),
                    .FreeFlag = true,
                };

                auto badHeader = TFileRingBufferEntryHeader{
                    .DataSize = 11,
                    .DataChecksum = 7,
                    .Tag = static_cast<ui32>(capabilities.MaxTag + 1),
                    .FreeFlag = true,
                };

                UNIT_ASSERT(processor->WriteEntryHeader(0, goodHeader));
                UNIT_ASSERT(!processor->WriteEntryHeader(0, badHeader));

                auto header = processor->ReadEntryHeader(0);
                UNIT_ASSERT_VALUES_EQUAL(goodHeader.DataSize, header.DataSize);
                UNIT_ASSERT_VALUES_EQUAL(
                    goodHeader.DataChecksum,
                    header.DataChecksum);
                UNIT_ASSERT_VALUES_EQUAL(goodHeader.Tag, header.Tag);
                UNIT_ASSERT_VALUES_EQUAL(goodHeader.FreeFlag, header.FreeFlag);
            }

            // Alignment
            if (capabilities.Alignment > 1) {
                auto header = TFileRingBufferEntryHeader{
                    .DataSize = 11,
                    .DataChecksum = 7,
                    .Tag = 0,
                    .FreeFlag = true,
                };

                UNIT_ASSERT(processor->WriteEntryHeader(
                    capabilities.Alignment,
                    header));

                UNIT_ASSERT(!processor->WriteEntryHeader(
                    capabilities.Alignment + 1,
                    header));

                auto badHeader =
                    processor->ReadEntryHeader(capabilities.Alignment + 1);

                UNIT_ASSERT_VALUES_EQUAL(0, badHeader.DataSize);

                auto goodHeader =
                    processor->ReadEntryHeader(capabilities.Alignment);

                UNIT_ASSERT_VALUES_EQUAL(goodHeader.DataSize, header.DataSize);
                UNIT_ASSERT_VALUES_EQUAL(
                    goodHeader.DataChecksum,
                    header.DataChecksum);
                UNIT_ASSERT_VALUES_EQUAL(goodHeader.Tag, header.Tag);
                UNIT_ASSERT_VALUES_EQUAL(goodHeader.FreeFlag, header.FreeFlag);
            }
        }
    }
}

}   // namespace NCloud
