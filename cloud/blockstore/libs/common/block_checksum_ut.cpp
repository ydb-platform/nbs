#include "block_checksum.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

const ui32 BlockSize = 4096;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockChecksumTest)
{
    Y_UNIT_TEST(TestMultipleExtends)
    {
        char data[BlockSize] = {42};

        TBlockChecksum bc1;
        ui32 sum1 = bc1.Extend(data, sizeof(data));

        TBlockChecksum bc2;
        bc2.Extend(data, sizeof(data));
        ui32 sum2 = bc2.Extend(data, sizeof(data));

        UNIT_ASSERT_VALUES_UNEQUAL(sum1, sum2);
    }

    Y_UNIT_TEST(TestReverseOrderExtends)
    {
        char data1[BlockSize] = {42};
        char data2[BlockSize] = {43};

        TBlockChecksum bc1;
        bc1.Extend(data1, sizeof(data1));
        ui32 sum1 = bc1.Extend(data2, sizeof(data2));

        TBlockChecksum bc2;
        bc2.Extend(data2, sizeof(data2));
        ui32 sum2 = bc2.Extend(data1, sizeof(data1));

        UNIT_ASSERT_VALUES_UNEQUAL(sum1, sum2);
    }

    Y_UNIT_TEST(TestZeroBlock)
    {
        char data[BlockSize] = {0};

        TBlockChecksum bc;
        ui32 sum = bc.Extend(data, sizeof(data));

        UNIT_ASSERT_VALUES_UNEQUAL(0, sum);
    }

    Y_UNIT_TEST(TestMultipleZeroBlocks)
    {
        char data[BlockSize] = {0};

        TBlockChecksum bc1;
        ui32 sum1 = bc1.Extend(data, sizeof(data));

        TBlockChecksum bc2;
        bc2.Extend(data, sizeof(data));
        ui32 sum2 = bc2.Extend(data, sizeof(data));

        UNIT_ASSERT_VALUES_UNEQUAL(sum1, sum2);
    }

    Y_UNIT_TEST(TestCombineValues)
    {
        char data1[BlockSize] = {42};
        char data2[BlockSize] = {43};

        TBlockChecksum bc1;
        bc1.Extend(data1, sizeof(data1));
        ui32 sum1 = bc1.Extend(data2, sizeof(data2));

        TBlockChecksum bc2;
        ui32 sum2 = bc2.Extend(data2, sizeof(data2));

        TBlockChecksum bc3;
        bc3.Extend(data1, sizeof(data1));
        ui32 sum3 = bc3.Combine(sum2, sizeof(data2));

        UNIT_ASSERT_VALUES_EQUAL(sum1, sum3);
    }
}

}   // namespace NCloud::NBlockStore
