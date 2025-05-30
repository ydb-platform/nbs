#include "log_title.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLogTitleTest)
{
    Y_UNIT_TEST(GetPartitionPrefixTest)
    {
        ui64 tabletId = 12345;

        {
            ui32 partitionIndex = 1;
            ui32 partitionCount = 1;
            auto result = TLogTitle::GetPartitionPrefix(
                tabletId,
                partitionIndex,
                partitionCount);
            UNIT_ASSERT_STRINGS_EQUAL(result, "p:12345");
        }

        {
            ui32 partitionIndex = 0;
            ui32 partitionCount = 2;
            auto result = TLogTitle::GetPartitionPrefix(
                tabletId,
                partitionIndex,
                partitionCount);
            UNIT_ASSERT_STRINGS_EQUAL("p0:12345", result);
        }

        {
            ui32 partitionIndex = 1;
            ui32 partitionCount = 2;
            auto result = TLogTitle::GetPartitionPrefix(
                tabletId,
                partitionIndex,
                partitionCount);
            UNIT_ASSERT_STRINGS_EQUAL("p1:12345", result);
        }
    }

    Y_UNIT_TEST(GetForVolume)
    {
        TLogTitle logTitle1(12345, "", GetCycleCount());

        UNIT_ASSERT_STRINGS_EQUAL(
            "[v:12345 g:? d:???]",
            logTitle1.Get(TLogTitle::EDetails::Brief));

        logTitle1.SetDiskId("disk1");
        UNIT_ASSERT_STRINGS_EQUAL(
            "[v:12345 g:? d:disk1]",
            logTitle1.Get(TLogTitle::EDetails::Brief));

        logTitle1.SetGeneration(5);
        UNIT_ASSERT_STRINGS_EQUAL(
            "[v:12345 g:5 d:disk1]",
            logTitle1.Get(TLogTitle::EDetails::Brief));

        UNIT_ASSERT_STRING_CONTAINS(
            logTitle1.Get(TLogTitle::EDetails::WithTime),
            "[v:12345 g:5 d:disk1 t:");
    }

    Y_UNIT_TEST(GetForPartition)
    {
        TLogTitle logTitle1(12345, "disk1", GetCycleCount(), 1, 2);

        UNIT_ASSERT_STRINGS_EQUAL(
            "[p1:12345 g:? d:disk1]",
            logTitle1.Get(TLogTitle::EDetails::Brief));

        logTitle1.SetGeneration(5);
        UNIT_ASSERT_STRINGS_EQUAL(
            "[p1:12345 g:5 d:disk1]",
            logTitle1.Get(TLogTitle::EDetails::Brief));

        UNIT_ASSERT_STRING_CONTAINS(
            logTitle1.Get(TLogTitle::EDetails::WithTime),
            "[p1:12345 g:5 d:disk1 t:");
    }
}

}   // namespace NCloud::NBlockStore::NStorage
