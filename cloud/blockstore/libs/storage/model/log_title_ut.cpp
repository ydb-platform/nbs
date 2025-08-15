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
            logTitle1.GetWithTime(),
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
            logTitle1.GetWithTime(),
            "[p1:12345 g:5 d:disk1 t:");
    }

    Y_UNIT_TEST(GetForSession)
    {
        TLogTitle logTitle(
            TLogTitle::EType::Session,
            "session-1",
            "disk1",
            false,
            GetCycleCount());

        UNIT_ASSERT_STRINGS_EQUAL(
            "[vs:? d:disk1 s:session-1]",
            logTitle.Get(TLogTitle::EDetails::Brief));

        logTitle.SetTabletId(12345);
        UNIT_ASSERT_STRINGS_EQUAL(
            "[vs:12345 d:disk1 s:session-1]",
            logTitle.Get(TLogTitle::EDetails::Brief));

        UNIT_ASSERT_STRING_CONTAINS(
            logTitle.GetWithTime(),
            "[vs:12345 d:disk1 s:session-1 t:");
    }

    Y_UNIT_TEST(GetForSessionOnTemporaryServer)
    {
        TLogTitle logTitle(
            TLogTitle::EType::Session,
            "session-1",
            "disk1",
            true,
            GetCycleCount());

        UNIT_ASSERT_STRINGS_EQUAL(
            "[~vs:? d:disk1 s:session-1]",
            logTitle.Get(TLogTitle::EDetails::Brief));

        logTitle.SetTabletId(12345);
        UNIT_ASSERT_STRINGS_EQUAL(
            "[~vs:12345 d:disk1 s:session-1]",
            logTitle.Get(TLogTitle::EDetails::Brief));

        UNIT_ASSERT_STRING_CONTAINS(
            logTitle.GetWithTime(),
            "[~vs:12345 d:disk1 s:session-1 t:");
    }

    Y_UNIT_TEST(GetForClient)
    {
        TLogTitle logTitle(
            12345,
            "session-1",
            "client-1",
            "disk1",
            false,
            GetCycleCount());

        UNIT_ASSERT_STRINGS_EQUAL(
            "[vc:12345 d:disk1 s:session-1 c:client-1 pg:?]",
            logTitle.Get(TLogTitle::EDetails::Brief));

        logTitle.SetGeneration(1);
        UNIT_ASSERT_STRINGS_EQUAL(
            "[vc:12345 d:disk1 s:session-1 c:client-1 pg:1]",
            logTitle.Get(TLogTitle::EDetails::Brief));

        UNIT_ASSERT_STRING_CONTAINS(
            logTitle.GetWithTime(),
            "[vc:12345 d:disk1 s:session-1 c:client-1 pg:1 t:");

        TLogTitle temporayLogTitle(
            12345,
            "session-1",
            "client-1",
            "disk1",
            true,
            GetCycleCount());
        UNIT_ASSERT_STRINGS_EQUAL(
            "[~vc:12345 d:disk1 s:session-1 c:client-1 pg:?]",
            temporayLogTitle.Get(TLogTitle::EDetails::Brief));
    }

    Y_UNIT_TEST(GetForVolumeProxy)
    {
        TLogTitle logTitle("disk1", false, GetCycleCount());

        UNIT_ASSERT_STRINGS_EQUAL(
            "[vp:? d:disk1 pg:0]",
            logTitle.Get(TLogTitle::EDetails::Brief));

        logTitle.SetTabletId(12345);
        UNIT_ASSERT_STRINGS_EQUAL(
            "[vp:12345 d:disk1 pg:0]",
            logTitle.Get(TLogTitle::EDetails::Brief));

        logTitle.SetGeneration(5);
        UNIT_ASSERT_STRINGS_EQUAL(
            "[vp:12345 d:disk1 pg:5]",
            logTitle.Get(TLogTitle::EDetails::Brief));

        TLogTitle temporayLogTitle("disk1", true, GetCycleCount());
        UNIT_ASSERT_STRINGS_EQUAL(
            "[~vp:? d:disk1 pg:0]",
            temporayLogTitle.Get(TLogTitle::EDetails::Brief));
    }

    Y_UNIT_TEST(GetForPartitionNonrepl)
    {
        TLogTitle logTitle =
            TLogTitle::MakeForPartitionNonrepl("disk1", GetCycleCount());

        UNIT_ASSERT_STRINGS_EQUAL(
            "[nrd:disk1]",
            logTitle.Get(TLogTitle::EDetails::Brief));

        UNIT_ASSERT_STRING_CONTAINS(logTitle.GetWithTime(), "[nrd:disk1 t:");
    }

    Y_UNIT_TEST(GetChildLogger)
    {
        const ui64 startTime =
            GetCycleCount() - GetCyclesPerMillisecond() * 2001;
        TLogTitle logTitle1(12345, "disk1", startTime);
        logTitle1.SetGeneration(5);

        auto childLogTitle =
            logTitle1.GetChild(startTime + GetCyclesPerMillisecond() * 1001);

        UNIT_ASSERT_STRING_CONTAINS(
            childLogTitle.GetWithTime(),
            "[v:12345 g:5 d:disk1 t:1.001s + 1.");
    }

    Y_UNIT_TEST(GetChildWithTagsLogger)
    {
        const ui64 startTime =
            GetCycleCount() - GetCyclesPerMillisecond() * 2001;
        TLogTitle logTitle1(12345, "disk1", startTime);
        logTitle1.SetGeneration(5);

        std::vector<std::pair<TString, TString>> tags = {{"cp", "123"}};

        auto childLogTitle = logTitle1.GetChildWithTags(
            startTime + GetCyclesPerMillisecond() * 1001,
            tags);

        UNIT_ASSERT_STRING_CONTAINS(
            childLogTitle.GetWithTime(),
            "[v:12345 g:5 d:disk1 cp:123 t:1.001s + 1.");
    }
}

}   // namespace NCloud::NBlockStore::NStorage
