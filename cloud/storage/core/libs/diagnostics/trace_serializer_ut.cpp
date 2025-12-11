#include "trace_serializer.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTraceSerializerTest)
{
    Y_UNIT_TEST(ShouldAdjustRemoteInterval)
    {
        {
            TTraceInterval local{100, 200};
            TTraceInterval remote{150, 250};

            auto res = AdjustRemoteInterval(local, remote);
            UNIT_ASSERT_VALUES_EQUAL(res.Shift, -50);
            UNIT_ASSERT_VALUES_EQUAL(res.Scale, 1);
        };

        {
            TTraceInterval local{100, 200};
            TTraceInterval remote{250, 350};

            auto res = AdjustRemoteInterval(local, remote);
            UNIT_ASSERT_VALUES_EQUAL(res.Shift, -150);
            UNIT_ASSERT_VALUES_EQUAL(res.Scale, 1);
        };

        {
            TTraceInterval local{100, 200};
            TTraceInterval remote{50, 150};

            auto res = AdjustRemoteInterval(local, remote);
            UNIT_ASSERT_VALUES_EQUAL(res.Shift, 50);
            UNIT_ASSERT_VALUES_EQUAL(res.Scale, 1);
        };

        {
            TTraceInterval local{100, 200};
            TTraceInterval remote{50, 250};

            auto res = AdjustRemoteInterval(local, remote);
            UNIT_ASSERT_VALUES_EQUAL(res.Shift, 50);
            UNIT_ASSERT_VALUES_EQUAL(res.Scale, 0.5);
        };

        {
            TTraceInterval local{100, 200};
            TTraceInterval remote{150, 190};

            auto res = AdjustRemoteInterval(local, remote);
            UNIT_ASSERT_VALUES_EQUAL(res.Shift, -20);
            UNIT_ASSERT_VALUES_EQUAL(res.Scale, 1);
        };
    }
}

}   // namespace NCloud
