#include "ring_buffer.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRingBufferTest)
{
    Y_UNIT_TEST(TestZeroSizeBuffer)
    {
        TRingBuffer<ui32> buffer(0);

        UNIT_ASSERT_VALUES_EQUAL(0, buffer.Size());

        UNIT_ASSERT(!buffer.Ready());
        UNIT_ASSERT(!buffer.LastTs().GetValue());
        UNIT_ASSERT_VALUES_EQUAL(0, buffer.Get(123).Value);

        buffer.Register({TInstant::Seconds(1), 1});
        UNIT_ASSERT(!buffer.Ready());
        UNIT_ASSERT(!buffer.LastTs().GetValue());
        UNIT_ASSERT_VALUES_EQUAL(0, buffer.Get(123).Value);
    }

    Y_UNIT_TEST(TestNonZeroSizeBuffer)
    {
        TRingBuffer<ui32> buffer(3);

        UNIT_ASSERT_VALUES_EQUAL(3, buffer.Size());

        UNIT_ASSERT(!buffer.Ready());
        UNIT_ASSERT(!buffer.LastTs().GetValue());
        UNIT_ASSERT_VALUES_EQUAL(0, buffer.Get(0).Value);

        buffer.Register({TInstant::Seconds(1), 1});
        UNIT_ASSERT(!buffer.Ready());
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(1), buffer.LastTs());
        UNIT_ASSERT_VALUES_EQUAL(1, buffer.Get(0).Value);

        buffer.Register({TInstant::Seconds(2), 2});
        UNIT_ASSERT(!buffer.Ready());
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(2), buffer.LastTs());
        UNIT_ASSERT_VALUES_EQUAL(2, buffer.Get(0).Value);
        UNIT_ASSERT_VALUES_EQUAL(1, buffer.Get(1).Value);

        buffer.Register({TInstant::Seconds(3), 3});
        UNIT_ASSERT(buffer.Ready());
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(3), buffer.LastTs());
        UNIT_ASSERT_VALUES_EQUAL(3, buffer.Get(0).Value);
        UNIT_ASSERT_VALUES_EQUAL(2, buffer.Get(1).Value);
        UNIT_ASSERT_VALUES_EQUAL(1, buffer.Get(2).Value);

        buffer.Register({TInstant::Seconds(4), 4});
        UNIT_ASSERT(buffer.Ready());
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(4), buffer.LastTs());
        UNIT_ASSERT_VALUES_EQUAL(4, buffer.Get(0).Value);
        UNIT_ASSERT_VALUES_EQUAL(3, buffer.Get(1).Value);
        UNIT_ASSERT_VALUES_EQUAL(2, buffer.Get(2).Value);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
