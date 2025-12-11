#include "composite_id.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCompositeIdTest)
{
    Y_UNIT_TEST(Basic)
    {
        TCompositeId id = TCompositeId::FromGeneration(100);
        UNIT_ASSERT_VALUES_EQUAL(100, id.GetGeneration());
        UNIT_ASSERT_VALUES_EQUAL(0, id.GetRequestId());

        id.Advance();
        UNIT_ASSERT_VALUES_EQUAL(100, id.GetGeneration());
        UNIT_ASSERT_VALUES_EQUAL(1, id.GetRequestId());

        id.Advance();
        UNIT_ASSERT_VALUES_EQUAL(100, id.GetGeneration());
        UNIT_ASSERT_VALUES_EQUAL(2, id.GetRequestId());
    }

    Y_UNIT_TEST(Parser)
    {
        TCompositeId id = TCompositeId::FromGeneration(100);
        id.Advance();
        UNIT_ASSERT_VALUES_EQUAL(100, id.GetGeneration());
        UNIT_ASSERT_VALUES_EQUAL(1, id.GetRequestId());

        TCompositeId parsedId = TCompositeId::FromRaw(id.GetValue());
        UNIT_ASSERT_VALUES_EQUAL(id.GetGeneration(), parsedId.GetGeneration());
        UNIT_ASSERT_VALUES_EQUAL(id.GetRequestId(), parsedId.GetRequestId());
    }

    Y_UNIT_TEST(CanAdvance)
    {
        TCompositeId id = TCompositeId::FromGeneration(0);
        UNIT_ASSERT(id.CanAdvance());
        UNIT_ASSERT(id.Advance());
        UNIT_ASSERT(id.CanAdvance());

        TCompositeId id2 =
            TCompositeId::FromRaw(std::numeric_limits<ui32>::max());
        UNIT_ASSERT(!id2.CanAdvance());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
