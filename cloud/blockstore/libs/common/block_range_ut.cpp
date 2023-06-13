#include "block_range.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
IOutputStream& operator<<(IOutputStream& out, const TBlockRange<T>& rhs)
{
    out << "{" << rhs.Start << "," << rhs.End << "}";
    return out;
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockRangeTest)
{
    Y_UNIT_TEST(Difference)
    {
        { // cut left
            auto result = TBlockRange<ui64>{10, 20}.Difference({0, 15});
            auto expect = TBlockRange<ui64>{16, 20};
            UNIT_ASSERT_VALUES_EQUAL(expect, *result.First);
            UNIT_ASSERT(result.Second == std::nullopt);
        }
        { // cut left
            auto result = TBlockRange<ui64>{10, 20}.Difference({10, 15});
            auto expect = TBlockRange<ui64>{16, 20};
            UNIT_ASSERT_VALUES_EQUAL(expect, *result.First);
            UNIT_ASSERT(result.Second == std::nullopt);
        }
        { // cut right
            auto result = TBlockRange<ui64>{10, 20}.Difference({16, 25});
            auto expect = TBlockRange<ui64>{10, 15};
            UNIT_ASSERT_VALUES_EQUAL(expect, *result.First);
            UNIT_ASSERT(result.Second == std::nullopt);
        }
        { // cut right
            auto result = TBlockRange<ui64>{10, 20}.Difference({16, 20});
            auto expect = TBlockRange<ui64>{10, 15};
            UNIT_ASSERT_VALUES_EQUAL(expect, *result.First);
            UNIT_ASSERT(result.Second == std::nullopt);
        }
        { // cut from middle
            auto result = TBlockRange<ui64>{10, 20}.Difference({16, 18});
            auto expectFirst = TBlockRange<ui64>{10, 15};
            auto expectSecond = TBlockRange<ui64>{19, 20};
            UNIT_ASSERT_VALUES_EQUAL(expectFirst, *result.First);
            UNIT_ASSERT_VALUES_EQUAL(expectSecond, *result.Second);
        }
    }
}

} // namespace NCloud::NBlockStore
