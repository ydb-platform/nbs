#include "stripe.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define ASSERT_STRIPE_INFO_EQUALS(expected, actual)                            \
    UNIT_ASSERT_VALUES_EQUAL(expected.PartitionId, actual.PartitionId);        \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        expected.BlockRange.Start,                                             \
        actual.BlockRange.Start                                                \
    );                                                                         \
    UNIT_ASSERT_VALUES_EQUAL(                                                  \
        expected.BlockRange.End,                                               \
        actual.BlockRange.End                                                  \
    );                                                                         \
// ASSERT_STRIPE_INFO_EQUALS


Y_UNIT_TEST_SUITE(TStripeTest)
{
    Y_UNIT_TEST(ShouldConvertToRelativeBlockRange)
    {
        ASSERT_STRIPE_INFO_EQUALS(
            TStripeInfo(
                TBlockRange64(50, 99),
                0
            ),
            ConvertToRelativeBlockRange(
                10,
                TBlockRange64(50, 99),
                1,
                0
            )
        );

        ASSERT_STRIPE_INFO_EQUALS(
            TStripeInfo(
                TBlockRange64(55, 95),
                0
            ),
            ConvertToRelativeBlockRange(
                10,
                TBlockRange64(55, 95),
                1,
                0
            )
        );

        ASSERT_STRIPE_INFO_EQUALS(
            TStripeInfo(
                TBlockRange64(55, 58),
                0
            ),
            ConvertToRelativeBlockRange(
                10,
                TBlockRange64(55, 58),
                1,
                0
            )
        );

        ASSERT_STRIPE_INFO_EQUALS(
            TStripeInfo(
                TBlockRange64(15, 29),
                2
            ),
            ConvertToRelativeBlockRange(
                10,
                TBlockRange64(55, 95),
                3,
                0
            )
        );

        ASSERT_STRIPE_INFO_EQUALS(
            TStripeInfo(
                TBlockRange64(20, 35),
                0
            ),
            ConvertToRelativeBlockRange(
                10,
                TBlockRange64(55, 95),
                3,
                1
            )
        );

        ASSERT_STRIPE_INFO_EQUALS(
            TStripeInfo(
                TBlockRange64(20, 29),
                1
            ),
            ConvertToRelativeBlockRange(
                10,
                TBlockRange64(55, 95),
                3,
                2
            )
        );

        ASSERT_STRIPE_INFO_EQUALS(
            TStripeInfo(
                TBlockRange64(0, 341),
                0
            ),
            ConvertToRelativeBlockRange(
                2,
                TBlockRange64(0, 1023),
                3,
                0
            )
        );

        ASSERT_STRIPE_INFO_EQUALS(
            TStripeInfo(
                TBlockRange64(0, 341),
                1
            ),
            ConvertToRelativeBlockRange(
                2,
                TBlockRange64(0, 1023),
                3,
                1
            )
        );

        ASSERT_STRIPE_INFO_EQUALS(
            TStripeInfo(
                TBlockRange64(0, 339),
                2
            ),
            ConvertToRelativeBlockRange(
                2,
                TBlockRange64(0, 1023),
                3,
                2
            )
        );
    }

    Y_UNIT_TEST(ShouldConvertRelativeToGlobalIndex)
    {
        UNIT_ASSERT_VALUES_EQUAL(0, RelativeToGlobalIndex(10, 0, 3, 0));
        UNIT_ASSERT_VALUES_EQUAL(10, RelativeToGlobalIndex(10, 0, 3, 1));
        UNIT_ASSERT_VALUES_EQUAL(20, RelativeToGlobalIndex(10, 0, 3, 2));
        UNIT_ASSERT_VALUES_EQUAL(30, RelativeToGlobalIndex(10, 10, 3, 0));
        UNIT_ASSERT_VALUES_EQUAL(35, RelativeToGlobalIndex(10, 15, 3, 0));
        UNIT_ASSERT_VALUES_EQUAL(65, RelativeToGlobalIndex(10, 25, 3, 0));
        UNIT_ASSERT_VALUES_EQUAL(75, RelativeToGlobalIndex(10, 25, 3, 1));
    }

    Y_UNIT_TEST(ShouldCalculateRequestCount)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            CalculateRequestCount(
                10,
                TBlockRange64(55, 58),
                3
            )
        );

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            CalculateRequestCount(
                10,
                TBlockRange64(55, 68),
                3
            )
        );

        UNIT_ASSERT_VALUES_EQUAL(
            3,
            CalculateRequestCount(
                10,
                TBlockRange64(55, 550),
                3
            )
        );
    }
}

}   // namespace NCloud::NBlockStore::NStorage
