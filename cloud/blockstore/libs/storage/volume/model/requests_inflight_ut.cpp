#include "requests_inflight.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRequestsInFlightTest)
{
    Y_UNIT_TEST(ShouldRejectIntersectingRequests)
    {
        TRequestsInFlight requestsInFlight;

        UNIT_ASSERT_VALUES_EQUAL(
            true,
            requestsInFlight
                .TryAddRequest(1, TBlockRange64::MakeClosedInterval(10, 20))
                .Added);

        UNIT_ASSERT_VALUES_EQUAL(
            true,
            requestsInFlight
                .TryAddRequest(2, TBlockRange64::MakeClosedInterval(80, 100))
                .Added);

        UNIT_ASSERT_VALUES_EQUAL(
            true,
            requestsInFlight
                .TryAddRequest(3, TBlockRange64::MakeClosedInterval(40, 50))
                .Added);

        auto addResult = requestsInFlight.TryAddRequest(
            4,
            TBlockRange64::MakeClosedInterval(5, 15));
        UNIT_ASSERT(!addResult.Added);
        UNIT_ASSERT_VALUES_EQUAL(
            TRequestsInFlight::InvalidRequestId,
            addResult.DuplicateRequestId);

        addResult = requestsInFlight.TryAddRequest(
            5,
            TBlockRange64::MakeClosedInterval(25, 45));
        UNIT_ASSERT(!addResult.Added);
        UNIT_ASSERT_VALUES_EQUAL(
            TRequestsInFlight::InvalidRequestId,
            addResult.DuplicateRequestId);

        addResult = requestsInFlight.TryAddRequest(
            6,
            TBlockRange64::MakeClosedInterval(35, 55));
        UNIT_ASSERT(!addResult.Added);
        UNIT_ASSERT_VALUES_EQUAL(
            TRequestsInFlight::InvalidRequestId,
            addResult.DuplicateRequestId);

        addResult = requestsInFlight.TryAddRequest(
            7,
            TBlockRange64::MakeClosedInterval(50, 120));
        UNIT_ASSERT(!addResult.Added);
        UNIT_ASSERT_VALUES_EQUAL(
            TRequestsInFlight::InvalidRequestId,
            addResult.DuplicateRequestId);

        addResult = requestsInFlight.TryAddRequest(
            8,
            TBlockRange64::MakeClosedInterval(40, 50));
        UNIT_ASSERT(!addResult.Added);
        UNIT_ASSERT_VALUES_EQUAL(3, addResult.DuplicateRequestId);

        requestsInFlight.RemoveRequest(3);

        addResult = requestsInFlight.TryAddRequest(
            9,
            TBlockRange64::MakeClosedInterval(15, 75));
        UNIT_ASSERT(!addResult.Added);
        UNIT_ASSERT_VALUES_EQUAL(
            TRequestsInFlight::InvalidRequestId,
            addResult.DuplicateRequestId);

        addResult = requestsInFlight.TryAddRequest(
            10,
            TBlockRange64::MakeClosedInterval(30, 85));
        UNIT_ASSERT(!addResult.Added);
        UNIT_ASSERT_VALUES_EQUAL(
            TRequestsInFlight::InvalidRequestId,
            addResult.DuplicateRequestId);

        requestsInFlight.RemoveRequest(2);

        addResult = requestsInFlight.TryAddRequest(
            11,
            TBlockRange64::MakeClosedInterval(5, 95));
        UNIT_ASSERT(!addResult.Added);
        UNIT_ASSERT_VALUES_EQUAL(
            TRequestsInFlight::InvalidRequestId,
            addResult.DuplicateRequestId);

        requestsInFlight.RemoveRequest(1);

        UNIT_ASSERT_VALUES_EQUAL(
            true,
            requestsInFlight
                .TryAddRequest(12, TBlockRange64::MakeClosedInterval(20, 30))
                .Added);

        addResult = requestsInFlight.TryAddRequest(
            13,
            TBlockRange64::MakeClosedInterval(25, 30));
        UNIT_ASSERT(!addResult.Added);
        UNIT_ASSERT_VALUES_EQUAL(12, addResult.DuplicateRequestId);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
