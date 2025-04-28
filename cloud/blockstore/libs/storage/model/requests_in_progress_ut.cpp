#include "common_constants.h"
#include "request_bounds_tracker.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRequestsInProgressTest)
{
    Y_UNIT_TEST(Basic)
    {
        TRequestsInProgress<EAllowedRequests::ReadWrite, ui32, TString>
            requestsInProgress;
        IRequestsInProgress* interface = &requestsInProgress;

        UNIT_ASSERT(requestsInProgress.Empty());
        UNIT_ASSERT_EQUAL(requestsInProgress.GetRequestCount(), 0);
        UNIT_ASSERT(!interface->WriteRequestInProgress());

        auto readId1 = requestsInProgress.AddReadRequest("Read Request 1");
        auto writeId1 = requestsInProgress.AddWriteRequest("Write Request 1");
        auto readId2 = requestsInProgress.AddReadRequest("Read Request 2");
        auto writeId2 = requestsInProgress.AddWriteRequest("Write Request 2");
        auto readId3 = requestsInProgress.AddReadRequest("Read Request 3");

        UNIT_ASSERT_EQUAL(requestsInProgress.GetRequestCount(), 5);
        UNIT_ASSERT(interface->WriteRequestInProgress());

        requestsInProgress.RemoveRequest(readId3);
        requestsInProgress.RemoveRequest(writeId1);

        UNIT_ASSERT_EQUAL(requestsInProgress.GetRequestCount(), 3);
        UNIT_ASSERT(interface->WriteRequestInProgress());

        requestsInProgress.RemoveRequest(writeId2);

        UNIT_ASSERT_EQUAL(requestsInProgress.GetRequestCount(), 2);
        UNIT_ASSERT(!interface->WriteRequestInProgress());

        requestsInProgress.RemoveRequest(readId1);
        requestsInProgress.RemoveRequest(readId2);

        UNIT_ASSERT_EQUAL(requestsInProgress.GetRequestCount(), 0);
        UNIT_ASSERT(!interface->WriteRequestInProgress());
    }

    Y_UNIT_TEST(CustomId)
    {
        TRequestsInProgress<EAllowedRequests::ReadWrite, ui32, TString>
            requestsInProgress;
        IRequestsInProgress* interface = &requestsInProgress;

        requestsInProgress.AddReadRequest(100, "Read Request 1");
        requestsInProgress.AddWriteRequest(200, "Write Request 1");

        UNIT_ASSERT_EQUAL(requestsInProgress.GetRequestCount(), 2);
        UNIT_ASSERT(interface->WriteRequestInProgress());

        requestsInProgress.RemoveRequest(100);
        UNIT_ASSERT_EQUAL(requestsInProgress.GetRequestCount(), 1);
        UNIT_ASSERT(interface->WriteRequestInProgress());

        requestsInProgress.RemoveRequest(200);
        UNIT_ASSERT_EQUAL(requestsInProgress.GetRequestCount(), 0);
        UNIT_ASSERT(!interface->WriteRequestInProgress());
    }

    Y_UNIT_TEST(ResetIdentityKey)
    {
        TRequestsInProgress<EAllowedRequests::ReadWrite, ui32, TString>
            requestsInProgress;

        requestsInProgress.SetRequestIdentityKey(1000);
        auto id1 = requestsInProgress.AddReadRequest("Read Request 1");
        UNIT_ASSERT_EQUAL(id1, 1000);

        auto id2 = requestsInProgress.AddWriteRequest("Write Request 1");
        UNIT_ASSERT_EQUAL(id2, 1001);
    }

    Y_UNIT_TEST(GenerateIdentityKey)
    {
        TRequestsInProgress<EAllowedRequests::ReadWrite, ui32, TString>
            requestsInProgress;

        auto id1 = requestsInProgress.GenerateRequestId();
        auto id2 = requestsInProgress.GenerateRequestId();
        UNIT_ASSERT_UNEQUAL(id1, id2);
    }

    Y_UNIT_TEST(AllRequests)
    {
        using TRequests =
            TRequestsInProgress<EAllowedRequests::ReadWrite, ui32, TString>;

        TMap<ui32, TRequests::TRequest> testData{
            {0, {.Value = "Read Request 1", .IsWrite = false}},
            {1, {.Value = "Write Request 1", .IsWrite = true}},
            {10, {.Value = "Read Request 2", .IsWrite = false}},
            {20, {.Value = "Write Request 2", .IsWrite = true}},
        };

        TRequests requestsInProgress;
        for (const auto& item: testData) {
            if (item.second.IsWrite) {
                requestsInProgress.AddWriteRequest(
                    item.first,
                    TString(item.second.Value));
            } else {
                requestsInProgress.AddReadRequest(
                    item.first,
                    TString(item.second.Value));
            }
        }

        for (const auto& request: requestsInProgress.AllRequests()) {
            ui32 id = request.first;
            const TRequests::TRequest& item = request.second;
            const auto& testItem = testData[id];
            UNIT_ASSERT_EQUAL(testItem.IsWrite, item.IsWrite);
            UNIT_ASSERT_EQUAL(testItem.Value, item.Value);
        }
    }

    Y_UNIT_TEST(ShouldWaitForInFlightWrites)
    {
        using TRequests =
            TRequestsInProgress<EAllowedRequests::ReadWrite, ui32, TString>;

        TMap<ui32, TRequests::TRequest> testData{
            {0, {.Value = "Read Request 1", .IsWrite = false}},
            {1, {.Value = "Write Request 1", .IsWrite = true}},
            {10, {.Value = "Read Request 2", .IsWrite = false}},
            {20, {.Value = "Write Request 2", .IsWrite = true}},
        };

        // When there is no in-flight requests waiting does nothing.
        TRequests requestsInProgress;
        UNIT_ASSERT(!requestsInProgress.IsWaitingForInFlightWrites());
        requestsInProgress.WaitForInFlightWrites();
        UNIT_ASSERT(!requestsInProgress.IsWaitingForInFlightWrites());

        for (const auto& item: testData) {
            if (item.second.IsWrite) {
                requestsInProgress.AddWriteRequest(
                    item.first,
                    TString(item.second.Value));
            } else {
                requestsInProgress.AddReadRequest(
                    item.first,
                    TString(item.second.Value));
            }
        }

        UNIT_ASSERT(!requestsInProgress.IsWaitingForInFlightWrites());
        requestsInProgress.WaitForInFlightWrites();
        UNIT_ASSERT(requestsInProgress.IsWaitingForInFlightWrites());

        requestsInProgress.RemoveRequest(1);
        UNIT_ASSERT(requestsInProgress.IsWaitingForInFlightWrites());
        requestsInProgress.RemoveRequest(20);
        UNIT_ASSERT(!requestsInProgress.IsWaitingForInFlightWrites());
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
