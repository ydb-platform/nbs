#include "requests_in_progress.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRequestsInProgressTest)
{
    Y_UNIT_TEST(Basic)
    {
        TRequestsInProgress<ui32, TString> requestsInProgress{
            EAllowedRequests::ReadWrite};
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
        TRequestsInProgress<ui32, TString> requestsInProgress{
            EAllowedRequests::ReadWrite};
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
        TRequestsInProgress<ui32, TString> requestsInProgress{
            EAllowedRequests::ReadWrite};

        requestsInProgress.SetRequestIdentityKey(1000);
        auto id1 = requestsInProgress.AddReadRequest("Read Request 1");
        UNIT_ASSERT_EQUAL(id1, 1000);

        auto id2 = requestsInProgress.AddWriteRequest("Write Request 1");
        UNIT_ASSERT_EQUAL(id2, 1001);
    }

    Y_UNIT_TEST(GenerateIdentityKey)
    {
        TRequestsInProgress<ui32, TString> requestsInProgress{
            EAllowedRequests::ReadWrite};

        auto id1 = requestsInProgress.GenerateRequestId();
        auto id2 = requestsInProgress.GenerateRequestId();
        UNIT_ASSERT_UNEQUAL(id1, id2);
    }

    Y_UNIT_TEST(AllRequests)
    {
        using TRequests = TRequestsInProgress<ui32, TString>;

        TMap<ui32, TRequests::TRequest> testData{
            {0, {"Read Request 1", false}},
            {1, {"Write Request 1", true}},
            {10, {"Read Request 2", false}},
            {20, {"Write Request 2", true}},
        };

        TRequests requestsInProgress{EAllowedRequests::ReadWrite};
        for (const auto& item : testData) {
            if (item.second.Write) {
                requestsInProgress.AddWriteRequest(
                    item.first, TString(item.second.Value));
            } else {
                requestsInProgress.AddReadRequest(
                    item.first, TString(item.second.Value));
            }
        }

        for (const auto& request : requestsInProgress.AllRequests()) {
            ui32 id = request.first;
            const TRequests::TRequest& item = request.second;
            const auto& testItem = testData[id];
            UNIT_ASSERT_EQUAL(testItem.Write, item.Write);
            UNIT_ASSERT_EQUAL(testItem.Value, item.Value);
        }
    }

    Y_UNIT_TEST(ShouldWaitForInFlightWrites)
    {
        using TRequests = TRequestsInProgress<ui32, TString>;

        TMap<ui32, TRequests::TRequest> testData{
            {0, {"Read Request 1", false}},
            {1, {"Write Request 1", true}},
            {10, {"Read Request 2", false}},
            {20, {"Write Request 2", true}},
        };

        // When there is no in-flight requests waiting does nothing.
        TRequests requestsInProgress{EAllowedRequests::ReadWrite};
        UNIT_ASSERT(!requestsInProgress.IsWaitingForInFlightWrites());
        requestsInProgress.WaitForInFlightWrites();
        UNIT_ASSERT(!requestsInProgress.IsWaitingForInFlightWrites());

        for (const auto& item : testData) {
            if (item.second.Write) {
                requestsInProgress.AddWriteRequest(
                    item.first, TString(item.second.Value));
            } else {
                requestsInProgress.AddReadRequest(
                    item.first, TString(item.second.Value));
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

}  // namespace NCloud::NBlockStore::NStorage::NPartition
