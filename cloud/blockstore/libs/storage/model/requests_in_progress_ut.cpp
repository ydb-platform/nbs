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

        auto readId1 = requestsInProgress.AddReadRequest(
            TBlockRange64::MakeOneBlock(1),
            "Read Request 1");
        auto writeId1 = requestsInProgress.AddWriteRequest(
            TBlockRange64::MakeOneBlock(1),
            "Write Request 1");
        auto readId2 = requestsInProgress.AddReadRequest(
            TBlockRange64::MakeOneBlock(2),
            "Read Request 2");
        auto writeId2 = requestsInProgress.AddWriteRequest(
            TBlockRange64::MakeOneBlock(2),
            "Write Request 2");
        auto readId3 = requestsInProgress.AddReadRequest(
            TBlockRange64::MakeOneBlock(3),
            "Read Request 3");

        UNIT_ASSERT_EQUAL(requestsInProgress.GetRequestCount(), 5);
        UNIT_ASSERT(interface->WriteRequestInProgress());

        requestsInProgress.RemoveReadRequest(readId3);
        requestsInProgress.RemoveWriteRequest(writeId1);

        UNIT_ASSERT_EQUAL(requestsInProgress.GetRequestCount(), 3);
        UNIT_ASSERT(interface->WriteRequestInProgress());

        requestsInProgress.RemoveWriteRequest(writeId2);

        UNIT_ASSERT_EQUAL(requestsInProgress.GetRequestCount(), 2);
        UNIT_ASSERT(!interface->WriteRequestInProgress());

        requestsInProgress.RemoveReadRequest(readId1);
        requestsInProgress.RemoveReadRequest(readId2);

        UNIT_ASSERT_EQUAL(requestsInProgress.GetRequestCount(), 0);
        UNIT_ASSERT(!interface->WriteRequestInProgress());
    }

    Y_UNIT_TEST(CustomId)
    {
        TRequestsInProgress<EAllowedRequests::ReadWrite, ui32, TString>
            requestsInProgress;
        IRequestsInProgress* interface = &requestsInProgress;

        requestsInProgress.AddReadRequest(
            100,
            TBlockRange64::MakeOneBlock(1),
            "Read Request 1");
        requestsInProgress.AddWriteRequest(
            200,
            TBlockRange64::MakeOneBlock(1),
            "Write Request 1");

        UNIT_ASSERT_EQUAL(requestsInProgress.GetRequestCount(), 2);
        UNIT_ASSERT(interface->WriteRequestInProgress());

        requestsInProgress.RemoveReadRequest(100);
        UNIT_ASSERT_EQUAL(requestsInProgress.GetRequestCount(), 1);
        UNIT_ASSERT(interface->WriteRequestInProgress());

        requestsInProgress.RemoveWriteRequest(200);
        UNIT_ASSERT_EQUAL(requestsInProgress.GetRequestCount(), 0);
        UNIT_ASSERT(!interface->WriteRequestInProgress());
    }

    Y_UNIT_TEST(ResetIdentityKey)
    {
        TRequestsInProgress<EAllowedRequests::ReadWrite, ui32, TString>
            requestsInProgress;

        requestsInProgress.SetRequestIdentityKey(1000);
        auto id1 = requestsInProgress.AddReadRequest(
            TBlockRange64::MakeOneBlock(1),
            "Read Request 1");
        UNIT_ASSERT_EQUAL(id1, 1000);

        auto id2 = requestsInProgress.AddWriteRequest(
            TBlockRange64::MakeOneBlock(1),
            "Write Request 1");
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

    Y_UNIT_TEST(EnumerateRequests)
    {
        using TRequests =
            TRequestsInProgress<EAllowedRequests::ReadWrite, ui32, TString>;

        struct TComparator
        {
            bool operator()(
                const TRequests::TItem& lhs,
                const TRequests::TItem& rhs) const
            {
                return lhs.Key < rhs.Key;
            }
        };

        TSet<TRequests::TItem, TComparator> testReadData{
            {.Key = 0,
             .Range = TBlockRange64::MakeOneBlock(1),
             .Value = "Read Request 1"},
            {.Key = 10,
             .Range = TBlockRange64::MakeOneBlock(2),
             .Value = "Read Request 2"},
        };

        TSet<TRequests::TItem, TComparator> testWriteData{
            {.Key = 1,
             .Range = TBlockRange64::MakeOneBlock(3),
             .Value = "Write Request 1"},
            {.Key = 20,
             .Range = TBlockRange64::MakeOneBlock(4),
             .Value = "Write Request 2"},
        };

        TRequests requestsInProgress;
        for (const auto& item: testReadData) {
            requestsInProgress.AddReadRequest(item.Key, item.Range, item.Value);
        }
        for (const auto& item: testWriteData) {
            requestsInProgress.AddWriteRequest(
                item.Key,
                item.Range,
                item.Value);
        }

        requestsInProgress.EnumerateRequests(
            [&](ui32 key,
                bool isWrite,
                TBlockRange64 range,
                const TString& value)
            {
                auto it = testWriteData.end();
                if (isWrite) {
                    it = testWriteData.find(TRequests::TItem{.Key = key});

                } else {
                    it = testReadData.find(TRequests::TItem{.Key = key});
                }
                UNIT_ASSERT_UNEQUAL(testWriteData.end(), it);
                UNIT_ASSERT_UNEQUAL(testReadData.end(), it);
                UNIT_ASSERT_EQUAL(it->Range, range);
                UNIT_ASSERT_EQUAL(it->Value, value);
            });
    }

    Y_UNIT_TEST(ShouldWaitForInFlightWrites)
    {
        using TRequests =
            TRequestsInProgress<EAllowedRequests::ReadWrite, ui32, TString>;

        TVector<TRequests::TItem> testReadData{
            {.Key = 0,
             .Range = TBlockRange64::MakeOneBlock(1),
             .Value = "Read Request 1"},
            {.Key = 10,
             .Range = TBlockRange64::MakeOneBlock(2),
             .Value = "Read Request 2"},
        };

        TVector<TRequests::TItem> testWriteData{
            {.Key = 1,
             .Range = TBlockRange64::MakeOneBlock(3),
             .Value = "Write Request 1"},
            {.Key = 20,
             .Range = TBlockRange64::MakeOneBlock(4),
             .Value = "Write Request 2"},
        };

        // When there is no in-flight requests waiting does nothing.
        TRequests requestsInProgress;
        UNIT_ASSERT(!requestsInProgress.IsWaitingForInFlightWrites());
        requestsInProgress.WaitForInFlightWrites();
        UNIT_ASSERT(!requestsInProgress.IsWaitingForInFlightWrites());

        for (const auto& item: testReadData) {
            requestsInProgress.AddReadRequest(item.Key, item.Range, item.Value);
        }
        for (const auto& item: testWriteData) {
            requestsInProgress.AddWriteRequest(
                item.Key,
                item.Range,
                item.Value);
        }

        UNIT_ASSERT(!requestsInProgress.IsWaitingForInFlightWrites());
        requestsInProgress.WaitForInFlightWrites();
        UNIT_ASSERT(requestsInProgress.IsWaitingForInFlightWrites());

        requestsInProgress.RemoveWriteRequest(1);
        UNIT_ASSERT(requestsInProgress.IsWaitingForInFlightWrites());
        requestsInProgress.RemoveWriteRequest(20);
        UNIT_ASSERT(!requestsInProgress.IsWaitingForInFlightWrites());
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
