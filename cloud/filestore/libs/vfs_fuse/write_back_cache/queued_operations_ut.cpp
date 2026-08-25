#include "queued_operations.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TProcessor final: public IQueuedOperationsProcessor
{
private:
    TQueuedOperations Operations{*this};
    ui32 CurrentDepth = 0;

public:
    ui32 MaxDepth = 0;
    ui32 ProcessedCount = 0;

    void Start(ui64 operationCount)
    {
        Operations.Acquire();
        Operations.ScheduleFlushNode(operationCount);
        Operations.Release();
    }

private:
    void ScheduleFlushNode(ui64 remainingOperationCount) override
    {
        ++CurrentDepth;
        MaxDepth = Max(MaxDepth, CurrentDepth);
        ++ProcessedCount;

        if (remainingOperationCount > 1) {
            Operations.Acquire();
            Operations.ScheduleFlushNode(remainingOperationCount - 1);
            Operations.Release();
        }

        --CurrentDepth;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TQueuedOperationsTest)
{
    Y_UNIT_TEST(ShouldProcessQueuedOperationsWithoutRecursion)
    {
        TProcessor processor;

        processor.Start(100'000);

        UNIT_ASSERT_VALUES_EQUAL(100'000, processor.ProcessedCount);
        UNIT_ASSERT_VALUES_EQUAL(1, processor.MaxDepth);
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
