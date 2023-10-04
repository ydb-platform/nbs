#include "operation.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TOperationTest)
{
    Y_UNIT_TEST(BlobIndexOpQueueTest)
    {
        TBlobIndexOpQueue q;
        q.Push(EBlobIndexOp::Cleanup);
        q.Push(EBlobIndexOp::Compaction);
        q.Push(EBlobIndexOp::FlushBytes);
        q.Push(EBlobIndexOp::Cleanup);
        q.Push(EBlobIndexOp::Compaction);

        UNIT_ASSERT(!q.Empty());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(EBlobIndexOp::Cleanup),
            static_cast<int>(q.Pop()));

        UNIT_ASSERT(!q.Empty());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(EBlobIndexOp::Compaction),
            static_cast<int>(q.Pop()));

        UNIT_ASSERT(!q.Empty());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(EBlobIndexOp::FlushBytes),
            static_cast<int>(q.Pop()));

        UNIT_ASSERT(q.Empty());

        q.Push(EBlobIndexOp::Compaction);
        q.Push(EBlobIndexOp::Cleanup);

        UNIT_ASSERT(!q.Empty());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(EBlobIndexOp::Compaction),
            static_cast<int>(q.Pop()));

        q.Push(EBlobIndexOp::Compaction);

        UNIT_ASSERT(!q.Empty());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(EBlobIndexOp::Cleanup),
            static_cast<int>(q.Pop()));

        UNIT_ASSERT(!q.Empty());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(EBlobIndexOp::Compaction),
            static_cast<int>(q.Pop()));

        UNIT_ASSERT(q.Empty());
    }
}

}   // namespace NCloud::NFileStore::NStorage
