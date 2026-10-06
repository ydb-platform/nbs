#include "commit_queue.h"

#include <library/cpp/testing/unittest/registar.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestItem
{
    const ui64 CommitId;

    explicit TTestItem(ui64 commitId)
        : CommitId(commitId)
    {}
};

using TTestCommitQueue = TCommitQueueImpl<std::unique_ptr<TTestItem>>;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCommitQueueTest)
{
    Y_UNIT_TEST(ShouldKeepTrackOfCommits)
    {
        TTestCommitQueue queue;

        queue.Enqueue(std::make_unique<TTestItem>(1), 1);
        queue.Enqueue(std::make_unique<TTestItem>(2), 2);
        queue.Enqueue(std::make_unique<TTestItem>(3), 3);

        UNIT_ASSERT(!queue.Empty());

        UNIT_ASSERT_EQUAL(queue.Peek(), 1);
        auto item = queue.Dequeue();
        UNIT_ASSERT_EQUAL(item->CommitId, 1);

        UNIT_ASSERT_EQUAL(queue.Peek(), 2);
        item = queue.Dequeue();
        UNIT_ASSERT_EQUAL(item->CommitId, 2);

        UNIT_ASSERT_EQUAL(queue.Peek(), 3);
        item = queue.Dequeue();
        UNIT_ASSERT_EQUAL(item->CommitId, 3);

        UNIT_ASSERT(queue.Empty());
        UNIT_ASSERT_EQUAL(queue.Peek(), Max<ui64>());
        UNIT_ASSERT(!queue.Dequeue());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
