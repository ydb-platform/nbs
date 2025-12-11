#include "commit_queue.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionStub: public ITransactionBase
{
    const ui64 CommitId;

    TTransactionStub(ui64 commitId)
        : CommitId(commitId)
    {}

    void Init(const NActors::TActorContext&) override
    {}

    bool Execute(
        NKikimr::NTabletFlatExecutor::TTransactionContext&,
        const NActors::TActorContext&) override
    {
        return false;
    }

    void Complete(const NActors::TActorContext&) override
    {}
};

ui64 GetCommitId(const ITransactionBase& tx)
{
    return static_cast<const TTransactionStub&>(tx).CommitId;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCommitQueueTest)
{
    Y_UNIT_TEST(ShouldKeepTrackOfCommits)
    {
        TCommitQueue queue;

        queue.Enqueue(std::make_unique<TTransactionStub>(1), 1);
        queue.Enqueue(std::make_unique<TTransactionStub>(2), 2);
        queue.Enqueue(std::make_unique<TTransactionStub>(3), 3);

        UNIT_ASSERT(!queue.Empty());

        UNIT_ASSERT_EQUAL(queue.Peek(), 1);
        auto tx = queue.Dequeue();
        UNIT_ASSERT_EQUAL(GetCommitId(*tx), 1);

        UNIT_ASSERT_EQUAL(queue.Peek(), 2);
        tx = queue.Dequeue();
        UNIT_ASSERT_EQUAL(GetCommitId(*tx), 2);

        UNIT_ASSERT_EQUAL(queue.Peek(), 3);
        tx = queue.Dequeue();
        UNIT_ASSERT_EQUAL(GetCommitId(*tx), 3);

        UNIT_ASSERT(queue.Empty());
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
