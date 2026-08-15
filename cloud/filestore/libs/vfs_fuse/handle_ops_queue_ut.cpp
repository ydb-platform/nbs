#include "handle_ops_queue.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 QueueSize = 4_KB;

////////////////////////////////////////////////////////////////////////////////

struct TEnv
{
    TTempDir Dir;

    THandleOpsQueuePtr CreateQueue()
    {
        const auto path = Dir.Path() / "handle_ops_queue";
        path.Touch();
        return CreateHandleOpsQueue(path.GetPath(), QueueSize);
    }

    static void AddCreate(THandleOpsQueue& queue, ui64 nodeId, ui64 handle)
    {
        NProto::TCreateHandleRequest request;
        request.SetNodeId(nodeId);
        UNIT_ASSERT_EQUAL(
            THandleOpsQueue::EResult::Ok,
            queue.AddCreateRequest(request, nodeId, handle, handle));
    }

    static void AddDestroy(THandleOpsQueue& queue, ui64 nodeId, ui64 handle)
    {
        UNIT_ASSERT_EQUAL(
            THandleOpsQueue::EResult::Ok,
            queue.AddDestroyRequest(nodeId, handle));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(THandleOpsQueueTest)
{
    Y_UNIT_TEST(ShouldTrackUnconfirmedCreatesOnly)
    {
        TEnv env;
        auto queue = env.CreateQueue();

        UNIT_ASSERT(!queue->HasUnconfirmedCreate(42));

        TEnv::AddDestroy(*queue, 10, 42);
        UNIT_ASSERT(!queue->HasUnconfirmedCreate(42));

        TEnv::AddCreate(*queue, 10, 42);
        UNIT_ASSERT(queue->HasUnconfirmedCreate(42));

        // the destroy entry confirms nothing, the create entry does
        auto popped = queue->PopFront();
        UNIT_ASSERT(!popped);
        UNIT_ASSERT(queue->HasUnconfirmedCreate(42));

        popped = queue->PopFront();
        UNIT_ASSERT(popped);
        UNIT_ASSERT_VALUES_EQUAL(42, *popped);
        UNIT_ASSERT(!queue->HasUnconfirmedCreate(42));
        UNIT_ASSERT(queue->Empty());
    }

    Y_UNIT_TEST(ShouldConfirmPoppedCreateOnly)
    {
        TEnv env;
        auto queue = env.CreateQueue();

        TEnv::AddCreate(*queue, 10, 1);
        TEnv::AddCreate(*queue, 10, 2);

        auto popped = queue->PopFront();
        UNIT_ASSERT(popped);
        UNIT_ASSERT_VALUES_EQUAL(1, *popped);

        // the second handle is still unconfirmed
        UNIT_ASSERT(!queue->HasUnconfirmedCreate(1));
        UNIT_ASSERT(queue->HasUnconfirmedCreate(2));

        popped = queue->PopFront();
        UNIT_ASSERT(popped);
        UNIT_ASSERT_VALUES_EQUAL(2, *popped);
        UNIT_ASSERT(!queue->HasUnconfirmedCreate(2));
        UNIT_ASSERT(queue->Empty());
    }

    Y_UNIT_TEST(ShouldRestoreUnconfirmedCreatesFromFile)
    {
        TEnv env;

        {
            auto queue = env.CreateQueue();
            TEnv::AddCreate(*queue, 10, 1);
            TEnv::AddDestroy(*queue, 10, 2);
        }

        auto queue = env.CreateQueue();
        UNIT_ASSERT_VALUES_EQUAL(2, queue->Size());
        UNIT_ASSERT(queue->HasUnconfirmedCreate(1));
        UNIT_ASSERT(!queue->HasUnconfirmedCreate(2));

        const auto popped = queue->PopFront();
        UNIT_ASSERT(popped);
        UNIT_ASSERT_VALUES_EQUAL(1, *popped);
        UNIT_ASSERT(!queue->HasUnconfirmedCreate(1));
    }
}

}   // namespace NCloud::NFileStore::NFuse
