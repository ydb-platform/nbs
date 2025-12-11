#include "concurrent_queue.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TConcurrentQueueTest)
{
    Y_UNIT_TEST(ShouldBeEmptyAtStart)
    {
        TConcurrentQueue<size_t> queue;

        UNIT_ASSERT(queue.IsEmpty());

        auto item = queue.Dequeue();
        UNIT_ASSERT(!item);
    }

    void ShouldKeepItems(size_t count)
    {
        TConcurrentQueue<size_t> queue;
        for (size_t i = 0; i < count; ++i) {
            queue.Enqueue(std::make_unique<size_t>(i));
        }

        UNIT_ASSERT(!queue.IsEmpty());

        for (size_t i = 0; i < count; ++i) {
            auto item = queue.Dequeue();
            UNIT_ASSERT(item);
            UNIT_ASSERT_EQUAL(*item, i);
        }

        auto item = queue.Dequeue();
        UNIT_ASSERT(!item);
    }

    Y_UNIT_TEST(ShouldKeepItems_OneSegment)
    {
        ShouldKeepItems(100);
    }

    Y_UNIT_TEST(ShouldKeepItems_ManySegments)
    {
        ShouldKeepItems(100 * 1000);
    }
}

}   // namespace NCloud
