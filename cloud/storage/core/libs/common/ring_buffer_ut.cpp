#include "ring_buffer.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRingBufferTest)
{
    Y_UNIT_TEST(ShouldReturnCorrectCapacity)
    {
        {
            auto ringBuffer = TRingBuffer<int>(1);
            UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Capacity(), 1);
        }

        {
            auto ringBuffer = TRingBuffer<int>(123);
            UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Capacity(), 123);
        }
    }

    Y_UNIT_TEST(ShouldReturnCorrectSize)
    {
        auto ringBuffer = TRingBuffer<int>(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 0);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 0);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 0);

        ringBuffer.PushBack(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 1);

        ringBuffer.PushFront(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 2);

        ringBuffer.PushBack(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 3);

        ringBuffer.PushFront(4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 3);

        ringBuffer.PushBack(5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 3);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 2);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 1);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 0);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 0);
    }

    Y_UNIT_TEST(ShouldReturnCorrectIsEmpty)
    {
        auto ringBuffer = TRingBuffer<int>(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsEmpty(), true);

        ringBuffer.PushFront(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsEmpty(), false);

        ringBuffer.PushBack(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsEmpty(), false);

        ringBuffer.PushBack(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsEmpty(), false);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsEmpty(), false);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsEmpty(), true);
    }

    Y_UNIT_TEST(ShouldReturnCorrectIsFull)
    {
        auto ringBuffer = TRingBuffer<int>(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsFull(), false);

        ringBuffer.PushFront(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsFull(), false);

        ringBuffer.PushBack(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsFull(), true);

        ringBuffer.PushBack(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsFull(), true);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsFull(), false);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsFull(), false);
    }

    Y_UNIT_TEST(ShouldCorrectlyClear)
    {
        auto ringBuffer = TRingBuffer<int>(2);

        ringBuffer.PushFront(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsEmpty(), false);

        ringBuffer.Clear();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsEmpty(), true);

        ringBuffer.PushBack(1);
        ringBuffer.PushBack(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsFull(), true);

        ringBuffer.Clear();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsEmpty(), true);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.IsFull(), false);
    }

    Y_UNIT_TEST(ShouldCorrectlyPushPopBack)
    {
        auto ringBuffer = TRingBuffer<int>(3);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 0);

        ringBuffer.PushBack(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 1);

        ringBuffer.PushBack(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 1);

        ringBuffer.PushBack(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 1);

        ringBuffer.PushBack(4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 2);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 2);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 2);

        ringBuffer.PushBack(5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 2);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 2);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 0);
    }

    Y_UNIT_TEST(ShouldCorrectlyPushPopFrontiFront)
    {
        auto ringBuffer = TRingBuffer<int>(3);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 0);

        ringBuffer.PushFront(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 1);

        ringBuffer.PushFront(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 1);

        ringBuffer.PushFront(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 1);

        ringBuffer.PushFront(4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 2);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 2);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 2);

        ringBuffer.PushFront(5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 2);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 2);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.Size(), 0);
    }

    Y_UNIT_TEST(ShouldCorrectlyGetBack)
    {
        auto ringBuffer = TRingBuffer<int>(3);

        ringBuffer.PushBack(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 1);

        ringBuffer.PushBack(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 2);

        ringBuffer.PushBack(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 3);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 2);

        ringBuffer.PushBack(4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 4);

        ringBuffer.PushBack(5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 5);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 4);

        ringBuffer.Clear();

        ringBuffer.PushFront(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 1);

        ringBuffer.PushFront(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 1);

        ringBuffer.PushFront(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 1);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 1);

        ringBuffer.PushFront(4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 1);

        ringBuffer.PushFront(5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 2);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 2);

        ringBuffer.Clear();

        ringBuffer.PushBack(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 1);

        ringBuffer.PushFront(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 1);

        ringBuffer.PushBack(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 3);

        ringBuffer.PushFront(4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 1);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 2);

        ringBuffer.PushFront(5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 2);

        ringBuffer.PushBack(6);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 6);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 2);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetBack(), 4);
    }

    Y_UNIT_TEST(ShouldCorrectlyGetFront)
    {
        auto ringBuffer = TRingBuffer<int>(3);

        ringBuffer.PushFront(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 1);

        ringBuffer.PushFront(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 2);

        ringBuffer.PushFront(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 3);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 2);

        ringBuffer.PushFront(4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 4);

        ringBuffer.PushFront(5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 5);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 4);

        ringBuffer.Clear();

        ringBuffer.PushBack(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 1);

        ringBuffer.PushBack(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 1);

        ringBuffer.PushBack(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 1);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 1);

        ringBuffer.PushBack(4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 1);

        ringBuffer.PushBack(5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 2);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 2);

        ringBuffer.Clear();

        ringBuffer.PushFront(1);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 1);

        ringBuffer.PushBack(2);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 1);

        ringBuffer.PushFront(3);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 3);

        ringBuffer.PushBack(4);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 1);

        ringBuffer.PopBack();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 1);

        ringBuffer.PushBack(5);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 1);

        ringBuffer.PushFront(6);
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 6);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 1);

        ringBuffer.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(ringBuffer.GetFront(), 2);
    }
}

}   // namespace NCloud
