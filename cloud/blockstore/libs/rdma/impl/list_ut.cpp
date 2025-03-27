#include "list.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NRdma {

namespace  {

////////////////////////////////////////////////////////////////////////////////

struct TTestNode : TListNode<TTestNode>
{
    const int Value;

    TTestNode(int value)
        : Value(value)
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSimpleListTest)
{
    Y_UNIT_TEST(ShouldDequeueItemsFIFO)
    {
        TSimpleList<TTestNode> list;
        for (int i = 1; i < 10; ++i) {
            list.Enqueue(std::make_unique<TTestNode>(i));
        }

        for (int i = 1; i < 10; ++i) {
            auto node = list.Dequeue();
            UNIT_ASSERT(node);
            UNIT_ASSERT_EQUAL(node->Value, i);
        }

        UNIT_ASSERT(!list.Dequeue());
    }

    Y_UNIT_TEST(ShouldAppendLists)
    {
        TSimpleList<TTestNode> list1;
        for (int i = 1; i < 5; ++i) {
            list1.Enqueue(std::make_unique<TTestNode>(i));
        }

        TSimpleList<TTestNode> list2;
        for (int i = 5; i < 10; ++i) {
            list2.Enqueue(std::make_unique<TTestNode>(i));
        }

        TSimpleList<TTestNode> list;
        list.Append(std::move(list1));
        list.Append(std::move(list2));

        for (int i = 1; i < 10; ++i) {
            auto node = list.Dequeue();
            UNIT_ASSERT(node);
            UNIT_ASSERT_EQUAL(node->Value, i);
        }

        UNIT_ASSERT(!list.Dequeue());
    }

    Y_UNIT_TEST(ShouldDequeIf)
    {
        TSimpleList<TTestNode> even;
        TSimpleList<TTestNode> odd;
        TSimpleList<TTestNode> list1;
        for (int i = 0; i < 10; ++i) {
            list1.Enqueue(std::make_unique<TTestNode>(i));
            if (i % 2 == 0) {
                even.Enqueue(std::make_unique<TTestNode>(i));
            } else {
                odd.Enqueue(std::make_unique<TTestNode>(i));
            }
        }

        auto onlyEven = list1.DequeueIf([](const auto& node)
                                        { return node.Value % 2 == 0; });

        while (onlyEven && even) {
            auto nodeExpected = even.Dequeue();
            auto nodeActual = onlyEven.Dequeue();
            UNIT_ASSERT_VALUES_EQUAL(nodeExpected->Value, nodeActual->Value);
        }

        UNIT_ASSERT(!onlyEven);
        UNIT_ASSERT(!even);

        while (list1 && odd) {
            auto nodeExpected = odd.Dequeue();
            auto nodeActual = list1.Dequeue();
            UNIT_ASSERT_VALUES_EQUAL(nodeExpected->Value, nodeActual->Value);
        }

        UNIT_ASSERT(!odd);
        UNIT_ASSERT(!list1);
    }

    Y_UNIT_TEST(ShouldIterate)
    {
        TSimpleList<TTestNode> list1;
        for (int i = 0; i < 10; ++i) {
            list1.Enqueue(std::make_unique<TTestNode>(i));
        }

        int i = 0;
        for (auto& node : list1) {
            UNIT_ASSERT_VALUES_EQUAL(i, node.Value);
            ++i;
        }

        UNIT_ASSERT_VALUES_EQUAL(10, i);
    }
};

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLockFreeListTest)
{
    Y_UNIT_TEST(ShouldDequeueItemsFIFO)
    {
        TLockFreeList<TTestNode> list;
        for (int i = 1; i < 10; ++i) {
            list.Enqueue(std::make_unique<TTestNode>(i));
        }

        auto items = list.DequeueAll();

        for (int i = 1; i < 10; ++i) {
            auto node = items.Dequeue();
            UNIT_ASSERT(node);
            UNIT_ASSERT_EQUAL(node->Value, i);
        }

        UNIT_ASSERT(!items.Dequeue());
    }
};

}   // namespace NCloud::NBlockStore::NRdma
