#include "list.h"

#include <library/cpp/testing/gtest/gtest.h>

namespace NCloud::NStorage::NRdma {

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

TEST(TSimpleListTest, ShouldDequeueItemsFIFO)
{
    TSimpleList<TTestNode> list;
    for (int i = 1; i < 10; ++i) {
        list.Enqueue(std::make_unique<TTestNode>(i));
    }

    for (int i = 1; i < 10; ++i) {
        auto node = list.Dequeue();
        ASSERT_TRUE(node);
        ASSERT_EQ(node->Value, i);
    }

    ASSERT_FALSE(list.Dequeue());
}

TEST(TSimpleListTest, ShouldAppendLists)
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
        ASSERT_TRUE(node);
        ASSERT_EQ(node->Value, i);
    }

    ASSERT_FALSE(list.Dequeue());
}

TEST(TSimpleListTest, ShouldDequeIf)
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
        ASSERT_EQ(nodeExpected->Value, nodeActual->Value);
    }

    ASSERT_FALSE(onlyEven);
    ASSERT_FALSE(even);

    while (list1 && odd) {
        auto nodeExpected = odd.Dequeue();
        auto nodeActual = list1.Dequeue();
        ASSERT_EQ(nodeExpected->Value, nodeActual->Value);
    }

    ASSERT_FALSE(odd);
    ASSERT_FALSE(list1);
}

TEST(TSimpleListTest, ShouldIterate)
{
    TSimpleList<TTestNode> list1;
    for (int i = 0; i < 10; ++i) {
        list1.Enqueue(std::make_unique<TTestNode>(i));
    }

    int i = 0;
    for (auto& node: list1) {
        ASSERT_EQ(i, node.Value);
        ++i;
    }

    ASSERT_EQ(10, i);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TLockFreeListTest, ShouldDequeueItemsFIFO)
{
    TLockFreeList<TTestNode> list;
    for (int i = 1; i < 10; ++i) {
        list.Enqueue(std::make_unique<TTestNode>(i));
    }

    auto items = list.DequeueAll();

    for (int i = 1; i < 10; ++i) {
        auto node = items.Dequeue();
        ASSERT_TRUE(node);
        ASSERT_EQ(node->Value, i);
    }

    ASSERT_FALSE(items.Dequeue());
}

}   // namespace NCloud::NStorage::NRdma
