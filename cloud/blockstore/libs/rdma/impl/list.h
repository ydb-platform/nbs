#pragma once

#include "public.h"

#include <library/cpp/deprecated/atomic/atomic.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TListNode
{
    T* Next = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void DeleteList(T* node)
{
    while (node) {
        T* next = node->Next;
        delete node;
        node = next;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TSimpleList
{
private:
    T* Head;
    T* Tail;

public:
    TSimpleList(T* head = nullptr, T* tail = nullptr)
        : Head(head)
        , Tail(tail)
    {}

    ~TSimpleList()
    {
        DeleteList(Head);
    }

    operator bool() const
    {
        return Head;
    }

    std::unique_ptr<T> Dequeue()
    {
        std::unique_ptr<T> node(Head);
        if (node) {
            T* next = node->Next;
            if (next) {
                Head = next;
            } else {
                // list is empty now
                Head = Tail = nullptr;
            }
        }
        return node;
    }

    void Enqueue(std::unique_ptr<T> node)
    {
        Append(node.get(), node.get());

        // ownership transferred
        node.release();
    }

    void Append(TSimpleList&& list)
    {
        Append(list.Head, list.Tail);

        // ownership transferred
        list.Head = list.Tail = nullptr;
    }

private:
    void Append(T* head, T* tail)
    {
        if (Tail) {
            Tail->Next = head;
            Tail = tail;
        } else {
            Head = head;
            Tail = tail;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TLockFreeList
{
private:
    T* Head = nullptr;

public:
    ~TLockFreeList()
    {
        DeleteList(Head);
    }

    void Enqueue(std::unique_ptr<T> node)
    {
        // ABA is not a problem in our case
        do {
            node->Next = AtomicGet(Head);
        } while (!AtomicCas(&Head, node.get(), node->Next));

        // ownership transferred
        node.release();
    }

    TSimpleList<T> DequeueAll()
    {
        T* node = AtomicSwap(&Head, nullptr);

        // reverse list to restore original order
        T* head = nullptr;
        T* tail = node;
        while (node) {
            T* next = node->Next;
            node->Next = head;
            head = node;
            node = next;
        }

        return { head, tail };
    }
};

}   // namespace NCloud::NBlockStore::NRdma
