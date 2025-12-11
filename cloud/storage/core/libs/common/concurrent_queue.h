#pragma once

#include "public.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/utility.h>
#include <util/system/spinlock.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TConcurrentQueue
{
    struct TSegment;

    struct TSegmentHeader
    {
        // reader postion
        Y_CACHE_ALIGNED size_t ReadPos;
        // writer position
        Y_CACHE_ALIGNED size_t WritePos;
        // linked list of live segments
        TSegment* Next;
        // linked list of detached segments
        TSegment* NextFree;
    };

    static constexpr size_t SegmentCapacity =
        (PLATFORM_PAGE_SIZE - sizeof(TSegmentHeader)) / sizeof(T*);

    struct TSegment: TSegmentHeader
    {
        // pointer could be published atomically,
        // but if you want to store more complex data -
        // you will need an additional atomic flag to act as a barrier
        T* Ptrs[SegmentCapacity];

        TSegment()
        {
            Zero(*this);
        }
    };

    static_assert(sizeof(TSegment) == PLATFORM_PAGE_SIZE);

    struct TOperationScope
    {
        TConcurrentQueue* Queue;
        TSegment* FreeList = nullptr;

        TOperationScope(TConcurrentQueue* queue)
            : Queue(queue)
        {
            Queue->EnterScope();
        }

        ~TOperationScope()
        {
            Queue->ExitScope(*this);
        }

        void Enqueue(TSegment* segment)
        {
            segment->NextFree = FreeList;
            FreeList = segment;
        }
    };

private:
    Y_CACHE_ALIGNED TSegment* Head;
    Y_CACHE_ALIGNED TSegment* Tail;

    // TODO: it is much better to use hazard-pointers for GC
    Y_CACHE_ALIGNED size_t ActiveThreads = 0;
    Y_CACHE_ALIGNED TSegment* FreeList = nullptr;

public:
    TConcurrentQueue()
    {
        Head = Tail = new TSegment();
    }

    ~TConcurrentQueue()
    {
        Clear();
        DeleteSegments(FreeList);
        delete Head;
    }

    bool IsEmpty()
    {
        TOperationScope scope(this);

        auto* head = AtomicGet(Head);
        auto* tail = AtomicGet(Tail);
        if (head != tail) {
            return false;
        }

        size_t readPos = AtomicGet(head->ReadPos);
        size_t writePos = AtomicGet(head->WritePos);
        if (readPos < Min(writePos, SegmentCapacity)) {
            return false;
        }

        return true;
    }

    void Enqueue(std::unique_ptr<T> item)
    {
        TOperationScope scope(this);

        auto* tail = AtomicGet(Tail);
        for (;;) {
            // advance the writer position (it is faster without CAS)
            size_t writePos = AtomicGetAndIncrement(tail->WritePos);
            if (writePos < SegmentCapacity) {
                // ... and then publish the pointer
                AtomicSet(tail->Ptrs[writePos], item.release());
                return;
            }
            // switch segments
            tail = SwitchTail(tail);
        }
    }

    std::unique_ptr<T> Dequeue()
    {
        TOperationScope scope(this);

        auto* head = AtomicGet(Head);
        while (head) {
            size_t readPos = AtomicGet(head->ReadPos);
            size_t writePos = AtomicGet(head->WritePos);
            if (readPos < Min(writePos, SegmentCapacity)) {
                // TODO. Investigate whether this prefetch optimization is
                // required.
                // Y_PREFETCH_READ(head->Ptrs[readPos], 3);
                // advance the reader position
                if (AtomicCas(&head->ReadPos, readPos + 1, readPos)) {
                    // ... and then wait for the pointer published by the writer
                    for (;;) {
                        auto* item = AtomicGet(head->Ptrs[readPos]);
                        if (item) {
                            return std::unique_ptr<T>(item);
                        }
                        SpinLockPause();
                    }
                }
                // contention detected
                SpinLockPause();
            } else {
                if (readPos < SegmentCapacity) {
                    // there are no items
                    break;
                }
                // switch segments
                head = SwitchHead(head, scope);
            }
        }

        return {};
    }

    void Clear()
    {
        while (Dequeue()) {
            // just throw all items away
        }
    }

private:
    TSegment* SwitchTail(TSegment* prev)
    {
        auto segment = std::make_unique<TSegment>();
        for (;;) {
            // check the tail - it could be switched by the concurrent writer
            auto* tail = AtomicGet(Tail);
            if (Y_UNLIKELY(tail != prev)) {
                return tail;
            }
            // ... and then check for the next segment - it could be published
            // already
            auto* next = AtomicGet(tail->Next);
            if (Y_UNLIKELY(next)) {
                return next;
            }
            // switch the tail
            if (AtomicCas(&Tail, segment.get(), tail)) {
                // ... and then publish the next segment for the reader
                AtomicSet(tail->Next, segment.get());
                return segment.release();
            }
            // contention detected
            SpinLockPause();
        }
    }

    TSegment* SwitchHead(TSegment* prev, TOperationScope& scope)
    {
        for (;;) {
            // check the head - it could be switched by the concurrent reader
            auto* head = AtomicGet(Head);
            if (Y_UNLIKELY(head != prev)) {
                return head;
            }
            // ... and then check for the next segment
            auto* next = AtomicGet(head->Next);
            if (Y_UNLIKELY(!next)) {
                return nullptr;
            }
            // switch the head
            if (AtomicCas(&Head, next, head)) {
                // keep track of detached segments
                scope.Enqueue(head);
                return next;
            }
            // contention detected
            SpinLockPause();
        }
    }

    void EnterScope()
    {
        // in this simple scheme this atomic counter acts as a barrier
        // for garbage collection - we can only delete segments if the are no
        // concurrent threads running
        AtomicIncrement(ActiveThreads);
    }

    void ExitScope(TOperationScope& scope)
    {
        // catch the free-list head
        auto* head = AtomicGet(FreeList);

        if (AtomicDecrement(ActiveThreads) == 0) {
            // there were no other threads running at the moment we catch the
            // free-list head use CAS to grab the head AND ensure no segments
            // removed since then
            if (head && AtomicCas(&FreeList, nullptr, head)) {
                DeleteSegments(head);
            }
            DeleteSegments(scope.FreeList);
        } else {
            // enqueue segments for later deletion
            if (scope.FreeList) {
                AsyncDeleteSegments(scope.FreeList);
            }
        }
    }

    void AsyncDeleteSegments(TSegment* head)
    {
        auto* tail = head;
        while (tail->NextFree) {
            tail = tail->NextFree;
        }

        for (;;) {
            tail->NextFree = AtomicGet(FreeList);
            if (AtomicCas(&FreeList, head, tail->NextFree)) {
                return;
            }
            SpinLockPause();
        }
    }

    static void DeleteSegments(TSegment* head)
    {
        while (head) {
            auto* next = head->NextFree;
            delete head;
            head = next;
        }
    }
};

}   // namespace NCloud
