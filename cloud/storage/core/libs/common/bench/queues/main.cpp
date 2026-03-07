#include <library/cpp/testing/benchmark/bench.h>

#include <util/generic/deque.h>
#include <util/generic/vector.h>
#include <util/random/fast.h>
#include <util/system/event.h>
#include <util/system/mutex.h>
#include <util/system/spinlock.h>
#include <util/thread/lfstack.h>
#include <util/thread/lfqueue.h>
#include <util/thread/factory.h>
#include <thread>

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TQueue>
void RunBench(TQueue& q, ui64 iters, ui32 producers, ui32 consumers)
{
    struct TContext
    {
        TManualEvent ThreadFinished;
        std::atomic<ui32> Producers;
        std::atomic<ui32> Consumers;

        TManualEvent CanStartProducers;
        std::atomic<ui32> ReadyConsumers;
    };

    auto context = std::make_shared<TContext>();
    context->Producers = producers;
    context->Consumers = consumers;

    for (ui32 i = 0; i < producers; ++i) {
        SystemThreadFactory()->Run(
            [&q, iters, producers, context]()
            {
                context->CanStartProducers.WaitI();
                for (size_t i = 0; i < iters / producers; ++i) {
                    q.Enqueue(i);
                }

                context->Producers.fetch_sub(1, std::memory_order_release);
                context->ThreadFinished.Signal();
            });
    }

    for (ui32 i = 0; i < consumers; ++i) {
        SystemThreadFactory()->Run(
            [&q, context]()
            {
                context->ReadyConsumers.fetch_add(1, std::memory_order_relaxed);
                while (true) {
                    if (!q.Dequeue() &&
                        !context->Producers.load(std::memory_order_acquire))
                    {
                        if (!q.Dequeue()) {
                            break;
                        }
                    }
                }

                context->Consumers.fetch_sub(1, std::memory_order_release);
                context->ThreadFinished.Signal();
            });
    }

    // Wait for all consumers to start consuming
    while (context->ReadyConsumers.load(std::memory_order_relaxed) < consumers)
    {
        std::this_thread::yield();
    }
    // Signal consumers to start consuming
    context->CanStartProducers.Signal();

    while (context->Producers.load(std::memory_order_acquire) ||
           context->Consumers.load(std::memory_order_acquire))
    {
        context->ThreadFinished.WaitI();
        context->ThreadFinished.Reset();
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TLFStackSingleConsumerDequeueAll
{
    TLockFreeStack<ui64> Data;
    TVector<ui64> Dequeued;

    void Enqueue(ui64 x)
    {
        Data.Enqueue(x);
    }

    bool Dequeue()
    {
        Data.DequeueAllSingleConsumer(&Dequeued);
        const bool haveData = Dequeued.size() > 0;
        Dequeued.clear();
        return haveData;
    }
};

struct TLFStackDequeueAll
{
    TLockFreeStack<ui64> Data;
    TVector<ui64> Dequeued;

    void Enqueue(ui64 x)
    {
        Data.Enqueue(x);
    }

    bool Dequeue()
    {
        Data.DequeueAll(&Dequeued);
        const bool haveData = Dequeued.size() > 0;
        Dequeued.clear();
        return haveData;
    }
};

struct TLFQueue
{
    TLockFreeQueue<ui64> Data;

    void Enqueue(ui64 x)
    {
        Data.Enqueue(x);
    }

    bool Dequeue()
    {
        ui64 x;
        return Data.Dequeue(&x);
    }
};

struct TLFQueueDequeueAll
{
    TLockFreeQueue<ui64> Data;
    TVector<ui64> Dequeued;

    void Enqueue(ui64 x)
    {
        Data.Enqueue(x);
    }

    bool Dequeue()
    {
        Data.DequeueAll(&Dequeued);
        const bool haveData = Dequeued.size() > 0;
        Dequeued.clear();
        return haveData;
    }
};

struct TDequeWithMutex
{
    TDeque<ui64> Data;
    TMutex Lock;

    void Enqueue(ui64 x)
    {
        auto g = Guard(Lock);
        Data.push_back(x);
    }

    bool Dequeue()
    {
        auto g = Guard(Lock);
        if (Data.size()) {
            Data.pop_front();
            return true;
        }

        return false;
    }
};

struct TDequeWithAdaptiveLock
{
    TDeque<ui64> Data;
    TAdaptiveLock Lock;

    void Enqueue(ui64 x)
    {
        auto g = Guard(Lock);
        Data.push_back(x);
    }

    bool Dequeue()
    {
        auto g = Guard(Lock);
        if (Data.size()) {
            Data.pop_front();
            return true;
        }

        return false;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define Q_BENCH(TQueue, producers, consumers)                                  \
    Y_CPU_BENCHMARK(TQueue##_##producers##_##consumers, iface)                 \
    {                                                                          \
        TQueue q;                                                              \
        RunBench(q, iface.Iterations(), producers, consumers);                 \
    }                                                                          \
// Q_BENCH

#define Q_BENCH_SET(TQueue, consumers)                                         \
    Q_BENCH(TQueue, 1, consumers)                                              \
    Q_BENCH(TQueue, 2, consumers)                                              \
    Q_BENCH(TQueue, 4, consumers)                                              \
    Q_BENCH(TQueue, 8, consumers)                                              \
    Q_BENCH(TQueue, 16, consumers)                                             \
// Q_BENCH_SET

//
// Non-DequeueAll LFStack versions are not tested because it's not possible to
// implement queue-like behaviour on top of them.
//
// With DequeueAll the supposed logic looks something like this:
// TVector<TItem> items;
// q.DequeueAll(items);
// for (auto it = items.rbegin(); it != items.rend(); ++it) {
//     ProcessItem(*it);
// }
//

Q_BENCH_SET(TLFStackSingleConsumerDequeueAll, 1)
Q_BENCH_SET(TLFStackDequeueAll, 1)
Q_BENCH_SET(TLFQueue, 1)
Q_BENCH_SET(TLFQueueDequeueAll, 1)
Q_BENCH_SET(TLFQueue, 16)
Q_BENCH_SET(TDequeWithMutex, 1)
Q_BENCH_SET(TDequeWithAdaptiveLock, 1)
Q_BENCH_SET(TDequeWithMutex, 16)
Q_BENCH_SET(TDequeWithAdaptiveLock, 16)
