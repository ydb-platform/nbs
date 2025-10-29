#include "thread_pool.h"

#include "concurrent_queue.h"
#include "task_queue.h"
#include "thread.h"
#include "thread_park.h"

#include <ydb/library/actors/prof/tag.h>

#include <util/datetime/cputimer.h>
#include <util/generic/scope.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/thread.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TThreadPool final
    : public ITaskQueue
{
    struct TWorker
    {
        enum {
            RUNNING,
            SPINNING,
            SLEEPING,
        };

        Y_CACHE_ALIGNED TAtomic State = RUNNING;
        TThreadPark ThreadPark;

        TString Name;
        std::unique_ptr<ISimpleThread> Thread;
    };

    struct TWorkerThread : ISimpleThread
    {
        TThreadPool& ThreadPool;
        TWorker& Worker;

        TWorkerThread(TThreadPool& threadPool, TWorker& worker)
            : ThreadPool(threadPool)
            , Worker(worker)
        {}

        void* ThreadProc() override
        {
            ThreadPool.Run(Worker);
            return nullptr;
        }
    };

private:
    static constexpr auto SPIN_TIMEOUT = TDuration::MicroSeconds(100);
    static constexpr auto SLEEP_TIMEOUT = TDuration::Seconds(1);

    static constexpr auto LOCK_TAG = Max<ui32>();

    const ui32 NumWorkers;
    const ui32 MaxSpinning;
    const ui64 SpinCycles;
    const TString MemoryTagScope;

    TVector<TWorker> Workers;
    TConcurrentQueue<ITask> Queue;

    Y_CACHE_ALIGNED TAtomic ShouldStop = 0;
    Y_CACHE_ALIGNED TAtomic RunningWorkers = 0;
    Y_CACHE_ALIGNED TAtomic SpinningWorkers = 0;

public:
    TThreadPool(
            const TString& threadName,
            size_t numWorkers,
            TString memoryTagScope)
        : NumWorkers(numWorkers)
        , MaxSpinning(Max<ui32>(1, numWorkers / 4))
        , SpinCycles(DurationToCyclesSafe(SPIN_TIMEOUT))
        , MemoryTagScope(std::move(memoryTagScope))
        , Workers(numWorkers)
    {
        size_t i = 1;
        for (auto& worker: Workers) {
            worker.Name = TStringBuilder() << threadName << i++;
            worker.Thread = std::make_unique<TWorkerThread>(*this, worker);
        }
    }

    ~TThreadPool() override
    {
        Stop();
    }

    void Start() override
    {
        AtomicSet(RunningWorkers, NumWorkers);

        for (auto& worker: Workers) {
            worker.Thread->Start();
        }
    }

    void Stop() override
    {
        AtomicSet(ShouldStop, 1);

        for (auto& worker: Workers) {
            WakeUp(worker);
        }

        for (auto& worker: Workers) {
            worker.Thread.reset();
        }
    }

    void Enqueue(ITaskPtr task) override
    {
        Queue.Enqueue(std::move(task));

        if (AllocateWorker()) {
            WakeUpWorker();
        }
    }

private:
    void Run(TWorker& worker)
    {
        ::NCloud::SetCurrentThreadName(worker.Name);
        NProfiling::TMemoryTagScope tagScope(MemoryTagScope.c_str());

        while (AtomicGet(ShouldStop) == 0) {
            if (auto task = Queue.Dequeue()) {
                task->Execute();
                continue;
            }

            if (ReleaseWorker()) {
                Wait(worker);
            }
        }
    }

    static bool AtomicIncrementWithLimit(TAtomic* p, size_t limit)
    {
        for (;;) {
            size_t v = AtomicGet(*p);
            if (v >= limit) {
                return false;
            }
            if (AtomicCas(p, v + 1, v)) {
                return true;
            }
        }
    }

    bool Wait(TWorker& worker)
    {
        AtomicSet(worker.State, TWorker::SPINNING);

        if (AtomicIncrementWithLimit(&SpinningWorkers, MaxSpinning)) {
            Y_DEFER {
                AtomicDecrement(SpinningWorkers);
            };

            ui64 deadLine = NDateTimeHelpers::SumWithSaturation(
                GetCycleCount(),
                SpinCycles);

            size_t spin = 0;
            for (;;) {
                if (++spin & 31) {
                    SpinLockPause();
                } else if (deadLine < GetCycleCount()) {
                    break;
                }

                if (AtomicGet(worker.State) == TWorker::RUNNING) {
                    return true;
                }
                if (AtomicGet(ShouldStop) != 0) {
                    return false;
                }
            }
        }

        if (AtomicCas(&worker.State, TWorker::SLEEPING, TWorker::SPINNING)) {
            for (;;) {
                worker.ThreadPark.WaitT(SLEEP_TIMEOUT);

                if (AtomicGet(worker.State) == TWorker::RUNNING) {
                    return true;
                }
                if (AtomicGet(ShouldStop) != 0) {
                    return false;
                }
            }
        }

        return true;
    }

    bool AllocateWorker()
    {
#if defined(NEED_MEMORY_BARRIER)
        AtomicBarrier();
#endif
        for (;;) {
            ui32 count = AtomicGet(RunningWorkers);
            if (count != LOCK_TAG) {
                if (count < NumWorkers) {
                    if (AtomicCas(&RunningWorkers, count + 1, count)) {
                        return true;
                    }
                } else {
                    Y_ABORT_UNLESS(count == NumWorkers);
                    return false;
                }
            }
            SpinLockPause();
        }
    }

    bool ReleaseWorker()
    {
#if defined(NEED_MEMORY_BARRIER)
        AtomicBarrier();
#endif
        for (;;) {
            ui32 count = AtomicGet(RunningWorkers);
            if (count != LOCK_TAG) {
                if (count == 1) {
                    // complex detach process for the last worker
                    if (AtomicCas(&RunningWorkers, LOCK_TAG, count)) {
                        if (Queue.IsEmpty()) {
                            AtomicSet(RunningWorkers, 0);
                            return true;
                        } else {
                            AtomicSet(RunningWorkers, 1);
                            return false;
                        }
                    }
                } else {
                    Y_ABORT_UNLESS(count > 1);
                    if (AtomicCas(&RunningWorkers, count - 1, count)) {
                        return true;
                    }
                }
            }
            SpinLockPause();
        }
    }

    void WakeUpWorker()
    {
        // first pass to find spinner - it is hot and can be activated quick
        for (auto& worker: Workers) {
            if (AtomicGet(worker.State) == TWorker::SPINNING) {
                if (WakeUp(worker)) {
                    return;
                }
            }
        }

        // there should be thread waiting for activation - find it
        for (;;) {
            for (auto& worker: Workers) {
                if (WakeUp(worker)) {
                    return;
                }
            }
        }
    }

    static bool WakeUp(TWorker& worker)
    {
        ui32 state = AtomicSwap(&worker.State, TWorker::RUNNING);
        switch (state) {
        case TWorker::RUNNING:
            return false;
        case TWorker::SPINNING:
            return true;
        case TWorker::SLEEPING:
            worker.ThreadPark.Signal();
            return true;
        default:
            Y_ABORT("unknown worker state: %u", state);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLongRunningTaskExecutor final
    : public ITaskQueue
{
    struct TThreadArgs
    {
        const TString ThreadName;
        const ITaskPtr Task;

        TThreadArgs(TString threadName, ITaskPtr task)
            : ThreadName(std::move(threadName))
            , Task(std::move(task))
        {}
    };

private:
    const TString ThreadName;

public:
    explicit TLongRunningTaskExecutor(TString threadName)
        : ThreadName(std::move(threadName))
    {}

    void Start() override
    {
        // nothing to do
    }

    void Stop() override
    {
        // nothing to do
    }

    void Enqueue(ITaskPtr task) override
    {
        TThread thread(ThreadProc, new TThreadArgs(ThreadName, std::move(task)));
        thread.Start();
        thread.Detach();
    }

private:
    static void* ThreadProc(void* data)
    {
        std::unique_ptr<TThreadArgs> args(static_cast<TThreadArgs*>(data));

        ::NCloud::SetCurrentThreadName(args->ThreadName);
        NProfiling::TMemoryTagScope tagScope("STORAGE_THREAD_WORKER");

        args->Task->Execute();
        return nullptr;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITaskQueuePtr CreateThreadPool(
    const TString& threadName,
    size_t numThreads,
    TString memoryTagScope)
{
    return std::make_shared<TThreadPool>(
        threadName,
        numThreads,
        std::move(memoryTagScope));
}

ITaskQueuePtr CreateThreadPool(
    const TString& threadName,
    size_t numThreads)
{
    return std::make_shared<TThreadPool>(
        threadName,
        numThreads,
        "STORAGE_" + threadName);
}

ITaskQueuePtr CreateLongRunningTaskExecutor(const TString& threadName)
{
    return std::make_shared<TLongRunningTaskExecutor>(threadName);
}

}   // namespace NCloud
