#include "thread_pool.h"

////////////////////////////////////////////////////////////////////////////////

TNaiveThreadPool::TNaiveThreadPool(size_t threadCount)
{
    Threads.reserve(threadCount);
}

void TNaiveThreadPool::Start()
{
    const size_t n = Threads.capacity();
    for (size_t i = 0; i != n; ++i) {
        Threads.emplace_back(&TNaiveThreadPool::ThreadFunc, this);
    }
}

void TNaiveThreadPool::Stop()
{
    {
        std::unique_lock lock{Mutex};

        std::generate_n(
            std::back_inserter(Queue),
            Threads.size(),
            [] { return NCloud::ITaskPtr(); });

        NotEmpty.notify_all();
    }

    for (auto& t: Threads) {
        t.join();
    }
}

void TNaiveThreadPool::ThreadFunc()
{
    for (;;) {
        NCloud::ITaskPtr task;
        {
            std::unique_lock lock{Mutex};
            if (Queue.empty()) {
                NotEmpty.wait(lock);
            }

            if (Queue.empty()) {
                continue;
            }

            task = std::move(Queue.back());
            Queue.pop_back();
        }

        if (!task) {
            break;
        }
        task->Execute();
    }
}

void TNaiveThreadPool::Enqueue(NCloud::ITaskPtr task)
{
    std::unique_lock lock{Mutex};
    Queue.push_front(std::move(task));
    NotEmpty.notify_one();
}
