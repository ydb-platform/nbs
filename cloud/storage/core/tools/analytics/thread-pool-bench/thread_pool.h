#pragma once

#include <cloud/storage/core/libs/common/task_queue.h>

#include <util/generic/deque.h>
#include <util/generic/vector.h>

#include <condition_variable>
#include <mutex>
#include <thread>

////////////////////////////////////////////////////////////////////////////////

class TNaiveThreadPool
    : public NCloud::ITaskQueue
{
private:
    std::mutex Mutex;
    std::condition_variable NotEmpty;

    TDeque<NCloud::ITaskPtr> Queue;
    TVector<std::thread> Threads;

public:
    explicit TNaiveThreadPool(size_t threadCount);

    void Start() override;
    void Stop() override;

    void Enqueue(NCloud::ITaskPtr task) override;

private:
    void ThreadFunc();
};
