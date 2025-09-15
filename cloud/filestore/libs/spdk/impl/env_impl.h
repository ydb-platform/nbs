#pragma once

#include "public.h"

#include "alloc.h"
#include "env.h"
#include "spdk.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/task_queue.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NFileStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

class TSpdkEnv final
    : public ISpdkEnv
    , public ITaskQueue
    , public std::enable_shared_from_this<TSpdkEnv>
{
    friend class TSpdkDevice;

private:
    class TMainThread;
    std::unique_ptr<TMainThread> MainThread;

public:
    explicit TSpdkEnv(TSpdkEnvConfigPtr config);
    ~TSpdkEnv();

    //
    // IStartable
    //

    void Start() override;
    void Stop() override;

    NThreading::TFuture<void> StartAsync() override;
    NThreading::TFuture<void> StopAsync() override;

    //
    // ITaskQueue
    //

    void Enqueue(ITaskPtr task) override;

    //
    // Memory
    //

    IAllocator* GetAllocator() override
    {
        return GetHugePageAllocator();
    }

private:
    int PickCore();
    ITaskQueue* GetTaskQueue(int index = -1);
};

////////////////////////////////////////////////////////////////////////////////

using TSpdkEnvPtr = std::shared_ptr<TSpdkEnv>;

}   // namespace NCloud::NFileStore::NSpdk
