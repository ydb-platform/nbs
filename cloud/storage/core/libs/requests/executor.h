#pragma once

#include "request.h"

#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/executor_counters.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <contrib/libs/grpc/include/grpcpp/completion_queue.h>
#include <contrib/libs/grpc/include/grpcpp/server.h>

#include <library/cpp/actors/prof/tag.h>

#include <util/generic/maybe.h>
#include <util/string/builder.h>
#include <util/system/thread.h>

namespace NCloud::NStorage::NRequests {

namespace NInternal {

////////////////////////////////////////////////////////////////////////////////

struct TDummyExecutorScope
    {
    int StartWait()
    {
        return {};
    }

    int StartExecute()
    {
        return {};
    }
};

}   // namespace NInternal

////////////////////////////////////////////////////////////////////////////////

template <typename TCompletionQueue, typename TRequestsInFlight>
struct TExecutorContext
{
    std::unique_ptr<TCompletionQueue> CompletionQueue;
    TRequestsInFlight RequestsInFlight;

    std::atomic_bool ShutdownCalled = false;
};

template <
    typename TCompletionQueue,
    typename TRequestsInFlight,
    typename TExecutorScope = NInternal::TDummyExecutorScope>
class TExecutor final
    : public ISimpleThread
    , public TExecutorContext<TCompletionQueue, TRequestsInFlight>
{
private:
    TString Name;
    TLog Log;
    TExecutorScope ExecutorScope;

public:
    TExecutor(
            TString name,
            std::unique_ptr<TCompletionQueue> completionQueue,
            TLog log,
            TExecutorScope executorScope = {})
        : Name(std::move(name))
        , Log(std::move(log))
        , ExecutorScope(std::move(executorScope))
    {
        this->CompletionQueue = std::move(completionQueue);
    }

    void Shutdown()
    {
        this->ShutdownCalled.store(true);

        STORAGE_TRACE(Name << " executor's shutdown() started");

        if (this->CompletionQueue) {
            this->CompletionQueue->Shutdown();
        }

        Join();

        this->CompletionQueue.reset();
    }

private:
    void* ThreadProc() override
    {
        ::NCloud::SetCurrentThreadName(Name);

        auto tagName = TStringBuilder() << "NFS_THREAD_" << Name;
        NProfiling::TMemoryTagScope tagScope(tagName.c_str());

        void* tag;
        bool ok;
        while (WaitRequest(&tag, &ok)) {
            ProcessRequest(tag, ok);
        }

        return nullptr;
    }

    bool WaitRequest(void** tag, bool* ok)
    {
        [[maybe_unused]] auto activity = ExecutorScope.StartWait();

        return this->CompletionQueue->Next(tag, ok);
    }

    void ProcessRequest(void* tag, bool ok)
    {
        [[maybe_unused]] auto activity = ExecutorScope.StartExecute();

        TRequestHandlerPtr handler(static_cast<TRequestHandlerBase*>(tag));

        if (!this->ShutdownCalled.load()) {
            handler->Process(ok);
        } else {
            // once Shutdown() is being called, no new requests can be processed
            // However, we still must drain completion queue and release
            // handlers, that is why we are not breaking out of the loop
            STORAGE_TRACE(
                Name << " executor's shutdown() is being called"
                        ", new requests are being dropped");
        }

        if (!handler->ReleaseCompletionTag()) {
            // ownership transferred to CompletionQueue
            Y_UNUSED(handler.release());
        }
    }
};

}   // namespace NCloud::NStorage::NRequests
