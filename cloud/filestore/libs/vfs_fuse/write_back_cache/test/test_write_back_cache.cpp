#include "test_write_back_cache.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>

#include <library/cpp/threading/future/async.h>

#include <util/thread/pool.h>

#include <exception>
#include <utility>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TTask>
void AddOrExecute(IThreadPool& threadPool, TTask task) noexcept
{
    try {
        // Pass a copy so that the task is still available if the pool rejects
        // it while shutting down.
        if (threadPool.AddFunc(task)) {
            return;
        }
    } catch (...) {
        // A stopped pool may reject work by throwing instead of returning
        // false. Complete it inline in either case.
    }

    task();
}

template <typename TCallable>
auto AsyncOrExecute(TCallable&& callable, IThreadPool& threadPool)
{
    auto promise = NewPromise<TFutureType<TFunctionResult<TCallable>>>();
    auto task =
        [promise,
         callable = std::forward<TCallable>(callable)]() mutable noexcept
    {
        try {
            NThreading::NImpl::SetValue(promise, callable);
        } catch (...) {
            promise.TrySetException(std::current_exception());
        }
    };

    AddOrExecute(threadPool, std::move(task));
    return promise.GetFuture();
}

////////////////////////////////////////////////////////////////////////////////

class TAsyncFileStore final: public IFileStore
{
private:
    const IFileStorePtr FileStore;
    IThreadPool& ThreadPool;

public:
    TAsyncFileStore(IFileStorePtr fileStore, IThreadPool& threadPool)
        : FileStore(std::move(fileStore))
        , ThreadPool(threadPool)
    {}

#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return AsyncOrExecute(                                                 \
            [fileStore = FileStore,                                            \
             callContext = std::move(callContext),                             \
             request = std::move(request)]() mutable                           \
            {                                                                  \
                return fileStore->name(                                        \
                    std::move(callContext),                                    \
                    std::move(request));                                       \
            },                                                                 \
            ThreadPool);                                                       \
    }                                                                          \
    // FILESTORE_IMPLEMENT_METHOD

    FILESTORE_DATA_SERVICE(FILESTORE_IMPLEMENT_METHOD)

#undef FILESTORE_IMPLEMENT_METHOD
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TTestWriteBackCache::TImpl
{
private:
    const bool AsyncExecution;

    TThreadPool ThreadPool{TThreadPoolParams("WBCTest")};

    TWriteBackCache Cache;

    // Complete futures returned by WriteBackCache asynchronously to mimic
    // the real completion queue.
    template <typename T>
    TFuture<T> CompleteAsync(const TFuture<T>& future)
    {
        auto promise = NewPromise<T>();
        future.Subscribe(
            [this, promise](TFuture<T> completed) mutable
            {
                AddOrExecute(
                    ThreadPool,
                    [promise = std::move(promise),
                     completed = std::move(completed)]() mutable
                    {
                        try {
                            promise.TrySetValue(completed.ExtractValue());
                        } catch (...) {
                            promise.TrySetException(std::current_exception());
                        }
                    });
            });
        return promise.GetFuture();
    }

    template <typename TCallable>
    auto Execute(TCallable&& callable)
    {
        if (!AsyncExecution) {
            return callable();
        }

        return CompleteAsync(AsyncOrExecute(
            std::forward<TCallable>(callable),
            ThreadPool));
    }

public:
    TImpl(TWriteBackCacheArgs args, size_t threadCount)
        : AsyncExecution(threadCount != 0)
    {
        if (AsyncExecution) {
            ThreadPool.Start(threadCount);

            args.Session = std::make_shared<TAsyncFileStore>(
                std::move(args.Session),
                ThreadPool);
        }

        Cache = TWriteBackCache(std::move(args));
    }

    ~TImpl()
    {
        ThreadPool.Stop();
    }

    TFuture<NProto::TError> Drain()
    {
        return Execute([this] { return Cache.Drain(); });
    }

    bool IsDrained() const
    {
        return Cache.IsDrained();
    }

    TFuture<NProto::TReadDataResponse> ReadData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request)
    {
        return Execute(
            [this,
             callContext = std::move(callContext),
             request = std::move(request)]() mutable
            {
                return Cache.ReadData(
                    std::move(callContext),
                    std::move(request));
            });
    }

    TFuture<NProto::TWriteDataResponse> WriteData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request)
    {
        return Execute(
            [this,
             callContext = std::move(callContext),
             request = std::move(request)]() mutable
            {
                return Cache.WriteData(
                    std::move(callContext),
                    std::move(request));
            });
    }

    TFuture<NProto::TError> FlushNodeData(ui64 nodeId)
    {
        return Execute([this, nodeId] { return Cache.FlushNodeData(nodeId); });
    }

    TFuture<NProto::TError> FlushAllData()
    {
        return Execute([this] { return Cache.FlushAllData(); });
    }

    TFuture<NProto::TError> ReleaseHandle(ui64 nodeId, ui64 handle)
    {
        return Execute([this, nodeId, handle]
                       { return Cache.ReleaseHandle(nodeId, handle); });
    }

    TFuture<NProto::TReadDataResponse> ReadDataDirect(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request)
    {
        return Execute(
            [this,
             callContext = std::move(callContext),
             request = std::move(request)]() mutable
            {
                return Cache.ReadDataDirect(
                    std::move(callContext),
                    std::move(request));
            });
    }

    TFuture<NProto::TWriteDataResponse> WriteDataDirect(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request)
    {
        return Execute(
            [this,
             callContext = std::move(callContext),
             request = std::move(request)]() mutable
            {
                return Cache.WriteDataDirect(
                    std::move(callContext),
                    std::move(request));
            });
    }

    TFuture<NProto::TSetNodeAttrResponse> SetNodeAttr(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TSetNodeAttrRequest> request)
    {
        return Execute(
            [this,
             callContext = std::move(callContext),
             request = std::move(request)]() mutable
            {
                return Cache.SetNodeAttr(
                    std::move(callContext),
                    std::move(request));
            });
    }

    TFuture<NProto::TCreateHandleResponse> CreateHandle(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TCreateHandleRequest> request)
    {
        return Execute(
            [this,
             callContext = std::move(callContext),
             request = std::move(request)]() mutable
            {
                return Cache.CreateHandle(
                    std::move(callContext),
                    std::move(request));
            });
    }

    ui64 GetMaxWrittenOffset(ui64 nodeId) const
    {
        return Cache.GetMaxWrittenOffset(nodeId);
    }

    IModuleStatsPtr CreateModuleStats() const
    {
        return Cache.CreateModuleStats();
    }
};

////////////////////////////////////////////////////////////////////////////////

TTestWriteBackCache::TTestWriteBackCache() = default;

TTestWriteBackCache::TTestWriteBackCache(
    TWriteBackCacheArgs args,
    size_t threadCount)
    : Impl(std::make_unique<TImpl>(std::move(args), threadCount))
{}

TTestWriteBackCache::~TTestWriteBackCache() = default;

TTestWriteBackCache::TTestWriteBackCache(
    TTestWriteBackCache&&) noexcept = default;

TTestWriteBackCache& TTestWriteBackCache::operator=(
    TTestWriteBackCache&&) noexcept = default;

TFuture<NProto::TError> TTestWriteBackCache::Drain()
{
    return Impl->Drain();
}

bool TTestWriteBackCache::IsDrained() const
{
    return Impl->IsDrained();
}

TFuture<NProto::TReadDataResponse> TTestWriteBackCache::ReadData(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadDataRequest> request)
{
    return Impl->ReadData(std::move(callContext), std::move(request));
}

TFuture<NProto::TWriteDataResponse> TTestWriteBackCache::WriteData(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteDataRequest> request)
{
    return Impl->WriteData(std::move(callContext), std::move(request));
}

TFuture<NProto::TError> TTestWriteBackCache::FlushNodeData(ui64 nodeId)
{
    return Impl->FlushNodeData(nodeId);
}

TFuture<NProto::TError> TTestWriteBackCache::FlushAllData()
{
    return Impl->FlushAllData();
}

TFuture<NProto::TError> TTestWriteBackCache::ReleaseHandle(
    ui64 nodeId,
    ui64 handle)
{
    return Impl->ReleaseHandle(nodeId, handle);
}

TFuture<NProto::TReadDataResponse> TTestWriteBackCache::ReadDataDirect(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadDataRequest> request)
{
    return Impl->ReadDataDirect(std::move(callContext), std::move(request));
}

TFuture<NProto::TWriteDataResponse> TTestWriteBackCache::WriteDataDirect(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteDataRequest> request)
{
    return Impl->WriteDataDirect(std::move(callContext), std::move(request));
}

TFuture<NProto::TSetNodeAttrResponse> TTestWriteBackCache::SetNodeAttr(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TSetNodeAttrRequest> request)
{
    return Impl->SetNodeAttr(std::move(callContext), std::move(request));
}

TFuture<NProto::TCreateHandleResponse> TTestWriteBackCache::CreateHandle(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TCreateHandleRequest> request)
{
    return Impl->CreateHandle(std::move(callContext), std::move(request));
}

ui64 TTestWriteBackCache::GetMaxWrittenOffset(ui64 nodeId) const
{
    return Impl->GetMaxWrittenOffset(nodeId);
}

IModuleStatsPtr TTestWriteBackCache::CreateModuleStats() const
{
    return Impl->CreateModuleStats();
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
