#include "service_null.h"

#include "context.h"
#include "service.h"

#include <cloud/blockstore/config/server.pb.h>
#include <cloud/blockstore/libs/service/service_method.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/thread.h>

#include <util/generic/guid.h>
#include <util/generic/map.h>
#include <util/generic/stack.h>
#include <util/generic/vector.h>
#include <util/system/event.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>
#include <util/thread/lfstack.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct IRequestHandler
{
    virtual ~IRequestHandler() = default;

    virtual void Execute() = 0;
};

using IRequestHandlerPtr = std::shared_ptr<IRequestHandler>;

////////////////////////////////////////////////////////////////////////////////

struct TAppContext
{
    NProto::TNullServiceConfig Config;

    TMutex Lock;
    TMap<TString, NProto::TMountVolumeResponse> MountedDisks;
};

////////////////////////////////////////////////////////////////////////////////

class TExecutor final: public ISimpleThread
{
private:
    static constexpr TDuration Timeout = TDuration::Seconds(1);

    TAppContext& AppCtx;
    const TString Name;

    TDeque<TInstant> ExecutedRequests;
    TLockFreeStack<IRequestHandlerPtr> LFPostponedRequests;
    TStack<IRequestHandlerPtr> PostponedRequests;
    TManualEvent Event;

    TAtomic ShouldStop = 0;

public:
    TExecutor(TAppContext& appCtx, TString name)
        : AppCtx(appCtx)
        , Name(std::move(name))
    {}

    void AddRequestHandler(IRequestHandlerPtr handler)
    {
        LFPostponedRequests.Enqueue(std::move(handler));
        Event.Signal();
    }

    void Stop()
    {
        if (AtomicSwap(&ShouldStop, 1) == 1) {
            return;
        }

        Join();
    }

private:
    void* ThreadProc() override
    {
        ::NCloud::SetCurrentThreadName(Name);

        while (!AtomicGet(ShouldStop)) {
            if (!PostponedRequestExists()) {
                if (Event.WaitT(Timeout)) {
                    Event.Reset();
                } else {
                    continue;
                }
            }

            if (IopsLimitReached()) {
                continue;
            }

            IRequestHandlerPtr handler;
            if (GetPostponedRequest(&handler)) {
                handler->Execute();
                ExecutedRequests.push_back(TInstant::Now());
            }
        }

        return nullptr;
    }

    bool PostponedRequestExists()
    {
        return !PostponedRequests.empty() || !LFPostponedRequests.IsEmpty();
    }

    bool GetPostponedRequest(IRequestHandlerPtr* handler)
    {
        if (PostponedRequests.empty()) {
            TVector<IRequestHandlerPtr> requests;
            LFPostponedRequests.DequeueAllSingleConsumer(&requests);

            for (auto& request: requests) {
                PostponedRequests.push(std::move(request));
            }

            if (PostponedRequests.empty()) {
                return false;
            }
        }

        *handler = std::move(PostponedRequests.top());
        PostponedRequests.pop();
        return true;
    }

    bool IopsLimitReached()
    {
        if (AppCtx.Config.GetIopsLimit() == 0) {
            ExecutedRequests.clear();
            return false;
        }

        auto now = TInstant::Now();

        while (!ExecutedRequests.empty() &&
               now - ExecutedRequests.front() > TDuration::Seconds(1))
        {
            ExecutedRequests.pop_front();
        }

        return ExecutedRequests.size() >= AppCtx.Config.GetIopsLimit();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest, typename TResponse>
class TRequestHandler final: public IRequestHandler
{
private:
    TAppContext& AppCtx;
    TCallContextPtr CallContext;
    std::shared_ptr<TRequest> Request;
    TPromise<TResponse> Promise;

public:
    TRequestHandler(
        TAppContext& appCtx,
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request,
        TPromise<TResponse> promise)
        : AppCtx(appCtx)
        , CallContext(std::move(callContext))
        , Request(std::move(request))
        , Promise(std::move(promise))
    {}

    void Execute() override
    {
        TResponse response;
        HandleRequest(std::move(CallContext), Request, response);

        // Emulate Kikimr behavior
        Request->Clear();

        Promise.SetValue(response);
    }

private:
    ui32 GetBlockSize() const
    {
        const auto blockSize = AppCtx.Config.GetDiskBlockSize();
        return blockSize ? blockSize : DefaultBlockSize;
    }

    ui64 GetBlocksCount() const
    {
        static constexpr ui64 DefaultBlocksCount = 1024 * 1024;

        const auto blocksCount = AppCtx.Config.GetDiskBlocksCount();
        return blocksCount ? blocksCount : DefaultBlocksCount;
    }

    template <typename TRequest_, typename TResponse_>
    void HandleRequest(
        TCallContextPtr ctx,
        std::shared_ptr<TRequest_> request,
        TResponse_& response)
    {
        Y_UNUSED(ctx);
        Y_UNUSED(request);
        Y_UNUSED(response);
    }

    void HandleRequest(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TReadBlocksRequest> request,
        NProto::TReadBlocksResponse& response)
    {
        Y_UNUSED(ctx);

        const auto blockSize = GetBlockSize();

        // simulate zero response
        auto& buffers = *response.MutableBlocks()->MutableBuffers();
        for (size_t i = 0; i < request->GetBlocksCount(); ++i) {
            auto* buf = buffers.Add();

            if (AppCtx.Config.GetAddReadResponseData()) {
                buf->ReserveAndResize(blockSize);
                memset((void*)buf->data(), 0, buf->size());
            }
        }
    }

    void HandleRequest(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request,
        NProto::TReadBlocksLocalResponse& response)
    {
        Y_UNUSED(ctx);

        if (!AppCtx.Config.GetAddReadResponseData()) {
            return;
        }

        const auto blockSize = GetBlockSize();
        const auto blocksCount = request->GetBlocksCount();
        size_t responseSize = blockSize * blocksCount;

        auto guard = request->Sglist.Acquire();
        if (!guard) {
            response = TErrorResponse(
                E_CANCELLED,
                "failed to acquire sglist in NullService");
            return;
        }

        // simulate zero response
        for (const auto& buf: guard.Get()) {
            if (responseSize == 0) {
                break;
            }

            auto size = std::min(buf.Size(), responseSize);
            memset((void*)buf.Data(), 0, size);
            responseSize -= size;
        }
    }

    void HandleRequest(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TDescribeVolumeRequest> request,
        NProto::TDescribeVolumeResponse& response)
    {
        Y_UNUSED(ctx);

        const auto diskId = request->GetDiskId();
        const auto blockSize = GetBlockSize();
        const auto blocksCount = GetBlocksCount();

        auto& volume = *response.MutableVolume();
        volume.SetDiskId(diskId);
        volume.SetBlockSize(blockSize);
        volume.SetBlocksCount(blocksCount);
    }

    void HandleRequest(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TMountVolumeRequest> request,
        NProto::TMountVolumeResponse& response)
    {
        Y_UNUSED(ctx);

        const auto diskId = request->GetDiskId();
        const auto blockSize = GetBlockSize();
        const auto blocksCount = GetBlocksCount();

        with_lock (AppCtx.Lock) {
            auto it = AppCtx.MountedDisks.find(diskId);
            if (it == AppCtx.MountedDisks.end()) {
                // simulate response
                NProto::TMountVolumeResponse mountResponse;
                mountResponse.SetSessionId(CreateGuidAsString());

                auto& volume = *mountResponse.MutableVolume();
                volume.SetDiskId(diskId);
                volume.SetBlockSize(blockSize);
                volume.SetBlocksCount(blocksCount);

                it = AppCtx.MountedDisks
                         .emplace(diskId, std::move(mountResponse))
                         .first;
            }

            response.CopyFrom(it->second);
        }
    }

    void HandleRequest(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TUnmountVolumeRequest> request,
        NProto::TUnmountVolumeResponse& response)
    {
        Y_UNUSED(ctx);
        Y_UNUSED(response);

        with_lock (AppCtx.Lock) {
            auto it = AppCtx.MountedDisks.find(request->GetDiskId());
            if (it != AppCtx.MountedDisks.end()) {
                AppCtx.MountedDisks.erase(it);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNullService final
    : public TAppContext
    , public TBlockStoreImpl<TNullService, IBlockStore>
{
private:
    std::unique_ptr<TExecutor> Executor;

public:
    TNullService(const NProto::TNullServiceConfig& config)
    {
        Config = config;
        Executor = std::make_unique<TExecutor>(*this, "SNULL");
    }

    ~TNullService()
    {
        Stop();
    }

    void Start() override
    {
        Executor->Start();
    }
    void Stop() override
    {
        Executor->Stop();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        auto promise = NewPromise<typename TMethod::TResponse>();
        auto handler = std::make_shared<TRequestHandler<
            typename TMethod::TRequest,
            typename TMethod::TResponse>>(
            *this,
            std::move(callContext),
            std::move(request),
            promise);
        Executor->AddRequestHandler(std::move(handler));
        return promise.GetFuture();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateNullService(const NProto::TNullServiceConfig& config)
{
    return std::make_shared<TNullService>(config);
}

}   // namespace NCloud::NBlockStore
