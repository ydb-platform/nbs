#include "vhost_test.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/folder/path.h>
#include <util/system/mutex.h>
#include <util/thread/lfqueue.h>

#include <atomic>

namespace NCloud::NBlockStore::NVhost {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestVhostRequest final
    : public TVhostRequest
{
private:
    TPromise<EResult> Promise;
    const std::function<void()> CompletionHandler;

public:
    TTestVhostRequest(
        TPromise<EResult> promise,
        EBlockStoreRequest type,
        ui64 from,
        ui64 length,
        TSgList sgList,
        void* cookie,
        bool isDiscardRequest,
        std::function<void()> completionHandler)
        : Promise(std::move(promise))
        , CompletionHandler(std::move(completionHandler))
    {
        Type = type;
        From = from;
        Length = length;
        SgList.SetSgList(std::move(sgList));
        Cookie = cookie;
        IsDiscardRequest = isDiscardRequest;
    }

    void Complete(EResult result) override
    {
        Promise.SetValue(result);
        if (CompletionHandler) {
            CompletionHandler();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestVhostDevice final
    : public ITestVhostDevice
    , public IVhostDevice
{
private:
    const TString SocketPath;
    void* const Cookie;
    const ui32 OptimalIoSize;
    const std::function<void()> RequestCompletionHandler;

    TLockFreeQueue<TVhostRequest*> Requests;

    TMutex Lock;
    TVector<TFuture<TVhostRequest::EResult>> Futures;

    std::atomic_flag Stopped = 0;

    TPromise<void> Autostop;

public:
    TTestVhostDevice(
        TString socketPath,
        void* cookie,
        ui32 optimalIoSize,
        std::function<void()> requestCompletionHandler)
        : SocketPath(std::move(socketPath))
        , Cookie(cookie)
        , OptimalIoSize(optimalIoSize)
        , RequestCompletionHandler(std::move(requestCompletionHandler))
    {
        Autostop = NewPromise<void>();
        Autostop.SetValue();
    }

    bool Start() override
    {
        TFsPath(SocketPath).Touch();
        return true;
    }

    TFuture<NProto::TError> Stop() override
    {
        Y_UNUSED(Stopped.test_and_set());
        return Autostop.GetFuture().Apply([this] (const auto&) {
            with_lock (Lock) {
                return WaitAll(Futures).Apply([] (const auto&) {
                    return NProto::TError();
                });
            }
        });
    }

    void Update(ui64 blocksCount) override
    {
        Y_UNUSED(blocksCount);
    }

    bool IsStopped() override
    {
        return Stopped.test();
    }

    void DisableAutostop(bool disable) override
    {
        if (disable) {
            auto promise = NewPromise<void>();
            Autostop.Swap(promise);
        } else {
            Autostop.SetValue();
        }
    }

    TFuture<TVhostRequest::EResult> SendTestRequest(
        EBlockStoreRequest type,
        ui64 from,
        ui64 length,
        TSgList sgList,
        bool isDiscardRequest = false) override
    {
        auto promise = NewPromise<TVhostRequest::EResult>();
        auto future = promise.GetFuture();

        with_lock (Lock) {
            if (Stopped.test()) {
                promise.SetValue(TVhostRequest::CANCELLED);
                return future;
            }
            Futures.push_back(future);
        }

        auto request = std::make_unique<TTestVhostRequest>(
            std::move(promise),
            type,
            from,
            length,
            std::move(sgList),
            Cookie,
            isDiscardRequest,
            RequestCompletionHandler);
        Requests.Enqueue(request.release());
        return future;
    }

    TVhostRequestPtr DequeueRequest()
    {
        TVhostRequest* request;
        if (Requests.Dequeue(&request)) {
            return std::unique_ptr<TVhostRequest>(request);
        }
        return nullptr;
    }

    ui32 GetOptimalIoSize() const override
    {
        return OptimalIoSize;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestVhostQueue final
    : public ITestVhostQueue
    , public IVhostQueue
{
private:
    TManualEvent& FailedEvent;

    enum EState {
        Undefined = 0,
        Running = 1,
        Stopped = 2,
        Broken = 3,
    };
    std::atomic<EState> State = Undefined;

    TMutex Lock;
    TVector<std::weak_ptr<TTestVhostDevice>> Devices;

public:
    TTestVhostQueue(TManualEvent& failedEvent)
        : FailedEvent(failedEvent)
    {}

    int Run() override
    {
        EState expected = Undefined;
        State.compare_exchange_strong(expected, Running);

        switch (State.load()) {
            case Running:
                return -EAGAIN;
            case Stopped:
                return 0;
            case Broken:
                FailedEvent.Signal();
                return -1;
            default:
                Y_ABORT();
        }
    }

    void Stop() override
    {
        EState expected = Running;
        bool wasRun = State.compare_exchange_strong(expected, Stopped);
        Y_ABORT_UNLESS(wasRun || State.load() == Broken);
    }

    // A device may be served by several queues at once, so the same device can
    // be registered in more than one queue.
    void AddDevice(std::weak_ptr<TTestVhostDevice> device)
    {
        with_lock (Lock) {
            Devices.push_back(std::move(device));
        }
    }

    TVhostRequestPtr DequeueRequest() override
    {
        if (State.load() == Running) {
            with_lock (Lock) {
                for (auto& device: Devices) {
                    if (auto ptr = device.lock()) {
                        auto request = ptr->DequeueRequest();
                        if (request) {
                            return request;
                        }
                    }
                }
            }
        }
        return nullptr;
    }

    bool IsRun() override
    {
        return State.load() == Running;
    }

    TVector<std::shared_ptr<ITestVhostDevice>> GetDevices() override
    {
        TVector<std::shared_ptr<ITestVhostDevice>> res;
        with_lock (Lock) {
            for (auto& device: Devices) {
                if (auto ptr = device.lock()) {
                    res.push_back(ptr);
                }
            }
        }
        return res;
    }

    void Break() override
    {
        State.store(Broken);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IVhostQueuePtr TTestVhostQueueFactory::CreateQueue()
{
    auto queue = std::make_shared<TTestVhostQueue>(FailedEvent);
    Queues.push_back(queue);
    return queue;
}

IVhostDevicePtr TTestVhostQueueFactory::CreateDevice(
    TString socketPath,
    TString deviceName,
    ui32 blockSize,
    ui64 blocksCount,
    ui32 queuesCount,
    bool discardEnabled,
    bool writeZeroesEnabled,
    ui32 optimalIoSize,
    TVector<IVhostQueuePtr> queues,
    void* cookie,
    const TVhostCallbacks& callbacks,
    bool readOnly)
{
    Y_UNUSED(deviceName);
    Y_UNUSED(blockSize);
    Y_UNUSED(blocksCount);
    Y_UNUSED(discardEnabled);
    Y_UNUSED(writeZeroesEnabled);
    Y_UNUSED(callbacks);
    Y_UNUSED(readOnly);

    Y_ABORT_UNLESS(!queues.empty());
    Y_ABORT_UNLESS(queues.size() <= queuesCount);

    auto vhostDevice = std::make_shared<TTestVhostDevice>(
        std::move(socketPath),
        cookie,
        optimalIoSize,
        RequestCompletionHandler);

    for (const auto& queue: queues) {
        static_cast<TTestVhostQueue*>(queue.get())->AddDevice(vhostDevice);
    }

    return vhostDevice;
}

}   // namespace NCloud::NBlockStore::NVhost
