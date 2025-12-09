#pragma once

#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/vhost-client/vhost-buffered-client.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/system/mutex.h>
#include <util/system/types.h>
#include <util/thread/factory.h>

#include <memory>

namespace NCloud::NFileStore::NVhost {

////////////////////////////////////////////////////////////////////////////////

class TFuseVirtioClient
    : public std::enable_shared_from_this<TFuseVirtioClient>
{
private:
    using TThread = THolder<IThreadFactory::IThread>;

    TDuration WaitTimeout;
    std::unique_ptr<NVHost::TBufferedClient> Client;
    ui64 RequestId = 0;
    ITaskQueuePtr ThreadPool;

public:
    TFuseVirtioClient(const TString& SocketPath, const TDuration& timeout = TDuration::MilliSeconds(1000))
        : WaitTimeout(timeout)
        , ThreadPool(CreateThreadPool("reqs", 4))
    {
        Client = std::make_unique<NVHost::TBufferedClient>(SocketPath);
        ThreadPool->Start();
    }

    ~TFuseVirtioClient()
    {
        ThreadPool->Stop();
    }

    template <typename T>
    auto SendRequest(std::shared_ptr<T> request)
    {
        request->In->Header.unique = ++RequestId;

        auto future = request->Result.GetFuture();

        auto weakPtr = weak_from_this();

        ThreadPool->Execute([weakPtr, request] {
            if (auto p = weakPtr.lock()) {
                p->SendRequestImpl(request);
            }
        });

        return future;
    }

    template <typename T, typename ...TArgs>
    auto SendRequest(TArgs&& ...args)
    {
        return SendRequest(std::make_shared<T>(std::forward<TArgs>(args)...));
    }

    void Init()
    {
        const bool ok = Client->Init();
        Y_ABORT_UNLESS(ok);
    }

    void DeInit()
    {
        Client->DeInit();
        ThreadPool->Stop();
    }

    ui64 GetLastRequestId() const
    {
        return RequestId;
    }

private:
    template <typename T>
    void SendRequestImpl(std::shared_ptr<T> request)
    {
        TVector<TVector<char>> inData{TVector<char>(request->In->Header.len)};
        memcpy(
            inData[0].data(),
            reinterpret_cast<char*>(&*request->In),
            inData[0].size());
        constexpr auto headerSize = sizeof(request->Out->Header);
        TVector<TVector<char>> outData{TVector<char>(headerSize)};

        if (request->Out->Header.len > headerSize) {
            outData.emplace_back(request->Out->Header.len - headerSize);
        }

        bool result = Client->Write(inData, outData);
        if (result) {
            char* dest = reinterpret_cast<char*>(&*request->Out);
            for (const auto& data: outData) {
                memcpy(dest, data.data(), data.size());
                dest += data.size();
            }
        }
        request->OnCompletion();
    }
};

}   // namespace NCloud::NFileStore::NVhost
