#pragma once
// https://stefanha.github.io/virtio/vhost-user-slave.html

#include "sglist.h"
#include "vhost-user-protocol/message.h"

#include <library/cpp/logger/log.h>
#include <library/cpp/threading/future/core/future.h>

#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/system/file.h>

#include <array>
#include <thread>

namespace NVHost {

////////////////////////////////////////////////////////////////////////////////

class TQueue;

struct TClientParams
{
    uint32_t QueueCount = 2;
    uint32_t QueueSize = 32;
    uint64_t MemorySize = 1_MB;
    uint64_t MemoryAlignment = 4_KB;
};

class TClient
{
    struct TMemTable
    {
        int Fd = -1;

        char* Addr = nullptr;
        size_t Size = 0;
    };

private:
    const TString SockPath;
    const TClientParams Params;

    TFileHandle Sock;
    TLog Logger;

    TMemTable MemTable;
    TVector<TQueue> Queues;

    std::atomic_bool ShouldStop = false;
    TVector<std::thread> QueueThreads;

public:
    explicit TClient(TString sockPath, TClientParams params = {});

    TClient(const TClient&) = delete;
    TClient(TClient&& other) = delete;

    ~TClient();

    TClient& operator=(const TClient&) = delete;
    TClient& operator=(TClient&& other) = delete;

    std::span<char> GetMemory();

    NThreading::TFuture<uint32_t> WriteAsync(
        uint32_t queueIndex,
        const TSgList& inBuffers,
        const TSgList& outBuffers);

    bool Init();
    void DeInit();

private:
    bool Execute(NVHostUser::IMessage& msg);

    bool Connect();
    bool CoordinationFeatures(
        uint64_t virtioFeatures = 0,
        uint64_t virtioProtocolFeatures = 0);
    bool CoordinationMemMap();
    bool CoordinationQueue();

    TMemTable MapMemory(uint64_t size);
    uint64_t MemoryPerQueue() const;
    uint64_t TotalQueuesMemory() const;
};

}   // namespace NVHost
