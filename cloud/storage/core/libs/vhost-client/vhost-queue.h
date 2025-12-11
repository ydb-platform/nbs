#pragma once

#include "sglist.h"

#include <cloud/contrib/vhost/virtio/virtio_spec.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>
#include <util/system/file.h>
#include <util/system/mutex.h>

#include <array>
#include <mutex>
#include <queue>
#include <span>

namespace NVHost {

////////////////////////////////////////////////////////////////////////////////

class TQueue
{
private:
    const uint32_t Size;

    virtq_desc* Descriptors = nullptr;
    virtq_avail* AvailableRings = nullptr;
    virtq_used* UsedRings = nullptr;

    TFileHandle KickFd;
    TFileHandle CallFd;
    TFileHandle ErrFd;

    std::mutex Mutex;
    std::queue<uint32_t> FreeBuffers;   // TODO: use buffer chain

    uint16_t LastSeenUsed = 0;   // type same is virtq_used.idx

    TVector<NThreading::TPromise<uint32_t>> InFlights;

public:
    explicit TQueue(uint32_t size, std::span<char> buffer);

    TQueue(const TQueue&) = delete;
    TQueue(TQueue&& other);

    TQueue& operator=(const TQueue&) = delete;
    TQueue& operator=(TQueue&& other) = delete;

    uint64_t GetDescriptorsAddr() const;
    uint64_t GetAvailableRingsAddr() const;
    uint64_t GetUsedRings() const;

    int GetKickFd() const;
    int GetCallFd() const;
    int GetErrFd() const;

    NThreading::TFuture<uint32_t> WriteAsync(
        const TSgList& inBuffers,
        const TSgList& outBuffers);

    bool RunOnce(TDuration timeout = TDuration::Max());

    static uint32_t GetQueueMemSize(uint32_t queueSize);

private:
    bool WaitCallEvent(TDuration timeout = TDuration::Max());
};

}   // namespace NVHost
