#include "vhost-queue.h"

#include "monotonic_buffer_resource.h"
#include <cloud/contrib/vhost/platform.h>

#include <util/generic/scope.h>

#include <new>

#include <poll.h>
#include <unistd.h>
#include <sys/eventfd.h>

namespace NVHost {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TQueue::TQueue(uint32_t size, std::span<char> buffer)
    : Size(size)
    , KickFd(eventfd(0, EFD_NONBLOCK))
    , CallFd(eventfd(0, EFD_NONBLOCK))
    , ErrFd(eventfd(0, EFD_NONBLOCK))
    , InFlights(size)
{
    init_platform_page_size();

    memset(buffer.data(), 0, buffer.size());

    TMonotonicBufferResource memory {buffer};

    Descriptors = reinterpret_cast<virtq_desc*>(memory.Allocate(
        sizeof(virtq_desc) * size,
        alignof(virtq_desc)).data());

    AvailableRings = reinterpret_cast<virtq_avail*>(memory.Allocate(
        sizeof(virtq_avail) + (size + 1) * sizeof(uint16_t),
        alignof(virtq_avail)).data());

    UsedRings = reinterpret_cast<virtq_used*>(memory.Allocate(
        sizeof(virtq_used) + size * sizeof(virtq_used_elem) + sizeof(uint16_t),
        platform_page_size).data());

    for (uint32_t i = 0; i != size; ++i) {
        FreeBuffers.push(i);
    }
}

TQueue::TQueue(TQueue&& other)
    : Size(other.Size)
    , Descriptors (other.Descriptors)
    , AvailableRings (other.AvailableRings)
    , UsedRings (other.UsedRings)
    , KickFd(std::move(other.KickFd))
    , CallFd(std::move(other.CallFd))
    , ErrFd(std::move(other.ErrFd))
    , FreeBuffers(std::move(other.FreeBuffers))
    , LastSeenUsed(other.LastSeenUsed)
    , InFlights(std::move(other.InFlights))
{
}

uint64_t TQueue::GetDescriptorsAddr() const
{
    return std::bit_cast<uint64_t>(Descriptors);
}

uint64_t TQueue::GetAvailableRingsAddr() const
{
    return std::bit_cast<uint64_t>(AvailableRings);
}

uint64_t TQueue::GetUsedRings() const
{
    return std::bit_cast<uint64_t>(UsedRings);
}

int TQueue::GetKickFd() const
{
    return KickFd;
}

int TQueue::GetCallFd() const
{
    return CallFd;
}

int TQueue::GetErrFd() const
{
    return ErrFd;
}

TFuture<uint32_t> TQueue::WriteAsync(
    const TSgList& inBuffers,
    const TSgList& outBuffers)
{
    std::unique_lock lock {Mutex};

    if (FreeBuffers.size() < (inBuffers.size() + outBuffers.size())) {
        return MakeFuture<uint32_t>();
    }

    TVector<size_t> usedBuffers;

    Y_DEFER {
        for (auto idx: usedBuffers) {
            FreeBuffers.push(idx);
        }
    };

    const uint32_t inflight = FreeBuffers.front();

    int prevIdx = -1;
    for (auto buf: inBuffers) {
        const size_t inIdx = FreeBuffers.front();

        FreeBuffers.pop();
        usedBuffers.push_back(inIdx);

        if (prevIdx == -1) {
            UsedRings->flags = 0;
            AvailableRings->flags = 0;
            AvailableRings->ring[AvailableRings->idx % Size] = inIdx;
            AvailableRings->ring[Size] = 0;
            ++AvailableRings->idx;
        } else {
            Descriptors[prevIdx].next = inIdx;
        }
        prevIdx = inIdx;
        Descriptors[inIdx] = {
            .addr = std::bit_cast<uint64_t>(buf.data()),
            .len = static_cast<uint32_t>(buf.size()),
            .flags = VIRTQ_DESC_F_NEXT
        };
    }

    for (auto buf: outBuffers) {
        const size_t outIdx = FreeBuffers.front();

        FreeBuffers.pop();
        usedBuffers.push_back(outIdx);

        Descriptors[prevIdx].next = outIdx;
        prevIdx = outIdx;

        Descriptors[outIdx] = {
            .addr = std::bit_cast<uint64_t>(buf.data()),
            .len = static_cast<uint32_t>(buf.size()),
            .flags = VIRTQ_DESC_F_WRITE | VIRTQ_DESC_F_NEXT
        };
    }

    Descriptors[prevIdx].flags &= ~((uint16_t) VIRTQ_DESC_F_NEXT);
    Descriptors[prevIdx].next = 0;

    if (eventfd_write(KickFd, 1) < 0) {
        return MakeFuture<uint32_t>();
    }
    usedBuffers.clear();

    InFlights[inflight] = NewPromise<uint32_t>();

    return InFlights[inflight].GetFuture();
}

bool TQueue::RunOnce(TDuration timeout)
{
    if (!WaitCallEvent(timeout)) {
        return false;
    }

    std::unique_lock lock {Mutex};

    for (; LastSeenUsed != UsedRings->idx; ++LastSeenUsed) {
        virtq_used_elem& elem = UsedRings->ring[LastSeenUsed % Size];
        InFlights[elem.id].SetValue(elem.len);

        // free buffers
        uint32_t i = elem.id;
        FreeBuffers.push(i);

        while (Descriptors[i].flags & VIRTQ_DESC_F_NEXT) {
            i = Descriptors[i].next;
            FreeBuffers.push(i);
        }
    }

    return true;
}

bool TQueue::WaitCallEvent(TDuration timeout)
{
    const int ms = timeout != TDuration::Max()
        ? timeout.MilliSeconds()
        : -1;

    pollfd fds[] {
        { .fd = ErrFd, .events = POLLIN },
        { .fd = CallFd, .events = POLLIN }
    };

    if (poll(fds, std::size(fds), ms) < 0) {
        return false;
    }

    uint64_t event;
    if ((fds[0].revents & POLLIN)) {
        eventfd_read(fds[0].fd, &event);
        return false;
    }

    if ((fds[0].revents & POLLERR) |
        (fds[0].revents & POLLHUP) |
        (fds[0].revents & POLLNVAL) |
        (fds[1].revents & POLLERR) |
        (fds[1].revents & POLLHUP) |
        (fds[1].revents & POLLNVAL))
    {
        return false;
    }

    if (fds[1].revents & POLLIN) {
        return eventfd_read(fds[1].fd, &event) >= 0;
    }

    return false;
}

uint32_t TQueue::GetQueueMemSize(uint32_t queueSize)
{
    return AlignUp(sizeof(virtq_desc) * queueSize
            + sizeof(uint16_t) * (3 + queueSize), platform_page_size)
        + AlignUp(sizeof(uint16_t) * 3
            + sizeof(struct virtq_used_elem) * queueSize, platform_page_size);
}

} // namespace NVHostQueue
