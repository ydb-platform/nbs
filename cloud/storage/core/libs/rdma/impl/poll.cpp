#include "poll.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/datetime/base.h>
#include <util/system/error.h>

#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

#define RDMA_THROW_ERROR(method)                                               \
    STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(LastSystemError()))          \
        << method << " failed with error " << LastSystemError()                \
        << ": " << LastSystemErrorText()                                       \
// RDMA_THROW_ERROR

////////////////////////////////////////////////////////////////////////////////

void SetNonBlock(int fd, bool nonblock)
{
    int res = fcntl(fd, F_GETFL);
    if (res < 0) {
        RDMA_THROW_ERROR("fcntl(F_GETFL)");
    }

    if (nonblock) {
        res |= O_NONBLOCK;
    } else {
        res &= ~ui32(O_NONBLOCK);
    }

    res = fcntl(fd, F_SETFL, res);
    if (res < 0) {
        RDMA_THROW_ERROR("fcntl(F_SETFL)");
    }
}

////////////////////////////////////////////////////////////////////////////////

TEventHandle::TEventHandle()
{
    Fd = eventfd(0, EFD_NONBLOCK);
    if (Fd < 0) {
        RDMA_THROW_ERROR("eventfd");
    }
}

TEventHandle::~TEventHandle()
{
    close(Fd);
}

void TEventHandle::Set()
{
    int res;
    while ((res = eventfd_write(Fd, 1)) < 0) {
        if (errno != EINTR) {
            RDMA_THROW_ERROR("eventfd_write");
        }
    }
}

void TEventHandle::Clear()
{
    eventfd_t unused;

    int res;
    while ((res = eventfd_read(Fd, &unused)) < 0) {
        if (errno != EINTR) {
            RDMA_THROW_ERROR("eventfd_read");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TTimerHandle::TTimerHandle()
{
    Fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (Fd < 0) {
        RDMA_THROW_ERROR("timerfd_create");
    }
}

TTimerHandle::~TTimerHandle()
{
    close(Fd);
}

void TTimerHandle::Set(TDuration duration)
{
    itimerspec spec = {
        .it_value = {
            .tv_sec = static_cast<__time_t>(duration.Seconds()),
            .tv_nsec = duration.NanoSecondsOfSecond(),
        }
    };
    int res = timerfd_settime(Fd, 0, &spec, nullptr);
    if (res < 0) {
        RDMA_THROW_ERROR("timerfd_settime");
    }
}

TDuration TTimerHandle::Get()
{
    itimerspec spec;
    int res = timerfd_gettime(Fd, &spec);
    if (res < 0) {
        RDMA_THROW_ERROR("timerfd_gettime");
    }
    return
        TDuration::Seconds(spec.it_value.tv_sec) +
        TDuration::MicroSeconds(spec.it_value.tv_nsec / 1000);
}

void TTimerHandle::Clear()
{
    itimerspec spec = {};
    int res = timerfd_settime(Fd, 0, &spec, nullptr);
    if (res < 0) {
        RDMA_THROW_ERROR("timerfd_settime");
    }
}

////////////////////////////////////////////////////////////////////////////////

TPollHandle::TPollHandle()
{
    Fd = epoll_create1(0);
    if (Fd < 0) {
        RDMA_THROW_ERROR("epoll_create");
    }

    Zero(Events);
}

TPollHandle::~TPollHandle()
{
    close(Fd);
}

void TPollHandle::Attach(int fd, ui32 events, void* data)
{
    epoll_event event = {
        .events = events,
        .data = {
            .ptr = data,
        },
    };

    int res = epoll_ctl(Fd, EPOLL_CTL_ADD, fd, &event);
    if (res < 0 && errno != EEXIST) {
        RDMA_THROW_ERROR("epoll_ctl(EPOLL_CTL_ADD)");
    }
}

void TPollHandle::Detach(int fd)
{
    int res = epoll_ctl(Fd, EPOLL_CTL_DEL, fd, nullptr);
    if (res < 0 && errno != ENOENT) {
        RDMA_THROW_ERROR("epoll_ctl(EPOLL_CTL_DEL)");
    }
}

size_t TPollHandle::Wait(TDuration timeout)
{
    const int timeoutMs = timeout.MilliSeconds();

    int res;
    while ((res = epoll_wait(Fd, Events, MAX_EVENTS, timeoutMs)) < 0) {
        if (errno != EAGAIN && errno != EINTR) {
            RDMA_THROW_ERROR("epoll_wait");
        }
    }

    Y_DEBUG_ABORT_UNLESS(res >= 0 && res < MAX_EVENTS);
    return res;
}

}   // namespace NCloud::NBlockStore::NRdma
