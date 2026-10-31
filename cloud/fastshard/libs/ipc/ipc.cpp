#include "ipc.h"

#include <silk/fibers/fiber.h>

#include <poll.h>
#include <sys/socket.h>

#include <cerrno>

namespace NCloud::NFastShard {

using silk::FiberScheduler;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFiberPoller final: IPoller
{
    int Poll(int fd, uint32_t events) override
    {
        return FiberScheduler::poll(fd, events);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int RecvAll(int fd, void* buf, size_t len)
{
    TFiberPoller poller;
    return RecvAll(fd, buf, len, poller);
}

int SendAll(int fd, const void* buf, size_t len)
{
    TFiberPoller poller;
    return SendAll(fd, buf, len, poller);
}

int RecvAll(int fd, void* buf, size_t len, IPoller& poller)
{
    auto* p = static_cast<ui8*>(buf);
    size_t done = 0;
    while (done < len) {
        ssize_t n = ::recv(fd, p + done, len - done, 0);
        if (n > 0) {
            done += static_cast<size_t>(n);
            continue;
        }
        if (n == 0) {
            return EIO;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            if (int r = poller.Poll(fd, POLLIN); r) {
                return r;
            }
            continue;
        }
        if (errno == EINTR) {
            continue;
        }
        return errno;
    }
    return 0;
}

int SendAll(int fd, const void* buf, size_t len, IPoller& poller)
{
    const auto* p = static_cast<const ui8*>(buf);
    size_t done = 0;
    while (done < len) {
        ssize_t n = ::send(fd, p + done, len - done, MSG_NOSIGNAL);
        if (n > 0) {
            done += static_cast<size_t>(n);
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            if (int r = poller.Poll(fd, POLLOUT); r) {
                return r;
            }
            continue;
        }
        if (errno == EINTR) {
            continue;
        }
        return errno;
    }
    return 0;
}

}   // namespace NCloud::NFastShard
