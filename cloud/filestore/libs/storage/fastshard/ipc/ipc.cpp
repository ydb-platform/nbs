#include "ipc.h"

#include <silk/fibers/fiber.h>

#include <cerrno>

#include <poll.h>
#include <sys/socket.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

using silk::FiberScheduler;

////////////////////////////////////////////////////////////////////////////////

int RecvAll(int fd, void* buf, size_t len)
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
            if (int r = FiberScheduler::poll(fd, POLLIN); r) {
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

int SendAll(int fd, const void* buf, size_t len)
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
            if (int r = FiberScheduler::poll(fd, POLLOUT); r) {
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

}   // namespace NCloud::NFileStore::NStorage::NFastShard
