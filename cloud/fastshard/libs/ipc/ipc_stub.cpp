#include "ipc.h"

#include <util/system/defaults.h>

#include <cerrno>

namespace NCloud::NFastShard {

////////////////////////////////////////////////////////////////////////////////

int RecvAll(int fd, void* buf, size_t len)
{
    Y_UNUSED(fd, buf, len);
    return EIO;
}

int SendAll(int fd, const void* buf, size_t len)
{
    Y_UNUSED(fd, buf, len);
    return EIO;
}

int RecvAll(int fd, void* buf, size_t len, IPoller& poller)
{
    Y_UNUSED(fd, buf, len, poller);
    return EIO;
}

int SendAll(int fd, const void* buf, size_t len, IPoller& poller)
{
    Y_UNUSED(fd, buf, len, poller);
    return EIO;
}

}   // namespace NCloud::NFastShard
