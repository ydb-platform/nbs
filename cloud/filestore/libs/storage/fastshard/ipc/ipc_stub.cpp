#include "ipc.h"

#include <util/system/defaults.h>

#include <cerrno>

namespace NCloud::NFileStore::NStorage::NFastShard {

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

}   // namespace NCloud::NFileStore::NStorage::NFastShard
