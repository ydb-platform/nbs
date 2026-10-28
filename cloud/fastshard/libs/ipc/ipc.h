#pragma once

#include <util/system/types.h>

#include <cstddef>
#include <cstdint>

namespace NCloud::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct IPoller
{
    virtual ~IPoller() = default;

    // 0 once one of @p events is ready on @p fd, errno otherwise.
    virtual int Poll(int fd, uint32_t events) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Async TCP helpers using silk's non-blocking poll.
// Must be called from a silk fiber context.
// Return 0 on success, errno on failure, EIO on unexpected EOF.

int RecvAll(int fd, void* buf, size_t len);
int SendAll(int fd, const void* buf, size_t len);

int RecvAll(int fd, void* buf, size_t len, IPoller& poller);
int SendAll(int fd, const void* buf, size_t len, IPoller& poller);

}   // namespace NCloud::NFastShard
