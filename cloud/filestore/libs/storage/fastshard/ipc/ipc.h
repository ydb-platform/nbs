#pragma once

#include <util/system/types.h>

#include <cstddef>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

// Async TCP helpers using silk's non-blocking poll.
// Must be called from a silk fiber context.
// Return 0 on success, errno on failure, EIO on unexpected EOF.

int RecvAll(int fd, void* buf, size_t len);
int SendAll(int fd, const void* buf, size_t len);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
