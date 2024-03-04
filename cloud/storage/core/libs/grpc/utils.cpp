#include "utils.h"

#include <contrib/libs/grpc/src/core/lib/iomgr/executor.h>

#include <util/string/cast.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

bool TryParseSourceFd(const TStringBuf& peer, ui32* fd)
{
    static constexpr TStringBuf PeerFdPrefix = "fd:";
    TStringBuf peerFd;
    if (!peer.AfterPrefix(PeerFdPrefix, peerFd)) {
        return false;
    }
    return TryFromString(peerFd, *fd);
}

////////////////////////////////////////////////////////////////////////////////

size_t SetExecutorThreadsLimit(size_t count)
{
    return grpc_core::Executor::SetThreadsLimit(count);
}

}   // namespace NCloud
