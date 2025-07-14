#include "utils.h"

#include <cloud/storage/core/libs/grpc/threadpool.h>

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

void SetGrpcThreadsLimit(ui32 maxThreads)
{
    grpc_core::Executor::SetThreadsLimit(maxThreads);
    SetDefaultThreadPoolLimit(maxThreads);
}

}   // namespace NCloud
