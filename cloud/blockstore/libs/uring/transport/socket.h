#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NUring {

////////////////////////////////////////////////////////////////////////////////

struct TSocketOptions
{
    TDuration TcpUserTimeout;
    TDuration KeepAliveIdle;
    TDuration KeepAliveInterval;
    ui32 KeepAliveCount = 0;
};

NProto::TError MakeSystemError(int code, TStringBuf message);

// TCP_NODELAY, SO_KEEPALIVE + TCP_KEEP*, TCP_USER_TIMEOUT. Zero durations
// leave the kernel default.
NProto::TError ApplySocketOptions(int fd, const TSocketOptions& options);

// Resolves host with AI_PASSIVE, binds, listens. Returns the listening fd.
// On error the fd is 0 and must not be closed; check HasError first.
TResultOrError<int>
CreateListenSocket(const TString& host, ui16 port, ui32 backlog);

TResultOrError<ui16> GetLocalPort(int fd);

void CloseSocket(int fd);

// SO_LINGER{1, 0} then close: RST instead of FIN, so no page is left pinned
// by the TCP stack and the fd number can be reused at once.
void CloseWithReset(int fd);

}   // namespace NCloud::NBlockStore::NUring
