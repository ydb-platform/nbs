#pragma once

#include <library/cpp/coroutine/engine/sockpool.h>

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore::NServer {

///////////////////////////////////////////////////////////////////////////////

// Similar to TContIO, but DoWrite() and DoRead() operations are executed with a
// timeout.
class TContIOWithTimeout
    : public IInputStream
    , public IOutputStream
{
    SOCKET Fd_;
    TCont* Cont_;
    TDuration Timeout;

public:
    TContIOWithTimeout(SOCKET fd, TCont* cont, TDuration timeout)
        : Fd_(fd)
        , Cont_(cont)
        , Timeout(timeout)
    {}

    void DoWrite(const void* buf, size_t len) override
    {
        NCoro::WriteD(Cont_, Fd_, buf, len, TInstant::Now() + Timeout)
            .Checked();
    }

    size_t DoRead(void* buf, size_t len) override
    {
        return NCoro::ReadD(Cont_, Fd_, buf, len, TInstant::Now() + Timeout)
            .Checked();
    }

    SOCKET Fd() const noexcept
    {
        return Fd_;
    }
};

}   // namespace NCloud::NBlockStore::NServer
