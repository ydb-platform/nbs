#pragma once

#include "public.h"

#include <util/datetime/base.h>

#include <sys/epoll.h>
#include <sys/timerfd.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

void SetNonBlock(int fd, bool nonblock);

////////////////////////////////////////////////////////////////////////////////

class TEventHandle
{
private:
    int Fd;

public:
    TEventHandle();
    ~TEventHandle();

    int Handle() const
    {
        return Fd;
    }

    void Set();
    void Clear();
};

////////////////////////////////////////////////////////////////////////////////

class TTimerHandle
{
private:
    int Fd;

public:
    TTimerHandle();
    ~TTimerHandle();

    int Handle() const
    {
        return Fd;
    }

    void Set(TDuration duration);
    void Clear();
};

////////////////////////////////////////////////////////////////////////////////

class TPollHandle
{
private:
    int Fd;

    static constexpr int MAX_EVENTS = 64;
    epoll_event Events[MAX_EVENTS];

public:
    TPollHandle();
    ~TPollHandle();

    int Handle() const
    {
        return Fd;
    }

    void Attach(int fd, ui32 events, void* data = nullptr);
    void Detach(int fd);

    size_t Wait(TDuration timeout);

    const epoll_event& GetEvent(size_t index) const
    {
        Y_VERIFY_DEBUG(index < MAX_EVENTS);
        return Events[index];
    }
};

}   // namespace NCloud::NBlockStore::NRdma
