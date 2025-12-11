#include "thread_park.h"

#include <util/system/error.h>
#include <util/system/event.h>

#if defined(_linux_)
#include <errno.h>
#include <linux/futex.h>
#include <sys/syscall.h>
#endif

namespace NCloud {

#if defined(_linux_)

////////////////////////////////////////////////////////////////////////////////

class TThreadPark::TImpl
{
private:
    int Signaled = 0;

public:
    void Wait()
    {
        for (;;) {
            int signaled = __atomic_exchange_n(&Signaled, 0, __ATOMIC_SEQ_CST);
            if (signaled) {
                return;
            }

            int ret = syscall(
                SYS_futex,
                &Signaled,
                FUTEX_WAIT_PRIVATE,
                0,
                nullptr,
                nullptr,
                0);
            if (ret == -1) {
                Y_ABORT_UNLESS(
                    errno == EAGAIN || errno == EINTR,
                    "unable to wait on futex: %s",
                    LastSystemErrorText(errno));
            }
        }
    }

    bool WaitT(TDuration timeout)
    {
        int signaled = __atomic_exchange_n(&Signaled, 0, __ATOMIC_SEQ_CST);
        if (signaled) {
            return true;
        }

        timespec ts;
        ts.tv_sec = timeout.Seconds();
        ts.tv_nsec = timeout.NanoSecondsOfSecond();

        int ret = syscall(
            SYS_futex,
            &Signaled,
            FUTEX_WAIT_PRIVATE,
            0,
            &ts,
            nullptr,
            0);
        if (ret == -1) {
            Y_ABORT_UNLESS(
                errno == EAGAIN || errno == EINTR || errno == ETIMEDOUT,
                "unable to wait on futex: %s",
                LastSystemErrorText(errno));
        }

        return __atomic_exchange_n(&Signaled, 0, __ATOMIC_SEQ_CST);
    }

    bool WaitD(TInstant deadLine)
    {
        for (;;) {
            TInstant now = TInstant::Now();
            if (deadLine < now) {
                return false;
            }
            if (WaitT(deadLine - now)) {
                return true;
            }
        }
    }

    void Signal()
    {
        int signaled = __atomic_exchange_n(&Signaled, 1, __ATOMIC_SEQ_CST);
        if (!signaled) {
            int ret = syscall(
                SYS_futex,
                &Signaled,
                FUTEX_WAKE_PRIVATE,
                INT_MAX,
                nullptr,
                nullptr,
                0);
            Y_ABORT_UNLESS(
                ret >= 0,
                "unable to signal on futex: %s",
                LastSystemErrorText(errno));
        }
    }
};

#else   // !defined(_linux_)

////////////////////////////////////////////////////////////////////////////////

class TThreadPark::TImpl
{
private:
    TSystemEvent Event;

public:
    TImpl()
        : Event(TSystemEvent::rAuto)
    {}

    void Wait()
    {
        Event.Wait();
    }

    bool WaitT(TDuration timeout)
    {
        return Event.WaitT(timeout);
    }

    bool WaitD(TInstant deadLine)
    {
        return Event.WaitD(deadLine);
    }

    void Signal()
    {
        Event.Signal();
    }
};

#endif

////////////////////////////////////////////////////////////////////////////////

TThreadPark::TThreadPark()
    : Impl(new TThreadPark::TImpl())
{}

TThreadPark::~TThreadPark()
{}

void TThreadPark::Wait()
{
    Impl->Wait();
}

bool TThreadPark::WaitT(TDuration timeout)
{
    return Impl->WaitT(timeout);
}

bool TThreadPark::WaitD(TInstant deadLine)
{
    return Impl->WaitD(deadLine);
}

void TThreadPark::Signal()
{
    Impl->Signal();
}

}   // namespace NCloud
