#pragma once

#include "public.h"

#include "protocol.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/thread.h>
#include <util/thread/lfqueue.h>

#include <liburing.h>
#include <sys/socket.h>

#include <atomic>
#include <functional>
#include <memory>

namespace NCloud::NBlockStore::NUring {

////////////////////////////////////////////////////////////////////////////////

struct TIoOp;

struct IIoHandler
{
    virtual ~IIoHandler() = default;

    virtual void OnIoComplete(TIoOp* op, int res, ui32 cqeFlags) = 0;
};

struct ITickable
{
    virtual ~ITickable() = default;

    virtual void OnTick(TInstant now) = 0;
};

// Tags at or above TAG_RESERVED_BASE belong to the ring's own operations;
// AllocOp rejects them.
constexpr ui32 TAG_RESERVED_BASE = 0xFFFF0000;

struct TIoOp
{
    IIoHandler* Handler = nullptr;
    ui32 Tag = 0;
    bool InFlight = false;
    bool ZeroCopy = false;

    iovec Iov[MAX_IOV];
    ui32 IovCount = 0;
    msghdr Msg{};
    sockaddr_storage Addr{};
    socklen_t AddrLen = 0;
    ui64 Word = 0;

    std::function<void(io_uring_sqe*)> Prep;
};

////////////////////////////////////////////////////////////////////////////////

class TRing final: private ISimpleThread
{
public:
    using TCommand = std::function<void()>;

private:
    const TString Name;
    const ui32 QueueSize;
    const ESendZeroCopyMode ZcMode;
    TLog Log;

    io_uring Ring{};
    bool RingInitialized = false;
    int EventFd = -1;
    int TimerFd = -1;
    bool ZcSupported = false;

    std::atomic<bool> Sleeping = false;
    std::atomic<bool> StopRequested = false;
    std::atomic<size_t> ActiveOps = 0;
    // ops with a caller-supplied tag; the ring's own reads and cancels are not
    // counted here
    size_t UserOps = 0;
    TLockFreeQueue<TCommand> Commands;
    TDeque<TIoOp*> Pending;
    TVector<ITickable*> Tickables;
    TVector<ITickable*> TickSweep;
    TVector<std::unique_ptr<TIoOp>> OpPool;

    TIoOp EventOp;
    TIoOp TimerOp;
    bool StopInitiated = false;
    bool Draining = false;

    NThreading::TPromise<NProto::TError> InitResult;
    std::atomic<TThread::TId> ThreadId = 0;

public:
    TRing(TString name, ui32 queueSize, ESendZeroCopyMode zcMode, TLog log);
    ~TRing() override;

    NProto::TError Start();

    // Requests shutdown and joins the ring thread. The loop does not exit
    // while an op is in flight, so Stop() blocks until the last user op is
    // reaped: the caller must either have no IO outstanding or cancel it,
    // which a command posted after Stop() can still do (see Execute).
    // A second Stop(), or one after a failed Start(), is a no-op. Not safe to
    // call concurrently with itself; must not be called from the ring thread.
    void Stop();

    // Runs the command on the ring thread; callable from any thread for the
    // lifetime of this object. A command enqueued while the loop is live runs
    // in it and may start IO. That includes shutdown while user ops are still
    // in flight: the ring keeps its eventfd read armed until the last one is
    // reaped, so a teardown cancel posted after Stop() is woken up, run, and
    // its own IO reaped. Once no user op is left the loop commits to exiting,
    // and racing Execute with that point is not supported: such a command
    // either runs in the final drain, where it must not start IO (Submit
    // aborts), or is dropped because the ring thread has already exited (also
    // after a failed Start()).
    void Execute(TCommand command);
    bool IsRingThread() const;

    bool SendZeroCopySupported() const
    {
        return ZcSupported;
    }

    // Includes the ring's own eventfd and timer reads, so an idle ring reports
    // 2.
    size_t InFlightOps() const
    {
        return ActiveOps.load();
    }

    TIoOp* AllocOp(IIoHandler* handler, ui32 tag);
    void FreeOp(TIoOp* op);

    void Recv(TIoOp* op, int fd, void* buf, size_t len, int flags);
    void RecvMsg(TIoOp* op, int fd, int flags);
    void SendMsg(TIoOp* op, int fd, int flags);
    void Send(TIoOp* op, int fd, const void* buf, size_t len);
    void Connect(TIoOp* op, int fd, const sockaddr* addr, socklen_t len);
    void Accept(TIoOp* op, int fd);
    void Cancel(TIoOp* target);

    // RemoveTickable takes effect immediately, including from inside an
    // OnTick: a tickable that removes itself or another one will not be called
    // again in that sweep.
    void AddTickable(ITickable* t);
    void RemoveTickable(ITickable* t);

private:
    void* ThreadProc() override;
    NProto::TError Init();
    TIoOp* NewOp(IIoHandler* handler, ui32 tag);
    template <typename F>
    void Guarded(const char* what, F&& f);
    void CheckSubmitResult(int ret);
    void Submit(TIoOp* op, std::function<void(io_uring_sqe*)> prep);
    void FlushPending();
    void DrainCommands();
    void Dispatch();
    void ArmEventFd();
    void ArmTimer();
};

////////////////////////////////////////////////////////////////////////////////

// Reads or writes exactly len (> 0) bytes; callback gets 0 or an errno value
// (ECONNRESET for EOF, ECANCELED after Cancel). Ring thread only; keep alive
// until the callback.
class TExactIo final: public IIoHandler
{
public:
    using TCallback = std::function<void(int error)>;

private:
    TRing& Ring;
    TIoOp* Op = nullptr;
    int Fd = -1;
    char* Buf = nullptr;
    size_t Len = 0;
    size_t Done = 0;
    bool IsRecv = false;
    bool Cancelled = false;
    TCallback Callback;

public:
    explicit TExactIo(TRing& ring);
    ~TExactIo() override;

    void Recv(int fd, void* buf, size_t len, TCallback cb);
    void Send(int fd, const void* buf, size_t len, TCallback cb);
    void Cancel();

    bool InFlight() const
    {
        return Op && Op->InFlight;
    }

private:
    void Issue();
    void OnIoComplete(TIoOp* op, int res, ui32 cqeFlags) override;
};

}   // namespace NCloud::NBlockStore::NUring
