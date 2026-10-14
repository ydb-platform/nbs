#pragma once

#include "public.h"
#include "protocol.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/thread.h>
#include <util/thread/lfqueue.h>

#include <library/cpp/threading/future/future.h>

#include <liburing.h>

#include <atomic>
#include <functional>
#include <memory>
#include <sys/socket.h>

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

    // Not safe to call concurrently with itself; must not be called from the
    // ring thread.
    void Stop();

    // The ring thread does not exit while commands are queued, so a command
    // enqueued during shutdown runs in the live loop and may start IO. One that
    // slips in after the loop's last check runs once more during the final
    // drain and must not start IO (Submit aborts). A command enqueued after the
    // ring thread has exited (after Stop() has returned, or a failed Start())
    // is dropped: it cannot be run on a ring that is gone.
    void Execute(TCommand command);
    bool IsRingThread() const;
    bool SendZeroCopySupported() const { return ZcSupported; }

    // Includes the ring's own eventfd and timer reads, so an idle ring reports
    // 2.
    size_t InFlightOps() const { return ActiveOps.load(); }

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
    bool InFlight() const { return Op && Op->InFlight; }

private:
    void Issue();
    void OnIoComplete(TIoOp* op, int res, ui32 cqeFlags) override;
};

}   // namespace NCloud::NBlockStore::NUring
