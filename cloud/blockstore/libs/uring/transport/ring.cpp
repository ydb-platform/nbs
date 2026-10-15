#include "ring.h"

#include "socket.h"

#include <cloud/storage/core/libs/common/thread.h>

#include <util/generic/yexception.h>
#include <util/system/yassert.h>

#include <errno.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>

namespace NCloud::NBlockStore::NUring {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 TAG_EVENTFD = TAG_RESERVED_BASE + 1;
constexpr ui32 TAG_TIMER = TAG_RESERVED_BASE + 2;
constexpr ui32 TAG_CANCEL = TAG_RESERVED_BASE + 3;
constexpr ui32 CQE_BATCH = 64;
const TDuration TICK = TDuration::MilliSeconds(100);

bool IsUserOp(const TIoOp* op)
{
    return op->Tag < TAG_RESERVED_BASE;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TRing::TRing(TString name, ui32 queueSize, ESendZeroCopyMode zcMode, TLog log)
    : Name(std::move(name))
    , QueueSize(queueSize)
    , ZcMode(zcMode)
    , Log(std::move(log))
    , InitResult(NThreading::NewPromise<NProto::TError>())
{
    EventOp.Tag = TAG_EVENTFD;
    TimerOp.Tag = TAG_TIMER;
}

TRing::~TRing()
{
    Stop();
    // the fds outlive the ring thread, so an Execute that raced its exit
    // cannot write to a closed (possibly reused) descriptor
    if (EventFd >= 0) {
        ::close(EventFd);
    }
    if (TimerFd >= 0) {
        ::close(TimerFd);
    }
}

NProto::TError TRing::Start()
{
    ISimpleThread::Start();
    auto error = InitResult.GetFuture().GetValueSync();
    if (HasError(error)) {
        Join();
    }
    return error;
}

void TRing::Stop()
{
    if (!Running()) {
        return;
    }
    StopRequested = true;
    eventfd_write(EventFd, 1);
    Join();
}

void TRing::Execute(TCommand command)
{
    Commands.Enqueue(std::move(command));
    if (Sleeping.load()) {
        eventfd_write(EventFd, 1);
    }
}

bool TRing::IsRingThread() const
{
    return ThreadId.load() == TThread::CurrentThreadId();
}

NProto::TError TRing::Init()
{
    io_uring_params params{
        .flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN |
                 IORING_SETUP_COOP_TASKRUN};
    int ret = io_uring_queue_init_params(QueueSize, &Ring, &params);
    if (ret == -EINVAL) {
        params = {};
        ret = io_uring_queue_init_params(QueueSize, &Ring, &params);
    }
    if (ret < 0) {
        return MakeSystemError(-ret, "io_uring_queue_init_params");
    }
    RingInitialized = true;

    if (ZcMode != ESendZeroCopyMode::Off) {
        io_uring_probe* probe = io_uring_get_probe_ring(&Ring);
        if (probe) {
            ZcSupported =
                io_uring_opcode_supported(probe, IORING_OP_SENDMSG_ZC);
            io_uring_free_probe(probe);
        }
        if (!ZcSupported && ZcMode == ESendZeroCopyMode::Required) {
            return MakeError(
                E_ARGUMENT,
                "SendZeroCopyMode=REQUIRED but IORING_OP_SENDMSG_ZC is not "
                "supported by the kernel");
        }
        if (!ZcSupported) {
            STORAGE_WARN(
                Name << ": IORING_OP_SENDMSG_ZC is not supported, "
                     << "falling back to sendmsg");
        }
    }

    EventFd = eventfd(0, EFD_CLOEXEC);
    if (EventFd < 0) {
        return MakeSystemError(errno, "eventfd");
    }
    TimerFd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC);
    if (TimerFd < 0) {
        return MakeSystemError(errno, "timerfd_create");
    }
    itimerspec spec{};
    spec.it_interval.tv_nsec = TICK.NanoSeconds();
    spec.it_value.tv_nsec = TICK.NanoSeconds();
    if (timerfd_settime(TimerFd, 0, &spec, nullptr) < 0) {
        return MakeSystemError(errno, "timerfd_settime");
    }
    return {};
}

void* TRing::ThreadProc()
{
    NCloud::SetCurrentThreadName(Name);
    ThreadId = TThread::CurrentThreadId();

    auto error = Init();
    const bool ok = !HasError(error);
    InitResult.SetValue(std::move(error));
    if (!ok) {
        if (RingInitialized) {
            io_uring_queue_exit(&Ring);
        }
        return nullptr;
    }

    ArmEventFd();
    ArmTimer();

    for (;;) {
        DrainCommands();
        FlushPending();

        // the eventfd read stays armed while user ops are in flight, so a
        // command posted during shutdown still wakes the loop and can cancel
        // them
        if (StopRequested && !StopInitiated && UserOps == 0 &&
            Commands.IsEmpty())
        {
            StopInitiated = true;
            Cancel(&EventOp);
            Cancel(&TimerOp);
        }
        if (StopInitiated && ActiveOps == 0 && Pending.empty() &&
            Commands.IsEmpty())
        {
            break;
        }

        Sleeping = true;
        if (!Commands.IsEmpty() || !Pending.empty()) {
            Sleeping = false;
            CheckSubmitResult(io_uring_submit(&Ring));
            Dispatch();
            continue;
        }
        const int ret = io_uring_submit_and_wait(&Ring, 1);
        Sleeping = false;
        CheckSubmitResult(ret);
        Dispatch();
    }

    Draining = true;
    DrainCommands();

    io_uring_queue_exit(&Ring);
    return nullptr;
}

template <typename F>
void TRing::Guarded(const char* what, F&& f)
{
    try {
        f();
    } catch (...) {
        STORAGE_ERROR(
            Name << ": " << what << " threw: " << CurrentExceptionMessage());
    }
}

void TRing::DrainCommands()
{
    TCommand command;
    while (Commands.Dequeue(&command)) {
        Guarded("command", command);
    }
}

void TRing::CheckSubmitResult(int ret)
{
    // EBUSY and EAGAIN mean the kernel wants completions reaped first; the
    // loop does that and submits again
    if (ret < 0 && ret != -EINTR && ret != -EAGAIN && ret != -EBUSY) {
        STORAGE_ERROR(
            Name << ": io_uring submit failed: " << LastSystemErrorText(-ret));
        Y_ABORT("io_uring submit: %s", LastSystemErrorText(-ret));
    }
}

void TRing::Submit(TIoOp* op, std::function<void(io_uring_sqe*)> prep)
{
    Y_ABORT_UNLESS(
        !Draining,
        "op %u submitted by a command running during the final drain",
        op->Tag);
    Y_ABORT_UNLESS(!op->InFlight, "op %u already in flight", op->Tag);
    op->InFlight = true;
    op->Prep = std::move(prep);
    ++ActiveOps;
    if (IsUserOp(op)) {
        ++UserOps;
    }
    Pending.push_back(op);
    FlushPending();
}

void TRing::FlushPending()
{
    while (!Pending.empty()) {
        io_uring_sqe* sqe = io_uring_get_sqe(&Ring);
        if (!sqe) {
            CheckSubmitResult(io_uring_submit(&Ring));
            sqe = io_uring_get_sqe(&Ring);
            if (!sqe) {
                return;
            }
        }
        TIoOp* op = Pending.front();
        Pending.pop_front();
        op->Prep(sqe);
        op->Prep = nullptr;
        io_uring_sqe_set_data(sqe, op);
    }
}

void TRing::Dispatch()
{
    io_uring_cqe* cqes[CQE_BATCH];
    for (;;) {
        const unsigned n = io_uring_peek_batch_cqe(&Ring, cqes, CQE_BATCH);
        if (!n) {
            return;
        }
        for (unsigned i = 0; i < n; ++i) {
            auto* op = static_cast<TIoOp*>(io_uring_cqe_get_data(cqes[i]));
            const int res = cqes[i]->res;
            const ui32 flags = cqes[i]->flags;
            if (!(flags & IORING_CQE_F_MORE)) {
                op->InFlight = false;
                --ActiveOps;
                if (IsUserOp(op)) {
                    --UserOps;
                }
            }
            switch (op->Tag) {
                case TAG_EVENTFD:
                    if (!StopInitiated) {
                        ArmEventFd();
                    }
                    break;
                case TAG_TIMER:
                    if (!StopInitiated) {
                        ArmTimer();
                        const auto now = TInstant::Now();
                        TickSweep = Tickables;
                        for (auto* t: TickSweep) {
                            if (t) {
                                Guarded("OnTick", [&] { t->OnTick(now); });
                            }
                        }
                        TickSweep.clear();
                    }
                    break;
                case TAG_CANCEL:
                    FreeOp(op);
                    break;
                default:
                    Guarded(
                        "OnIoComplete",
                        [&] { op->Handler->OnIoComplete(op, res, flags); });
                    break;
            }
        }
        io_uring_cq_advance(&Ring, n);
    }
}

void TRing::ArmEventFd()
{
    Submit(
        &EventOp,
        [this](io_uring_sqe* sqe)
        { io_uring_prep_read(sqe, EventFd, &EventOp.Word, sizeof(ui64), 0); });
}

void TRing::ArmTimer()
{
    Submit(
        &TimerOp,
        [this](io_uring_sqe* sqe)
        { io_uring_prep_read(sqe, TimerFd, &TimerOp.Word, sizeof(ui64), 0); });
}

TIoOp* TRing::AllocOp(IIoHandler* handler, ui32 tag)
{
    Y_ABORT_UNLESS(
        tag < TAG_RESERVED_BASE,
        "tag %u is reserved for the ring",
        tag);
    return NewOp(handler, tag);
}

TIoOp* TRing::NewOp(IIoHandler* handler, ui32 tag)
{
    std::unique_ptr<TIoOp> op;
    if (!OpPool.empty()) {
        op = std::move(OpPool.back());
        OpPool.pop_back();
    } else {
        op = std::make_unique<TIoOp>();
    }
    op->Handler = handler;
    op->Tag = tag;
    op->InFlight = false;
    op->ZeroCopy = false;
    op->IovCount = 0;
    op->Msg = {};
    op->AddrLen = 0;
    return op.release();
}

void TRing::FreeOp(TIoOp* op)
{
    Y_ABORT_UNLESS(!op->InFlight, "freeing op %u in flight", op->Tag);
    if (OpPool.size() < 1024) {
        OpPool.emplace_back(op);
    } else {
        delete op;
    }
}

void TRing::Recv(TIoOp* op, int fd, void* buf, size_t len, int flags)
{
    Submit(
        op,
        [=](io_uring_sqe* sqe)
        { io_uring_prep_recv(sqe, fd, buf, len, flags); });
}

void TRing::RecvMsg(TIoOp* op, int fd, int flags)
{
    op->Msg = {};
    op->Msg.msg_iov = op->Iov;
    op->Msg.msg_iovlen = op->IovCount;
    Submit(
        op,
        [=](io_uring_sqe* sqe)
        { io_uring_prep_recvmsg(sqe, fd, &op->Msg, flags); });
}

void TRing::SendMsg(TIoOp* op, int fd, int flags)
{
    op->Msg = {};
    op->Msg.msg_iov = op->Iov;
    op->Msg.msg_iovlen = op->IovCount;
    const bool zc = op->ZeroCopy && ZcSupported;
    op->ZeroCopy = zc;
    Submit(
        op,
        [=](io_uring_sqe* sqe)
        {
            if (zc) {
                io_uring_prep_sendmsg_zc(
                    sqe,
                    fd,
                    &op->Msg,
                    flags | MSG_NOSIGNAL);
            } else {
                io_uring_prep_sendmsg(sqe, fd, &op->Msg, flags | MSG_NOSIGNAL);
            }
        });
}

void TRing::Send(TIoOp* op, int fd, const void* buf, size_t len)
{
    Submit(
        op,
        [=](io_uring_sqe* sqe)
        { io_uring_prep_send(sqe, fd, buf, len, MSG_NOSIGNAL); });
}

void TRing::Connect(TIoOp* op, int fd, const sockaddr* addr, socklen_t len)
{
    memcpy(&op->Addr, addr, len);
    op->AddrLen = len;
    Submit(
        op,
        [=](io_uring_sqe* sqe)
        {
            io_uring_prep_connect(
                sqe,
                fd,
                reinterpret_cast<const sockaddr*>(&op->Addr),
                op->AddrLen);
        });
}

void TRing::Accept(TIoOp* op, int fd)
{
    op->AddrLen = sizeof(op->Addr);
    Submit(
        op,
        [=](io_uring_sqe* sqe)
        {
            io_uring_prep_accept(
                sqe,
                fd,
                reinterpret_cast<sockaddr*>(&op->Addr),
                &op->AddrLen,
                SOCK_CLOEXEC);
        });
}

void TRing::Cancel(TIoOp* target)
{
    if (!target->InFlight) {
        return;
    }
    TIoOp* op = NewOp(nullptr, TAG_CANCEL);
    Submit(
        op,
        [=](io_uring_sqe* sqe)
        {
            // matches by op address: relies on Pending and the SQ being FIFO,
            // an out-of-order flush could cancel an unrelated transfer that
            // reuses it
            io_uring_prep_cancel(sqe, target, 0);
        });
}

void TRing::AddTickable(ITickable* t)
{
    Tickables.push_back(t);
}

void TRing::RemoveTickable(ITickable* t)
{
    std::erase(Tickables, t);
    std::ranges::replace(TickSweep, t, nullptr);
}

////////////////////////////////////////////////////////////////////////////////

TExactIo::TExactIo(TRing& ring)
    : Ring(ring)
{}

TExactIo::~TExactIo()
{
    Y_ABORT_UNLESS(!InFlight(), "TExactIo destroyed with an op in flight");
    if (Op) {
        Ring.FreeOp(Op);
    }
}

void TExactIo::Recv(int fd, void* buf, size_t len, TCallback cb)
{
    Fd = fd;
    Buf = static_cast<char*>(buf);
    Len = len;
    Done = 0;
    Cancelled = false;
    IsRecv = true;
    Callback = std::move(cb);
    Issue();
}

void TExactIo::Send(int fd, const void* buf, size_t len, TCallback cb)
{
    Fd = fd;
    Buf = const_cast<char*>(static_cast<const char*>(buf));
    Len = len;
    Done = 0;
    Cancelled = false;
    IsRecv = false;
    Callback = std::move(cb);
    Issue();
}

void TExactIo::Cancel()
{
    if (InFlight()) {
        Cancelled = true;
        Ring.Cancel(Op);
    }
}

void TExactIo::Issue()
{
    if (!Op) {
        Op = Ring.AllocOp(this, 0);
    }
    if (IsRecv) {
        Ring.Recv(Op, Fd, Buf + Done, Len - Done, 0);
    } else {
        Ring.Send(Op, Fd, Buf + Done, Len - Done);
    }
}

void TExactIo::OnIoComplete(TIoOp*, int res, ui32)
{
    int error = 0;
    if (res < 0) {
        error = -res;
    } else if (res == 0) {
        error = ECONNRESET;
    } else {
        Done += res;
        if (Done < Len) {
            if (!Cancelled) {
                Issue();
                return;
            }
            error = ECANCELED;
        }
    }
    auto cb = std::move(Callback);
    cb(error);
}

}   // namespace NCloud::NBlockStore::NUring
