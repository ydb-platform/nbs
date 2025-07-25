#include "context.h"

#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/common/thread_pool.h>

#include <util/string/builder.h>
#include <util/system/error.h>
#include <util/system/sanitizers.h>

namespace NCloud::NIoUring {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TDuration DefaultTimeout = TDuration::Minutes(1);

////////////////////////////////////////////////////////////////////////////////

NProto::TError MakeSystemError(int code, TStringBuf message)
{
    return MakeError(
        MAKE_SYSTEM_ERROR(code),
        TStringBuilder() << "(" << LastSystemErrorText(code) << ") "
                         << message);
}

bool IsRetriable(int error)
{
    return error == EINTR || error == EAGAIN || error == EBUSY;
}

template <auto func, typename... Ts>
int Retry(Ts... args)
{
    const TInstant deadLine = DefaultTimeout.ToDeadLine();

    int ret = 0;
    for (;;) {
        ret = func(args...);
        if (ret >= 0 || !IsRetriable(-ret)) {
            break;
        }

        if (deadLine <= Now()) {
            break;
        }

        SpinLockPause();
    }

    return ret;
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError Submit(io_uring* ring)
{
    const int ret = Retry<io_uring_submit>(ring);
    if (ret < 0) {
        return MakeSystemError(-ret, "unable to submit async IO operation");
    }

    return {};
}

NProto::TError SetMaxWorkers(io_uring* ring, ui32 bound)
{
    std::array<ui32, 2> workers{bound, 0};

    const int ret =
        Retry<io_uring_register_iowq_max_workers>(ring, workers.data());
    if (ret < 0) {
        return MakeSystemError(
            -ret,
            "unable to setup max kernel workers count");
    }

    return {};
}

NProto::TError EnableRing(io_uring* ring)
{
    const int ret = Retry<io_uring_enable_rings>(ring);
    if (ret < 0) {
        return MakeSystemError(-ret, "unable to enable the ring");
    }

    return {};
}

NProto::TError InitRing(io_uring* ring, ui32 entries, io_uring* wqOwner)
{
    io_uring_params params{
        .flags = IORING_SETUP_R_DISABLED | IORING_SETUP_SINGLE_ISSUER};

    if (wqOwner) {
        params.flags |= IORING_SETUP_ATTACH_WQ;
        params.wq_fd = wqOwner->ring_fd;
    }

    for (;;) {
        const int ret = io_uring_queue_init_params(entries, ring, &params);

        if (ret == -EINVAL && params.flags & IORING_SETUP_SINGLE_ISSUER) {
            // IORING_SETUP_SINGLE_ISSUER is not supported
            params.flags &= ~IORING_SETUP_SINGLE_ISSUER;
            continue;
        }

        if (ret < 0) {
            return MakeSystemError(-ret, "io_uring_queue_init_params");
        }

        return {};
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TContext::TContext(TParams params)
    : SubmissionThread(CreateThreadPool(params.SubmissionThreadName, 1))
    , CompletionThread(
          std::bind_front(
              &TContext::CompletionThreadProc,
              this,
              std::move(params.CompletionThreadName)))
{
    const auto error = InitRing(
        &Ring,
        params.SubmissionQueueEntries,
        params.WqOwner ? &params.WqOwner->Ring : nullptr);

    Y_ABORT_IF(
        HasError(error),
        "can't initialize the ring: %s",
        FormatError(error).c_str());

    if (!params.WqOwner && params.MaxKernelWorkersCount) {
        const auto error = SetMaxWorkers(&Ring, params.MaxKernelWorkersCount);
        Y_ABORT_IF(
            HasError(error),
            "can't set max workers: %s",
            FormatError(error).c_str());
    }
}

TContext::~TContext()
{
    Stop();

    io_uring_queue_exit(&Ring);
    Ring = {};
}

void TContext::Start()
{
    SubmissionThread->Start();

    // Since IORING_SETUP_R_DISABLED and IORING_SETUP_SINGLE_ISSUER
    // are set, we must enable the ring in the context of the
    // submission thread.
    Started = SubmissionThread->Execute(
        [this]
        {
            const auto error = EnableRing(&Ring);
            Y_ABORT_IF(
                HasError(error),
                "can't enable the ring: %s",
                FormatError(error).c_str());

            // Start the completion thread only after enabling the ring.
            CompletionThread.Start();
        });
}

void TContext::Stop()
{
    if (!Started.Initialized()) {
        return;
    }

    Started.Wait();
    Started = {};

    SubmissionThread->ExecuteSimple([this] { SubmitStopSignal(); });

    CompletionThread.Join();
    SubmissionThread->Stop();
}

void TContext::AsyncIO(
    int op,
    int fd,
    const void* addr,
    ui32 len,
    ui64 offset,
    TFileIOCompletion* completion,
    ui32 flags)
{
    SubmissionThread->ExecuteSimple(
        [=, this] { SubmitIO(op, fd, addr, len, offset, completion, flags); });
}

void TContext::AsyncNOP(TFileIOCompletion* completion, ui32 flags)
{
    SubmissionThread->ExecuteSimple([=, this] { SubmitNOP(completion, flags); });
}

void TContext::PostCompletion(TFileIOCompletion* completion, int res)
{
    SubmissionThread->ExecuteSimple([=, this] { SubmitMsg(completion, res); });
}

void TContext::SubmitMsg(TFileIOCompletion* completion, int res)
{
    io_uring_sqe* sqe = io_uring_get_sqe(&Ring);
    if (!sqe) {
        completion->Func(completion, MakeError(E_REJECTED, "Overloaded"), 0);
        return;
    }

    io_uring_prep_msg_ring(
        sqe,
        Ring.ring_fd,
        res,
        std::bit_cast<ui64>(completion),
        0);   // flags
    io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS);
    io_uring_sqe_set_data(sqe, completion);

    NSan::Release(completion);

    // TODO(sharpeye): more resilent error handling for io_uring_submit
    const auto error = Submit(&Ring);
    Y_ABORT_IF(
        HasError(error),
        "can't submit IO: %s",
        FormatError(error).c_str());
}

void TContext::SubmitIO(
    int op,
    int fd,
    const void* addr,
    ui32 len,
    ui64 offset,
    TFileIOCompletion* completion,
    ui32 flags)
{
    io_uring_sqe* sqe = io_uring_get_sqe(&Ring);
    if (!sqe) {
        completion->Func(completion, MakeError(E_REJECTED, "Overloaded"), 0);
        return;
    }

    io_uring_prep_rw(op, sqe, fd, addr, len, offset);
    io_uring_sqe_set_data(sqe, completion);
    io_uring_sqe_set_flags(sqe, flags);

    NSan::Release(completion);

    // TODO(sharpeye): more resilent error handling for io_uring_submit
    const auto error = Submit(&Ring);
    Y_ABORT_IF(
        HasError(error),
        "can't submit IO: %s",
        FormatError(error).c_str());
}

void TContext::SubmitNOP(TFileIOCompletion* completion, ui32 flags)
{
    io_uring_sqe* sqe = io_uring_get_sqe(&Ring);
    if (!sqe) {
        completion->Func(completion, MakeError(E_REJECTED, "Overloaded"), 0);
        return;
    }

    io_uring_prep_nop(sqe);
    io_uring_sqe_set_data(sqe, completion);
    io_uring_sqe_set_flags(sqe, flags);

    NSan::Release(completion);

    // TODO(sharpeye): more resilent error handling for io_uring_submit
    const auto error = Submit(&Ring);
    Y_ABORT_IF(
        HasError(error),
        "can't submit NOP: %s",
        FormatError(error).c_str());
}

void TContext::SubmitStopSignal()
{
    for (;;) {
        io_uring_sqe* sqe = io_uring_get_sqe(&Ring);
        if (sqe) {
            io_uring_prep_nop(sqe);
            io_uring_sqe_set_data(sqe, nullptr);

            const auto error = Submit(&Ring);
            Y_ABORT_IF(
                HasError(error),
                "can't submit a stop signal: %s",
                FormatError(error).c_str());
            break;
        }

        // the submission queue is full, so we're waiting
        SpinLockPause();
    }
}

void TContext::ProcessCompletion(io_uring_cqe* cqe)
{
    void* data = io_uring_cqe_get_data(cqe);
    Y_DEBUG_ABORT_UNLESS(data);

    if (!data) {
        return;
    }

    auto* completion = static_cast<TFileIOCompletion*>(data);
    NSan::Acquire(completion);

    if (cqe->res < 0) {
        completion->Func(
            completion,
            MakeSystemError(-cqe->res, "async IO operation failed"),
            0);
    } else {
        completion->Func(completion, {}, cqe->res);
    }
}

void TContext::CompletionThreadProc(const TString& threadName)
{
    SetHighestThreadPriority();
    NCloud::SetCurrentThreadName(threadName);

    for (;;) {
        io_uring_cqe* cqe = nullptr;
        const int ret = io_uring_wait_cqe(&Ring, &cqe);
        if (ret < 0) {
            Y_ABORT_UNLESS(
                IsRetriable(-ret),
                "io_uring_wait_cqe: %s (%d)",
                LastSystemErrorText(-ret),
                -ret);
            continue;
        }

        if (!io_uring_cqe_get_data(cqe)) {
            break;
        }

        ProcessCompletion(cqe);

        io_uring_cqe_seen(&Ring, cqe);
    }
}

void TContext::AsyncWrite(
    int fd,
    TArrayRef<const char> buffer,
    ui64 offset,
    TFileIOCompletion* completion,
    ui32 flags)
{
    AsyncIO(
        IORING_OP_WRITE,
        fd,
        buffer.data(),
        buffer.size(),
        offset,
        completion,
        flags);
}

void TContext::AsyncRead(
    int fd,
    TArrayRef<char> buffer,
    ui64 offset,
    TFileIOCompletion* completion,
    ui32 flags)
{
    AsyncIO(
        IORING_OP_READ,
        fd,
        buffer.data(),
        buffer.size(),
        offset,
        completion,
        flags);
}

void TContext::AsyncWriteV(
    int fd,
    TArrayRef<const TArrayRef<const char>> buffers,
    ui64 offset,
    TFileIOCompletion* completion,
    ui32 flags)
{
    AsyncIO(
        IORING_OP_WRITEV,
        fd,
        buffers.data(),
        buffers.size(),
        offset,
        completion,
        flags);
}

void TContext::AsyncReadV(
    int fd,
    TArrayRef<const TArrayRef<char>> buffers,
    ui64 offset,
    TFileIOCompletion* completion,
    ui32 flags)
{
    AsyncIO(
        IORING_OP_READV,
        fd,
        buffers.data(),
        buffers.size(),
        offset,
        completion,
        flags);
}

}   // namespace NCloud::NIoUring
