#include "context.h"

#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/common/thread_pool.h>

#include <library/cpp/threading/future/future.h>

#include <util/string/builder.h>
#include <util/system/error.h>
#include <util/system/sanitizers.h>

namespace NCloud {

namespace {

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

NProto::TError Submit(io_uring* ring)
{
    for (;;) {
        const int ret = io_uring_submit(ring);
        if (ret >= 0) {
            return {};
        }

        if (!IsRetriable(-ret)) {
            return MakeSystemError(-ret, "unable to submit async IO operation");
        }
    }
}

NProto::TError SetMaxWorkers(io_uring* ring, ui32 bound)
{
    ui32 workers[2] = {bound, 0};

    const int ret = io_uring_register_iowq_max_workers(ring, workers);
    if (ret < 0) {
        return MakeSystemError(-ret, "io_uring_register_iowq_max_workers");
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

NProto::TError EnableRing(io_uring* ring)
{
    for (;;) {
        const int ret = io_uring_enable_rings(ring);
        if (!ret) {
            return {};
        }

        if (!IsRetriable(-ret)) {
            return MakeSystemError(-ret, "io_uring_enable_rings");
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TIoUring::TIoUring(TParams params, TIoUring* wqOwner)
    : SubmissionThread(CreateThreadPool(params.SubmissionThreadName, 1))
    , CompletionThread(
          std::bind_front(
              &TIoUring::CompletionThreadProc,
              this,
              std::move(params.CompletionThreadName)))
{
    const auto error = InitRing(
        &Ring,
        params.SubmissionQueueEntries,
        wqOwner ? &wqOwner->Ring : nullptr);

    Y_ABORT_IF(
        HasError(error),
        "can't initialize the ring: %s",
        FormatError(error).c_str());

    if (!wqOwner && params.MaxKernelWorkersCount) {
        const auto error = SetMaxWorkers(&Ring, params.MaxKernelWorkersCount);
        Y_ABORT_IF(
            HasError(error),
            "can't set max workers: %s",
            FormatError(error).c_str());
    }
}

TIoUring::~TIoUring()
{
    Stop();

    io_uring_queue_exit(&Ring);
    Ring = {};
}

void TIoUring::Start()
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

void TIoUring::Stop()
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

void TIoUring::AsyncIO(
    int op,
    int fd,
    const void* addr,
    ui32 len,
    ui64 offset,
    TFileIOCompletion* completion)
{
    SubmissionThread->ExecuteSimple(
        [=, this] { SubmitIO(op, fd, addr, len, offset, completion); });
}

void TIoUring::AsyncNOP(TFileIOCompletion* completion)
{
    SubmissionThread->ExecuteSimple([=, this] { SubmitNOP(completion); });
}

void TIoUring::SubmitIO(
    int op,
    int fd,
    const void* addr,
    ui32 len,
    ui64 offset,
    TFileIOCompletion* completion)
{
    io_uring_sqe* sqe = io_uring_get_sqe(&Ring);
    if (!sqe) {
        completion->Func(
            completion,
            MakeError(E_REJECTED, "Overloaded"),
            0);
        return;
    }

    io_uring_prep_rw(op, sqe, fd, addr, len, offset);
    io_uring_sqe_set_data(sqe, completion);

    NSan::Release(completion);

    const auto error = Submit(&Ring);
    if (HasError(error)) {
        completion->Func(completion, error, 0);
    }
}

void TIoUring::SubmitNOP(TFileIOCompletion* completion)
{
    io_uring_sqe* sqe = io_uring_get_sqe(&Ring);
    if (!sqe) {
        completion->Func(
            completion,
            MakeError(E_REJECTED, "Overloaded"),
            0);
        return;
    }

    io_uring_prep_nop(sqe);
    io_uring_sqe_set_data(sqe, completion);

    NSan::Release(completion);

    const auto error = Submit(&Ring);
    if (HasError(error)) {
        completion->Func(completion, error, 0);
    }
}

void TIoUring::SubmitStopSignal()
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

void TIoUring::ProcessCompletion(io_uring_cqe* cqe)
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

void TIoUring::CompletionThreadProc(const TString& threadName)
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

void TIoUring::AsyncWrite(
    int fd,
    TArrayRef<const char> buffer,
    ui64 offset,
    TFileIOCompletion* completion)
{
    AsyncIO(
        IORING_OP_WRITE,
        fd,
        buffer.data(),
        buffer.size(),
        offset,
        completion);
}

void TIoUring::AsyncRead(
    int fd,
    TArrayRef<char> buffer,
    ui64 offset,
    TFileIOCompletion* completion)
{
    AsyncIO(
        IORING_OP_READ,
        fd,
        buffer.data(),
        buffer.size(),
        offset,
        completion);
}

void TIoUring::AsyncWriteV(
    int fd,
    TArrayRef<const TArrayRef<const char>> buffers,
    ui64 offset,
    TFileIOCompletion* completion)
{
    AsyncIO(
        IORING_OP_WRITEV,
        fd,
        buffers.data(),
        buffers.size(),
        offset,
        completion);
}

void TIoUring::AsyncReadV(
    int fd,
    TArrayRef<const TArrayRef<char>> buffers,
    ui64 offset,
    TFileIOCompletion* completion)
{
    AsyncIO(
        IORING_OP_READV,
        fd,
        buffers.data(),
        buffers.size(),
        offset,
        completion);
}

}   // namespace NCloud
