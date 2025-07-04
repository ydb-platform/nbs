#include "service.h"

#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/thread.h>

#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/system/error.h>
#include <util/system/file.h>
#include <util/system/thread.h>

#include <liburing.h>
#include <sys/eventfd.h>

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

////////////////////////////////////////////////////////////////////////////////

class TRing
{
private:
    io_uring Ring;

public:
    explicit TRing(ui32 submissionQueueEntries)
    {
        const int ret = io_uring_queue_init(submissionQueueEntries, &Ring, 0);
        Y_ABORT_UNLESS(
            ret == 0,
            "io_uring_queue_init: %s (%d)",
            LastSystemErrorText(-ret),
            -ret);
    }

    ~TRing()
    {
        io_uring_queue_exit(&Ring);
    }

    operator io_uring* ()
    {
        return &Ring;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TEvent
{
private:
    const TFileHandle Fd;
    ui64 Value = 0;

public:
    TEvent()
        : Fd(eventfd(0, EFD_CLOEXEC))
    {
        const int error = errno;
        Y_ABORT_UNLESS(
            Fd.IsOpen(),
            "eventfd: %s (%d)",
            LastSystemErrorText(error),
            error);
    }

    void Signal()
    {
        while (eventfd_write(Fd, 1)) {
            const int error = errno;
            Y_ABORT_UNLESS(
                IsRetriable(error),
                "eventfd_write: %s (%d)",
                LastSystemErrorText(error),
                error);
        }
    }

    [[nodiscard]] NProto::TError Register(TRing& ring)
    {
        io_uring_sqe* sqe = io_uring_get_sqe(ring);
        if (!sqe) {
            return MakeError(E_INVALID_STATE, "submission queue is full");
        }

        io_uring_prep_read(sqe, Fd, &Value, sizeof(Value), 0);
        io_uring_sqe_set_data(sqe, this);

        for (;;) {
            const int ret = io_uring_submit(ring);
            if (ret >= 0) {
                break;
            }

            if (!IsRetriable(-ret)) {
                return MakeSystemError(-ret, "io_uring_submit");
            }
        }
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCompletionThread final: public ISimpleThread
{
private:
    const TString Name;
    TRing& Ring;
    TEvent StopEvent;

public:
    TCompletionThread(TString name, TRing& ring)
        : Name(std::move(name))
        , Ring(ring)
    {}

    void Start()
    {
        // The very first submission should always work fine
        const auto error = StopEvent.Register(Ring);
        Y_ABORT_IF(
            HasError(error),
            "can't register a stop event: %s",
            FormatError(error).c_str());

        ISimpleThread::Start();
    }

    void Stop()
    {
        if (!Running()) {
            return;
        }

        StopEvent.Signal();
        Join();
    }

private:
    void ProcessCompletion(io_uring_cqe* cqe)
    {
        void* data = io_uring_cqe_get_data(cqe);
        Y_DEBUG_ABORT_UNLESS(data);

        if (!data) {
            return;
        }

        auto* completion = static_cast<TFileIOCompletion*>(data);

        if (cqe->res < 0) {
            completion->Func(
                completion,
                MakeSystemError(-cqe->res, "async IO operation failed"),
                0);
        } else {
            completion->Func(completion, {}, cqe->res);
        }
    }

    void* ThreadProc() final
    {
        SetHighestThreadPriority();
        NCloud::SetCurrentThreadName(Name);

        for (;;) {
            io_uring_cqe* cqe = nullptr;
            const int ret = io_uring_wait_cqe(Ring, &cqe);
            if (ret < 0) {
                Y_ABORT_UNLESS(
                    IsRetriable(-ret),
                    "io_uring_wait_cqe: %s (%d)",
                    LastSystemErrorText(-ret),
                    -ret);
                continue;
            }

            if (io_uring_cqe_get_data(cqe) == &StopEvent) {
                break;
            }

            ProcessCompletion(cqe);

            io_uring_cqe_seen(Ring, cqe);
        }

        return nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TIoUringService final
    : public IFileIOService
{
private:
    TRing Ring;
    TCompletionThread CompletionThread;

public:
    TIoUringService(TString completionThreadName, ui32 submissionQueueEntries)
        : Ring(submissionQueueEntries)
        , CompletionThread(std::move(completionThreadName), Ring)
    {}

    void Start() final
    {
        CompletionThread.Start();
    }

    void Stop() final
    {
        CompletionThread.Stop();
    }

    void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        TFileIOCompletion* completion) final
    {
        SubmitIO(
            IORING_OP_READ,
            file,
            buffer.data(),
            buffer.size(),
            offset,
            completion);
    }

    void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) final
    {
        SubmitIO(
            IORING_OP_READV,
            file,
            buffers.data(),
            buffers.size(),
            offset,
            completion);
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) final
    {
        SubmitIO(
            IORING_OP_WRITE,
            file,
            buffer.data(),
            buffer.size(),
            offset,
            completion);
    }

    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion) final
    {
        SubmitIO(
            IORING_OP_WRITEV,
            file,
            buffers.data(),
            buffers.size(),
            offset,
            completion);
    }

private:
    void SubmitIO(
        int op,
        int fd,
        const void* addr,
        unsigned len,
        ui64 offset,
        TFileIOCompletion* completion)
    {
        io_uring_sqe* sqe = io_uring_get_sqe(Ring);
        if (!sqe) {
            completion->Func(
                completion,
                MakeError(E_REJECTED, "Overloaded"),
                0);
            return;
        }

        io_uring_prep_rw(op, sqe, fd, addr, len, offset);
        io_uring_sqe_set_data(sqe, completion);

        const auto error = Submit(Ring);
        if (HasError(error)) {
            completion->Func(completion, error, 0);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TIoUringServiceNull final
    : public IFileIOService
{
private:
    TRing Ring;
    TCompletionThread CompletionThread;

public:
    TIoUringServiceNull(
            TString completionThreadName,
            ui32 submissionQueueEntries)
        : Ring(submissionQueueEntries)
        , CompletionThread(std::move(completionThreadName), Ring)
    {}

    void Start() final
    {
        CompletionThread.Start();
    }

    void Stop() final
    {
        CompletionThread.Stop();
    }

    void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset, buffer);

        SubmitNOP(completion);
    }

    void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset, buffers);

        SubmitNOP(completion);
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset, buffer);

        SubmitNOP(completion);
    }

    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset, buffers);

        SubmitNOP(completion);
    }

private:
    void SubmitNOP(TFileIOCompletion* completion)
    {
        io_uring_sqe* sqe = io_uring_get_sqe(Ring);
        if (!sqe) {
            completion->Func(
                completion,
                MakeError(E_REJECTED, "Overloaded"),
                0);
            return;
        }

        io_uring_prep_nop(sqe);
        io_uring_sqe_set_data(sqe, completion);

        const auto error = Submit(Ring);
        if (HasError(error)) {
            completion->Func(completion, error, 0);
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileIOServicePtr CreateIoUringService(
    TString completionThreadName,
    ui32 submissionQueueEntries)
{
    return std::make_shared<TIoUringService>(
        std::move(completionThreadName),
        submissionQueueEntries);
}

IFileIOServicePtr CreateIoUringServiceNull(
    TString completionThreadName,
    ui32 submissionQueueEntries)
{
    return std::make_shared<TIoUringServiceNull>(
        std::move(completionThreadName),
        submissionQueueEntries);
}

}   // namespace NCloud
