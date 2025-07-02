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

#include <mutex>
#include <thread>

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

////////////////////////////////////////////////////////////////////////////////

class TCompletionThread final: public ISimpleThread
{
private:
    const TString Name;
    io_uring* const Ring;
    TFileHandle StopFd;

public:
    TCompletionThread(TString name, io_uring& ring)
        : Name(std::move(name))
        , Ring(&ring)
        , StopFd(eventfd(0, EFD_CLOEXEC))
    {
        Y_ABORT_UNLESS(StopFd.IsOpen());
    }

    void Stop()
    {
        int ret = eventfd_write(StopFd, 1);
        Y_ABORT_UNLESS(!ret);
    }

private:
    void* ThreadProc() final
    {
        SetHighestThreadPriority();
        NCloud::SetCurrentThreadName(Name);

        uint64_t shouldStop = 0;
        ReadStopEvent(shouldStop);

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

            void* data = io_uring_cqe_get_data(cqe);
            if (data == &shouldStop) {
                break;
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

            io_uring_cqe_seen(Ring, cqe);
        }

        return nullptr;
    }

    void ReadStopEvent(uint64_t& shouldStop)
    {
        io_uring_sqe* sqe = io_uring_get_sqe(Ring);
        Y_ABORT_UNLESS(sqe);

        io_uring_prep_read(
            sqe,
            StopFd,
            &shouldStop,
            sizeof(shouldStop),
            0);
        io_uring_sqe_set_data(sqe, &shouldStop);

        for (;;) {
            const int ret = io_uring_submit(Ring);
            if (ret >= 0) {
                break;
            }
            Y_ABORT_UNLESS(IsRetriable(-ret));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TIoUringService final
    : public IFileIOService
{
private:
    io_uring Ring = {};
    TCompletionThread CQ;

public:
    TIoUringService(TString completionThreadName, ui32 size)
        : CQ(std::move(completionThreadName), Ring)
    {
        int ret = io_uring_queue_init(size, &Ring, 0);
        Y_ABORT_UNLESS(
            ret == 0,
            "io_uring_queue_init: %s (%d)",
            LastSystemErrorText(-ret),
            -ret);
    }

    ~TIoUringService() final
    {
        io_uring_queue_exit(&Ring);
    }

    void Start() final
    {
        CQ.Start();
    }

    void Stop() final
    {
        if (!CQ.Running()) {
            return;
        }

        CQ.Stop();
        CQ.Join();
    }

    void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        TFileIOCompletion* completion) final
    {
        Submit(
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
        Submit(
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
        Submit(
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
        Submit(
            IORING_OP_WRITEV,
            file,
            buffers.data(),
            buffers.size(),
            offset,
            completion);
    }

private:
    void Submit(
        int op,
        int fd,
        const void* addr,
        unsigned len,
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

        for (;;) {
            const int ret = io_uring_submit(&Ring);
            if (ret >= 0) {
                break;
            }

            if (!IsRetriable(-ret)) {
                completion->Func(
                    completion,
                    MakeSystemError(
                        -ret,
                        "unable to submit async IO operation"),
                    0);
                break;
            }
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileIOServicePtr CreateIoUringService(
    TString completionThreadName,
    ui32 ringSize)
{
    return std::make_shared<TIoUringService>(
        std::move(completionThreadName),
        ringSize);
}

}   // namespace NCloud
