#include "service.h"

#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/common/thread_pool.h>

#include <util/generic/scope.h>
#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/system/error.h>
#include <util/system/file.h>
#include <util/system/sanitizers.h>
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

NProto::TError SetMaxWorkers(io_uring* ring, ui32 bound, ui32 unbound)
{
    ui32 workers[2] = {bound, unbound};

    const int ret = io_uring_register_iowq_max_workers(ring, workers);
    if (ret < 0) {
        return MakeSystemError(-ret, "io_uring_register_iowq_max_workers");
    }
    return {};
}

NProto::TError InitRing(io_uring* ring, ui32 entries, io_uring* other)
{
    io_uring_params params{
        .flags = IORING_SETUP_R_DISABLED | IORING_SETUP_SINGLE_ISSUER};

    if (other) {
        params.flags |= IORING_SETUP_ATTACH_WQ;
        params.wq_fd = other->ring_fd;
    }

    int ret = io_uring_queue_init_params(entries, ring, &params);
    if (ret == -EINVAL) {
        // IORING_SETUP_SINGLE_ISSUER is not supported
        params.flags = params.flags & ~IORING_SETUP_SINGLE_ISSUER;
        ret = io_uring_queue_init_params(entries, ring, &params);
    }

    if (ret < 0) {
        return MakeSystemError(-ret, "io_uring_queue_init_params");
    }

    return {};
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

////////////////////////////////////////////////////////////////////////////////

class TIoUring
{
private:
    io_uring Ring = {};

    ITaskQueuePtr SubmissionThread;
    TThread CompletionThread;

public:
    explicit TIoUring(TIoUringServiceParams params, TIoUring* other = nullptr)
        : SubmissionThread(CreateThreadPool(params.SubmissionThreadName, 1))
        , CompletionThread(
              [this, threadName = std::move(params.CompletionThreadName)]
              {
                  SetHighestThreadPriority();
                  ::NCloud::SetCurrentThreadName(threadName);
                  ProcessCompletionQueue();
              })
    {
        io_uring* otherRing = nullptr;
        if (other) {
            otherRing = &other->Ring;
        }

        const auto error =
            InitRing(&Ring, params.SubmissionQueueEntries, otherRing);

        Y_ABORT_IF(
            HasError(error),
            "can't initialize the ring: %s",
            FormatError(error).c_str());

        if (!otherRing && (params.BoundWorkers || params.UnboundWorkers)) {
            const auto error = SetMaxWorkers(
                &Ring,
                params.BoundWorkers,
                params.UnboundWorkers);
            Y_ABORT_IF(
                HasError(error),
                "can't set max workers: %s",
                FormatError(error).c_str());
        }
    }

    ~TIoUring()
    {
        io_uring_queue_exit(&Ring);
        Ring = {};
    }

    void Start()
    {
        SubmissionThread->Start();
        SubmissionThread->ExecuteSimple(
            [this]
            {
                // Since IORING_SETUP_R_DISABLED and IORING_SETUP_SINGLE_ISSUER
                // are set, we must enable the ring and register a stop event in
                // the context of the submission thread.
                if (auto error = EnableRing(&Ring); HasError(error)) {
                    Y_ABORT(
                        "can't enable the ring: %s",
                        FormatError(error).c_str());
                }

                CompletionThread.Start();
            });
    }

    void Stop()
    {
        if (!CompletionThread.Running()) {
            return;
        }

        // Send a stop signal
        AsyncNOP(nullptr);

        CompletionThread.Join();
        SubmissionThread->Stop();
    }

    void AsyncIO(
        int op,
        int fd,
        const void* addr,
        ui32 len,
        ui64 offset,
        TFileIOCompletion* completion)
    {
        SubmissionThread->ExecuteSimple([=, this] {
            SubmitIO(op, fd, addr, len, offset, completion);
        });
    }

    void AsyncNOP(TFileIOCompletion* completion)
    {
        SubmissionThread->ExecuteSimple([=, this] {
            SubmitNOP(completion);
        });
    }

private:
    void SubmitIO(
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

    void SubmitNOP(TFileIOCompletion* completion)
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

    void ProcessCompletion(io_uring_cqe* cqe)
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

    void ProcessCompletionQueue()
    {
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
};

////////////////////////////////////////////////////////////////////////////////

class TIoUringService final
    : public IFileIOService
{
private:
    TIoUring IoUring;

public:
    explicit TIoUringService(TIoUringServiceParams params)
        : IoUring(std::move(params))
    {}

    TIoUringService(
            TIoUringServiceParams params,
            TIoUringService& other)
        : IoUring(std::move(params), &other.IoUring)
    {}

    void Start() final
    {
        IoUring.Start();
    }

    void Stop() final
    {
        IoUring.Stop();
    }

    void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        TFileIOCompletion* completion) final
    {
        IoUring.AsyncIO(
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
        IoUring.AsyncIO(
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
        IoUring.AsyncIO(
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
        IoUring.AsyncIO(
            IORING_OP_WRITEV,
            file,
            buffers.data(),
            buffers.size(),
            offset,
            completion);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TIoUringServiceNull final
    : public IFileIOService
{
private:
    TIoUring IoUring;

public:
    explicit TIoUringServiceNull(TIoUringServiceParams params)
        : IoUring(std::move(params))
    {}

    TIoUringServiceNull(
            TIoUringServiceParams params,
            TIoUringServiceNull& other)
        : IoUring(std::move(params), &other.IoUring)
    {}

    void Start() final
    {
        IoUring.Start();
    }

    void Stop() final
    {
        IoUring.Stop();
    }

    void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset, buffer);

        IoUring.AsyncNOP(completion);
    }

    void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset, buffers);

        IoUring.AsyncNOP(completion);
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset, buffer);

        IoUring.AsyncNOP(completion);
    }

    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset, buffers);

        IoUring.AsyncNOP(completion);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TIoUringServiceFactory
{
private:
    const TIoUringServiceParams Params;

    std::shared_ptr<T> RootService;
    ui32 Index = 0;

public:
    explicit TIoUringServiceFactory(TIoUringServiceParams params)
        : Params(std::move(params))
    {}

    IFileIOServicePtr operator()()
    {
        return std::static_pointer_cast<IFileIOService>(CreateService());
    }

private:
    IFileIOServicePtr CreateService()
    {
        const ui32 index = Index++;

        TIoUringServiceParams params = Params;
        params.SubmissionThreadName = TStringBuilder()
                                      << Params.SubmissionThreadName << index;
        params.CompletionThreadName = TStringBuilder()
                                      << Params.CompletionThreadName << index;

        if (!RootService) {
            RootService = std::make_shared<T>(std::move(params));
            return RootService;
        }

        return std::make_shared<T>(std::move(params), *RootService);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileIOServicePtr CreateIoUringService(TIoUringServiceParams params)
{
    return std::make_shared<TIoUringService>(std::move(params));
}

IFileIOServicePtr CreateIoUringServiceNull(TIoUringServiceParams params)
{
    return std::make_shared<TIoUringServiceNull>(std::move(params));
}

std::function<IFileIOServicePtr()> CreateIoUringServiceFactory(
    TIoUringServiceParams params)
{
    return TIoUringServiceFactory<TIoUringService>{std::move(params)};
}

std::function<IFileIOServicePtr()> CreateIoUringServiceNullFactory(
    TIoUringServiceParams params)
{
    return TIoUringServiceFactory<TIoUringServiceNull>{std::move(params)};
}

}   // namespace NCloud
