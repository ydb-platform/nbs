#include "context.h"

#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/common/thread_pool.h>

#include <util/generic/scope.h>
#include <util/string/builder.h>
#include <util/system/error.h>
#include <util/system/sanitizers.h>

#include <linux/nvme_ioctl.h>

namespace NCloud::NIoUring {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TDuration DefaultTimeout = TDuration::Minutes(1);

////////////////////////////////////////////////////////////////////////////////

struct TNvmeUringCmd
{
    ui8 opcode;
    ui8 flags;
    ui16 rsvd1;
    ui32 nsid;
    ui32 cdw2;
    ui32 cdw3;
    ui64 metadata;
    ui64 addr;
    ui32 metadata_len;
    ui32 data_len;
    ui32 cdw10;
    ui32 cdw11;
    ui32 cdw12;
    ui32 cdw13;
    ui32 cdw14;
    ui32 cdw15;
    ui32 timeout_ms;
    ui32 rsvd2;
};

enum ENvmeOpcode
{
    Write = 0x01,
    Read = 0x02,
    WriteZeroes = 0x08
};

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

NProto::TError InitRing(
    io_uring* ring,
    ui32 entries,
    io_uring* wqOwner,
    bool bigRing)
{
    io_uring_params params{
        .flags = IORING_SETUP_R_DISABLED | IORING_SETUP_SINGLE_ISSUER};

    if (bigRing) {
        params.flags |= IORING_SETUP_SQE128 | IORING_SETUP_CQE32;
    }

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
        params.WqOwner ? &params.WqOwner->Ring : nullptr,
        params.EnableNvmePassthrough);

    if (HasError(error)) {
        ythrow TSystemError(error.GetCode())
            << "can't initialize the ring: " << error.GetMessage();
    }

    if (!params.WqOwner && params.MaxKernelWorkersCount) {
        const auto error = SetMaxWorkers(&Ring, params.MaxKernelWorkersCount);
        if (HasError(error)) {
            ythrow TSystemError(error.GetCode())
                << "can't set max workers: " << error.GetMessage();
        }
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

void TContext::AsyncNvmeWriteZeroes(
    int fd,
    int nsId,
    ui32 lbaShift,
    ui64 len,
    ui64 offset,
    TFileIOCompletion* completion,
    ui32 flags)
{
    AsyncNvmeIO(
        ENvmeOpcode::WriteZeroes,
        NVME_URING_CMD_IO,
        fd,
        nsId,
        lbaShift,
        nullptr,   // addr
        len,
        len,
        offset,
        completion,
        flags);
}

void TContext::AsyncNvmeWrite(
    int fd,
    int nsId,
    ui32 lbaShift,
    TArrayRef<const char> buffer,
    ui64 offset,
    TFileIOCompletion* completion,
    ui32 flags)
{
    AsyncNvmeIO(
        ENvmeOpcode::Write,
        NVME_URING_CMD_IO,
        fd,
        nsId,
        lbaShift,
        buffer.data(),
        buffer.size(),
        buffer.size(),
        offset,
        completion,
        flags);
}

void TContext::AsyncNvmeRead(
    int fd,
    int nsId,
    ui32 lbaShift,
    TArrayRef<char> buffer,
    ui64 offset,
    TFileIOCompletion* completion,
    ui32 flags)
{
    AsyncNvmeIO(
        ENvmeOpcode::Read,
        NVME_URING_CMD_IO,
        fd,
        nsId,
        lbaShift,
        buffer.data(),
        buffer.size(),
        buffer.size(),
        offset,
        completion,
        flags);
}

void TContext::AsyncNvmeReadV(
    int fd,
    int nsId,
    ui32 lbaShift,
    TArrayRef<const TArrayRef<char>> buffers,
    ui64 offset,
    TFileIOCompletion* completion,
    ui32 flags)
{
    ui64 totalLen = 0;
    for (const auto buf: buffers) {
        totalLen += buf.size();
    }

    AsyncNvmeIO(
        ENvmeOpcode::Read,
        NVME_URING_CMD_IO_VEC,
        fd,
        nsId,
        lbaShift,
        buffers.data(),
        buffers.size(),
        totalLen,
        offset,
        completion,
        flags);
}

void TContext::AsyncNvmeWriteV(
    int fd,
    int nsId,
    ui32 lbaShift,
    TArrayRef<const TArrayRef<const char>> buffers,
    ui64 offset,
    TFileIOCompletion* completion,
    ui32 flags)
{
    ui64 totalLen = 0;
    for (const auto buf: buffers) {
        totalLen += buf.size();
    }

    AsyncNvmeIO(
        ENvmeOpcode::Write,
        NVME_URING_CMD_IO_VEC,
        fd,
        nsId,
        lbaShift,
        buffers.data(),
        buffers.size(),
        totalLen,
        offset,
        completion,
        flags);
}

void TContext::AsyncNvmeIO(
    ui8 op,
    ui32 cmd,
    int fd,
    ui32 nsId,
    ui32 lbaShift,
    const void* addr,
    ui32 dataLen,
    ui64 totalLen,
    ui64 offset,
    TFileIOCompletion* completion,
    ui32 flags)
{
    SubmissionThread->ExecuteSimple([=, this] {
        SubmitNvmeIO(
            op,
            cmd,
            fd,
            nsId,
            lbaShift,
            addr,
            dataLen,
            totalLen,
            offset,
            completion,
            flags);
    });
}

void TContext::SubmitNvmeIO(
    ui8 op,
    ui32 cmd,
    int fd,
    ui32 nsId,
    ui32 lbaShift,
    const void* addr,
    ui32 dataLen,
    ui64 totalLen,
    ui64 offset,
    TFileIOCompletion* completion,
    ui32 flags)
{
    io_uring_sqe* sqe = io_uring_get_sqe(&Ring);
    if (!sqe) {
        completion->Func(completion, MakeError(E_REJECTED, "Overloaded"), 0);
        return;
    }

    sqe->fd = fd;
    sqe->cmd_op = cmd;
    sqe->opcode = IORING_OP_URING_CMD;

    const ui64 startingLba = offset >> lbaShift;
    const ui32 lbaCount = static_cast<ui32>(totalLen >> lbaShift) - 1;

    new (sqe->cmd) TNvmeUringCmd{
        .opcode = op,
        .nsid = nsId,
        .addr = std::bit_cast<ui64>(addr),
        .data_len = dataLen,
        .cdw10 = static_cast<ui32>(startingLba & 0xffffffff),
        .cdw11 = static_cast<ui32>(startingLba >> 32),
        .cdw12 = lbaCount,
    };

    io_uring_sqe_set_data(sqe, completion);
    io_uring_sqe_set_flags(sqe, flags);

    NSan::Release(completion);

    const auto error = Submit(&Ring);
    if (HasError(error)) {
        completion->Func(completion, error, 0);
    }
}

////////////////////////////////////////////////////////////////////////////////

TResultOrError<bool> IsNvmePassthroughSupported()
{
    io_uring ring = {};
    const auto error = InitRing(&ring, 1, nullptr, true);
    if (HasError(error)) {
        return false;
    }
    Y_DEFER { io_uring_queue_exit(&ring); };

    auto* probe = io_uring_get_probe_ring(&ring);
    if (!probe) {
        return MakeSystemError(ENOTSUP, "io_uring_get_probe_ring");
    }

    Y_DEFER { io_uring_free_probe(probe); };

    return io_uring_opcode_supported(probe, IORING_OP_URING_CMD);
}

}   // namespace NCloud::NIoUring
