#pragma once

#include <cloud/storage/core/libs/common/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/system/thread.h>

#include <liburing.h>

namespace NCloud::NIoUring {

////////////////////////////////////////////////////////////////////////////////

class TContext final
{
private:
    io_uring Ring = {};

    ITaskQueuePtr SubmissionThread;
    TThread CompletionThread;

    NThreading::TFuture<void> Started;

public:
    static constexpr ui32 DefaultSubmissionQueueEntries = 1024;

    struct TParams
    {
        TString SubmissionThreadName = "IO.SQ";
        TString CompletionThreadName = "IO.CQ";

        ui32 SubmissionQueueEntries = DefaultSubmissionQueueEntries;

        ui32 MaxKernelWorkersCount = 0;

        // Share kernel worker threads with `WqOwner`
        TContext* WqOwner = nullptr;
    };

    explicit TContext(TParams params);

    TContext(TContext&&) = delete;
    TContext(const TContext&&) = delete;

    TContext& operator = (TContext&&) = delete;
    TContext& operator = (const TContext&&) = delete;

    ~TContext();

    void Start();
    void Stop();

    void AsyncWrite(
        int fd,
        TArrayRef<const char> buffer,
        ui64 offset,
        TFileIOCompletion* completion,
        ui32 flags = 0);

    void AsyncRead(
        int fd,
        TArrayRef<char> buffer,
        ui64 offset,
        TFileIOCompletion* completion,
        ui32 flags = 0);

    void AsyncWriteV(
        int fd,
        TArrayRef<const TArrayRef<const char>> buffer,
        ui64 offset,
        TFileIOCompletion* completion,
        ui32 flags = 0);

    void AsyncReadV(
        int fd,
        TArrayRef<const TArrayRef<char>> buffer,
        ui64 offset,
        TFileIOCompletion* completion,
        ui32 flags = 0);

    void AsyncNOP(TFileIOCompletion* completion, ui32 flags = 0);

    void PostCompletion(TFileIOCompletion* completion, int res);

private:
    void SubmitIO(
        int op,
        int fd,
        const void* addr,
        ui32 len,
        ui64 offset,
        TFileIOCompletion* completion,
        ui32 flags);

    void AsyncIO(
        int op,
        int fd,
        const void* addr,
        ui32 len,
        ui64 offset,
        TFileIOCompletion* completion,
        ui32 flags);

    void SubmitNOP(TFileIOCompletion* completion, ui32 flags);
    void SubmitMsg(TFileIOCompletion* completion, int res);
    void SubmitStopSignal();
    void ProcessCompletion(io_uring_cqe* cqe);
    void CompletionThreadProc(const TString& threadName);
};

}   // namespace NCloud::NIoUring
