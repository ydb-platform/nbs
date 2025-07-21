#pragma once

#include <cloud/storage/core/libs/common/file_io_service.h>

#include <util/generic/string.h>
#include <util/system/file.h>
#include <util/system/thread.h>

#include <liburing.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TIoUring final
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
        bool ShareKernelWorkers = false;
    };

    TIoUring();
    // Share kernel worker threads with `wqOwner`
    explicit TIoUring(TParams params,  TIoUring* wqOwner = nullptr);

    TIoUring(TIoUring&&) = default;
    TIoUring(const TIoUring&&) = delete;

    TIoUring& operator = (TIoUring&&) = delete;
    TIoUring& operator = (const TIoUring&&) = delete;

    ~TIoUring();

    void Start();
    void Stop();

    void AsyncWrite(
        int fd,
        TArrayRef<const char> buffer,
        ui64 offset,
        TFileIOCompletion* completion);

    void AsyncRead(
        int fd,
        TArrayRef<char> buffer,
        ui64 offset,
        TFileIOCompletion* completion);

    void AsyncWriteV(
        int fd,
        TArrayRef<const TArrayRef<const char>> buffer,
        ui64 offset,
        TFileIOCompletion* completion);

    void AsyncReadV(
        int fd,
        TArrayRef<const TArrayRef<char>> buffer,
        ui64 offset,
        TFileIOCompletion* completion);

    void AsyncNOP(TFileIOCompletion* completion);

private:
    void SubmitIO(
        int op,
        int fd,
        const void* addr,
        ui32 len,
        ui64 offset,
        TFileIOCompletion* completion);

    void AsyncIO(
        int op,
        int fd,
        const void* addr,
        ui32 len,
        ui64 offset,
        TFileIOCompletion* completion);

    void SubmitNOP(TFileIOCompletion* completion);
    void SubmitStopSignal();
    void ProcessCompletion(io_uring_cqe* cqe);
    void CompletionThreadProc(const TString& threadName);
};

}   // namespace NCloud
