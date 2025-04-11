#include "service.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/thread.h>

#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/file.h>
#include <util/system/thread.h>

#include <atomic>

#include <libaio.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MAX_EVENTS_BATCH = 32;
constexpr timespec WAIT_TIMEOUT = {1, 0}; // 1 sec

////////////////////////////////////////////////////////////////////////////////

auto MakeSystemError(int error, TStringBuf message)
{
    return MakeError(MAKE_SYSTEM_ERROR(error), TStringBuilder()
        << "(" << LastSystemErrorText(error) << ") "
        << message);
}

auto MakeIOError(i64 ret)
{
    return ret < 0
        ? MakeSystemError(-ret, "async IO operation failed")
        : NProto::TError {};
}

////////////////////////////////////////////////////////////////////////////////

class TAsyncIOContext
{
private:
    io_context* Context = nullptr;

public:
    explicit TAsyncIOContext(int nr)
    {
        int code = 0;
        int iterations = 0;
        const int maxIterations = 1000;
        const auto waitTime = TDuration::MilliSeconds(100);
        while (iterations < maxIterations) {
            ++iterations;
            code = io_setup(nr, &Context);
            if (code == -EAGAIN) {
                const auto aioNr =
                    TIFStream("/proc/sys/fs/aio-nr").ReadLine();
                const auto aioMaxNr =
                    TIFStream("/proc/sys/fs/aio-max-nr").ReadLine();
                Cerr << "retrying EAGAIN from io_setup, aio-nr/max: "
                    << aioNr << "/" << aioMaxNr << Endl;
                Sleep(waitTime);
            } else {
                break;
            }
        }

        Y_ABORT_UNLESS(code == 0,
            "unable to initialize context: %s, iterations: %d",
            LastSystemErrorText(-code),
            iterations);
    }

    ~TAsyncIOContext()
    {
        if (Context) {
            int ret = io_destroy(Context);
            Y_ABORT_UNLESS(ret == 0,
                "unable to destroy context: %s",
                LastSystemErrorText(-ret));
        }
    }

    NProto::TError Submit(iocb* io)
    {
        const int ret = io_submit(Context, 1, &io);
        if (ret < 0) {
            if (ret == -EAGAIN) {
                // retry EAGAIN
                return MakeError(E_REJECTED, "EAGAIN received");
            }

            return MakeSystemError(-ret, "unable to submit async IO operation");
        }

        return NProto::TError{};
    }

    TArrayRef<io_event> GetEvents(TArrayRef<io_event> events, timespec& timeout)
    {
        int ret = io_getevents(Context, 1, events.size(), events.data(), &timeout);

        if (ret == -EINTR) {
            return {};
        }

        Y_ABORT_UNLESS(ret >= 0,
            "unable to get async IO completion: %s",
            LastSystemErrorText(-ret));

        return events.subspan(0, ret);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAIOService final
    : public IFileIOService
{
private:
    TAsyncIOContext IOContext;

    TThread PollerThread;
    std::atomic_flag ShouldStop = {};

public:
    explicit TAIOService(size_t maxEvents)
        : IOContext(maxEvents)
        , PollerThread(ThreadProc, this)
    {}

    // IStartable

    void Start() override
    {
        PollerThread.Start();
    }

    void Stop() override
    {
        if (!ShouldStop.test_and_set()) {
            PollerThread.Join();
        }
    }

    // IFileIOService

    void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        TFileIOCompletion* completion) override
    {
        auto req = std::make_unique<iocb>();

        io_prep_pread(
            req.get(),
            file,
            buffer.data(),
            buffer.size(),
            offset);

        req->data = completion;

        Submit(std::move(req));
    }

    void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) override
    {
        auto req = std::make_unique<iocb>();

        TVector<iovec> iov(buffers.size());
        for (ui32 i = 0; i < buffers.size(); ++i) {
            iov[i].iov_base = static_cast<void*>(buffers[i].data());
            iov[i].iov_len = buffers[i].size();
        }

        io_prep_preadv(
            req.get(),
            file,
            iov.data(),
            iov.size(),
            offset);

        req->data = completion;

        Submit(std::move(req));
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) override
    {
        auto req = std::make_unique<iocb>();

        io_prep_pwrite(
            req.get(),
            file,
            const_cast<char*>(buffer.data()),
            buffer.size(),
            offset);

        req->data = completion;

        Submit(std::move(req));
    }

    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion) override
    {
        auto req = std::make_unique<iocb>();

        TVector<iovec> iov(buffers.size());
        for (ui32 i = 0; i < buffers.size(); ++i) {
            iov[i].iov_base =
                static_cast<void*>(const_cast<char*>(buffers[i].data()));
            iov[i].iov_len = buffers[i].size();
        }

        io_prep_pwritev(
            req.get(),
            file,
            iov.data(),
            iov.size(),
            offset);

        req->data = completion;

        Submit(std::move(req));
    }

private:
    void Complete(
        TFileIOCompletion* completion,
        const NProto::TError& error,
        ui32 bytes)
    {
        std::invoke(completion->Func, completion, error, bytes);
    }

    void Submit(std::unique_ptr<iocb> request)
    {
        iocb* ptr = request.get();

#if defined(_tsan_enabled_)
        // tsan is not aware of barrier between io_submit/io_getevents
        AtomicSet(*(TAtomicBase*)ptr, *(TAtomicBase*)ptr);
#endif
        auto error = IOContext.Submit(ptr);

        if (HasError(error)) {
            Complete(static_cast<TFileIOCompletion*>(ptr->data), error, 0);
        } else {
            Y_UNUSED(request.release());  // ownership transferred
        }
    }

    static void* ThreadProc(void* arg)
    {
        static_cast<TAIOService*>(arg)->Run();
        return nullptr;
    }

    void Run()
    {
        SetHighestThreadPriority();

        static std::atomic<ui64> index = 0;
        NCloud::SetCurrentThreadName("AIO" + ToString(index++));

        timespec timeout = WAIT_TIMEOUT;

        io_event events[MAX_EVENTS_BATCH] {};

        while (!ShouldStop.test()) {
            for (auto& ev: IOContext.GetEvents(events, timeout)) {
                std::unique_ptr<iocb> ptr {ev.obj};

#if defined(_tsan_enabled_)
                // tsan is not aware of barrier between io_submit/io_getevents
                AtomicGet(*(TAtomicBase*)ptr.get());
#endif
                const i64 ret = static_cast<i64>(ev.res); // it is really signed

                Complete(
                    static_cast<TFileIOCompletion*>(ev.data),
                    MakeIOError(ret),
                    ev.res);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TThreadedAIOService final
    : public IFileIOService
{
private:
    TVector<IFileIOServicePtr> IoServices;
    std::atomic<i64> NextService = 0;

public:
    TThreadedAIOService(ui32 threadCount, size_t maxEvents)
    {
        Y_ABORT_UNLESS(threadCount > 0);

        for (ui32 i = 0; i < threadCount; i++) {
            IoServices.push_back(CreateAIOService(maxEvents));
        }
    }

    void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        TFileIOCompletion* completion) override
    {
        auto index = NextService++;
        IoServices[index % IoServices.size()]
            ->AsyncRead(file, offset, buffer, completion);
    }

    void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) override
    {
        auto index = NextService++;
        IoServices[index % IoServices.size()]
            ->AsyncReadV(file, offset, buffers, completion);
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) override
    {
        auto index = NextService++;
        IoServices[index % IoServices.size()]
            ->AsyncWrite(file, offset, buffer, completion);
    }

    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion) override
    {
        auto index = NextService++;
        IoServices[index % IoServices.size()]
            ->AsyncWriteV(file, offset, buffers, completion);
    }

    void Start() override
    {
        for (auto& ioService: IoServices) {
            ioService->Start();
        }
    }

    void Stop() override
    {
        for (auto& ioService: IoServices) {
            ioService->Stop();
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileIOServicePtr CreateAIOService(size_t maxEvents)
{
    return std::make_shared<TAIOService>(maxEvents);
}

IFileIOServicePtr CreateThreadedAIOService(ui32 threadCount, size_t maxEvents)
{
    return std::make_shared<TThreadedAIOService>(threadCount, maxEvents);
}

}   // namespace NCloud
