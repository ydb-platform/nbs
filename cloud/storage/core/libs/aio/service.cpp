#include "service.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/common/write_sync_flags.h>

#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/file.h>
#include <util/system/thread.h>

#include <libaio.h>
#include <sys/eventfd.h>

#include <atomic>

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

    // returns the number of submitted iocbs or a negative errno
    int SubmitBatch(iocb** ios, int count)
    {
        return io_submit(Context, count, ios);
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
    : public IExternalCompletionFileIOService
{
private:
    TAsyncIOContext IOContext;

    TThread PollerThread;
    std::atomic_flag ShouldStop = false;

    // PROTOTYPE: external completion mode, see IExternalCompletionFileIOService
    const bool External = false;
    TFileHandle EventFd;
    std::atomic<size_t> InFlightCount = 0;

public:
    explicit TAIOService(TAioServiceParams params, bool external = false)
        : IOContext(params.MaxEvents)
        , PollerThread(
              std::bind_front(
                  &TAIOService::Run,
                  this,
                  std::move(params.CompletionThreadName)))
        , External(external)
    {
        if (External) {
            EventFd = TFileHandle(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
            Y_ABORT_UNLESS(EventFd.IsOpen());
        }
    }

    // IExternalCompletionFileIOService

    int GetCompletionFd() override
    {
        return EventFd;
    }

    size_t InFlight() override
    {
        return InFlightCount.load(std::memory_order_acquire);
    }

    // wakes whoever waits on the completion fd, so a spinning reaper starts
    // polling right after a submit
    void Kick()
    {
        if (External) {
            ui64 one = 1;
            Y_UNUSED(::write(EventFd, &one, sizeof(one)));
        }
    }

    size_t ProcessCompletions() override
    {
        size_t reaped = 0;
        io_event events[MAX_EVENTS_BATCH] {};
        for (;;) {
            timespec zero {0, 0};
            auto batch = IOContext.GetEvents(events, zero);
            if (batch.empty()) {
                break;
            }
            reaped += batch.size();
            for (auto& ev: batch) {
                std::unique_ptr<iocb> ptr {ev.obj};
                const i64 ret = static_cast<i64>(ev.res);
                Complete(
                    static_cast<TFileIOCompletion*>(ev.data),
                    MakeIOError(ret),
                    ev.res);
            }
        }
        return reaped;
    }

    // IStartable

    void Start() override
    {
        if (!External) {
            PollerThread.Start();
        }
    }

    void Stop() override
    {
        if (ShouldStop.test()) {
            return;
        }

        if (External) {
            ShouldStop.test_and_set();
            ui64 one = 1;
            Y_UNUSED(::write(EventFd, &one, sizeof(one)));
            return;
        }

        // unblock the poller thread that may be blocked on io_getevents by
        // initiating a read operation

        TFileHandle file {eventfd(1, EFD_NONBLOCK)};

        ui64 value = 0;
        IFileIOService::AsyncRead(
            file,
            0,
            {std::bit_cast<char*>(&value), sizeof(value)},
            [&](const auto&, ui32) { ShouldStop.test_and_set(); });

        PollerThread.Join();
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
        if (External) {
            io_set_eventfd(req.get(), EventFd);
        }

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
        if (External) {
            io_set_eventfd(req.get(), EventFd);
        }

        Submit(std::move(req));
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion,
        ui32 flags) override
    {
        auto req = std::make_unique<iocb>();
        const ui32 syncFlags = GetWriteSyncFlags(flags);
        // the iovec lives until the iocb is handed to the kernel, which may
        // happen later in a batch
        auto iov = std::make_unique<iovec[]>(1);
        iov[0].iov_base = const_cast<char*>(buffer.data());
        iov[0].iov_len = buffer.size();
        io_prep_pwritev2(
            req.get(),
            file,
            iov.get(),
            1,
            offset,
            syncFlags);

        req->data = completion;
        if (External) {
            io_set_eventfd(req.get(), EventFd);
        }

        Submit(std::move(req), std::move(iov));
    }

    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion,
        ui32 flags) override
    {
        auto req = std::make_unique<iocb>();
        const ui32 syncFlags = GetWriteSyncFlags(flags);

        auto iov = std::make_unique<iovec[]>(buffers.size());
        for (ui32 i = 0; i < buffers.size(); ++i) {
            iov[i].iov_base = const_cast<char*>(buffers[i].data());
            iov[i].iov_len = buffers[i].size();
        }
        io_prep_pwritev2(
            req.get(),
            file,
            iov.get(),
            buffers.size(),
            offset,
            syncFlags);

        req->data = completion;
        if (External) {
            io_set_eventfd(req.get(), EventFd);
        }

        Submit(std::move(req), std::move(iov));
    }

private:
    void Complete(
        TFileIOCompletion* completion,
        const NProto::TError& error,
        ui32 bytes)
    {
        InFlightCount.fetch_sub(1, std::memory_order_acq_rel);
        std::invoke(completion->Func, completion, error, bytes);
    }

    // PROTOTYPE: iocbs deferred within a FileIOBeginBatch/EndBatch scope
    struct TPending
    {
        iocb* Request;
        std::unique_ptr<iovec[]> Iov;
    };
    static inline thread_local TVector<TPending> PendingBatch;

    void FlushBatch()
    {
        TVector<TPending> batch = std::move(PendingBatch);
        PendingBatch.clear();
        if (batch.empty()) {
            return;
        }

        TVector<iocb*> ios;
        ios.reserve(batch.size());
        for (const auto& pending: batch) {
            ios.push_back(pending.Request);
        }

        InFlightCount.fetch_add(ios.size(), std::memory_order_acq_rel);
        int submitted = IOContext.SubmitBatch(ios.data(), ios.size());
        if (submitted < 0) {
            submitted = 0;
        }
        if (submitted) {
            Kick();
        }

        // the iocbs the kernel did not take fail here; the iovecs of the
        // taken ones were copied by the kernel and die with |batch|
        for (size_t i = submitted; i < ios.size(); ++i) {
            std::unique_ptr<iocb> failed(ios[i]);
            Complete(
                static_cast<TFileIOCompletion*>(failed->data),
                MakeError(E_REJECTED, "batched io_submit failed"),
                0);
        }
    }

    void Submit(
        std::unique_ptr<iocb> request,
        std::unique_ptr<iovec[]> iov = {})
    {
        iocb* ptr = request.get();

#if defined(_tsan_enabled_)
        // tsan is not aware of barrier between io_submit/io_getevents
        AtomicSet(*(TAtomicBase*)ptr, *(TAtomicBase*)ptr);
#endif
        if (FileIOInBatch()) {
            if (PendingBatch.empty()) {
                FileIOSetBatchFlusher([this] { FlushBatch(); });
            }
            PendingBatch.push_back({request.release(), std::move(iov)});
            return;
        }

        InFlightCount.fetch_add(1, std::memory_order_acq_rel);
        auto error = IOContext.Submit(ptr);

        if (HasError(error)) {
            Complete(static_cast<TFileIOCompletion*>(ptr->data), error, 0);
        } else {
            Y_UNUSED(request.release());  // ownership transferred
            Kick();
        }
    }

    void Run(const TString& threadName)
    {
        SetHighestThreadPriority();
        NCloud::SetCurrentThreadName(threadName);

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

class TAIOServiceFactory final
    : public IFileIOServiceFactory
{
private:
    const TAioServiceParams Params;
    ui32 Index = 0;

public:
    explicit TAIOServiceFactory(TAioServiceParams params)
        : Params(std::move(params))
    {}

    IFileIOServicePtr CreateFileIOService() final
    {
        const ui32 index = Index++;
        TAioServiceParams params = Params;
        params.CompletionThreadName = TStringBuilder()
                                      << params.CompletionThreadName << index;

        return std::make_shared<TAIOService>(std::move(params));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IExternalCompletionFileIOService>
CreateAIOServiceExternalCompletion(TAioServiceParams params)
{
    return std::make_shared<TAIOService>(std::move(params), true);
}

IFileIOServicePtr CreateAIOService(TAioServiceParams params)
{
    TAIOServiceFactory factory(std::move(params));
    return factory.CreateFileIOService();
}

IFileIOServicePtr CreateThreadedAIOService(
    ui32 threadCount,
    TAioServiceParams params)
{
    TAIOServiceFactory factory(std::move(params));

    Y_ABORT_UNLESS(threadCount > 0);

    return CreateRoundRobinFileIOService(threadCount, factory);
}

IFileIOServiceFactoryPtr CreateAIOServiceFactory(TAioServiceParams params)
{
    return std::make_shared<TAIOServiceFactory>(std::move(params));
}

}   // namespace NCloud
