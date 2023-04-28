#include "aio.h"

#include <util/generic/noncopyable.h>
#include <util/generic/yexception.h>
#include <util/stream/str.h>
#include <util/system/error.h>
#include <util/system/thread.h>

#include <errno.h>
#include <libaio.h>

namespace NAsyncIO {
    namespace {
        ////////////////////////////////////////////////////////////////////////////////

        class TAsyncIOContext: private TNonCopyable {
        private:
            io_context_t Context;

        public:
            TAsyncIOContext(int maxEvents)
                : Context(nullptr)
            {
                int ret = io_setup(maxEvents, &Context);
                if (Y_UNLIKELY(ret < 0)) {
                    ythrow TSystemError(-ret) << "unable to initialize context";
                }
            }

            ~TAsyncIOContext() {
                if (Context) {
                    int ret = io_destroy(Context);
                    Y_VERIFY(ret == 0, "unable to destroy context: %s", LastSystemErrorText(-ret));
                }
            }

            int Submit(long nr, iocb* ios[]) {
                int ret = io_submit(Context, nr, ios);
                if (Y_UNLIKELY(ret < 0)) {
                    ythrow TSystemError(-ret) << "unable to submit operation";
                }
                return ret;
            }

            int Cancel(iocb* iocb, io_event* evt) {
                int ret = io_cancel(Context, iocb, evt);
                if (Y_UNLIKELY(ret < 0)) {
                    ythrow TSystemError(-ret) << "unable to cancel operation";
                }
                return ret;
            }

            int GetEvents(long min_nr, long nr, io_event* events, timespec* timeout) {
                int ret = io_getevents(Context, min_nr, nr, events, timeout);
                if (Y_UNLIKELY(ret < 0 && ret != -EINTR)) {
                    ythrow TSystemError(-ret) << "unable to get events";
                }
                return ret;
            }
        };

        ////////////////////////////////////////////////////////////////////////////////

        struct TAsyncIORequest: public iocb {
            NThreading::TPromise<ui32> Result;
        };

    }

    ////////////////////////////////////////////////////////////////////////////////

    class TAsyncIOService::TImpl {
    private:
        static const int WAIT_TIMEOUT = 1; // sec
        static const int MAX_EVENTS_BATCH = 32;

        TAsyncIOContext Context;

        TThread Thread;
        TAtomic ShouldStop = 0;

    public:
        TImpl(ui32 maxEvents)
            : Context(maxEvents)
            , Thread(ThreadProc, this)
        {
        }

        void Start() {
            Thread.Start();
        }

        void Stop() {
            AtomicSet(ShouldStop, 1);
            Thread.Join();
        }

        void Submit(TAutoPtr<TAsyncIORequest> request) {
            iocb* ptr = request.Get();

#if defined(_tsan_enabled_)
            // tsan does not aware of barrier between io_submit/io_getevents
            AtomicSet(*(TAtomicBase*)ptr, *(TAtomicBase*)ptr);
#endif

            iocb* ios[] = {ptr};
            int count = Context.Submit(1, ios);
            Y_VERIFY(count == 1);

            Y_UNUSED(request.Release());
        }

    private:
        static void* ThreadProc(void* arg) {
            static_cast<TImpl*>(arg)->Run();
            return nullptr;
        }

        void Run() {
            SetHighestThreadPriority();

            timespec timeout = {WAIT_TIMEOUT, 0};

            io_event events[MAX_EVENTS_BATCH];
            Zero(events);

            while (!AtomicGet(ShouldStop)) {
                int count = Context.GetEvents(1, MAX_EVENTS_BATCH, events, &timeout);
                for (int i = 0; i < count; ++i) {
                    iocb* ptr = events[i].obj;

#if defined(_tsan_enabled_)
                    // tsan does not aware of barrier between io_submit/io_getevents
                    AtomicGet(*(TAtomicBase*)ptr);
#endif

                    TAutoPtr<TAsyncIORequest> request(static_cast<TAsyncIORequest*>(ptr));

                    long ret = (long)events[i].res; // it is really signed
                    if (Y_UNLIKELY(ret < 0)) {
                        TStringStream out;
                        out << "(" << LastSystemErrorText(-ret) << ") "
                            << "async IO operation failed";
                        request->Result.SetException(out.Str());
                        continue;
                    }

                    request->Result.SetValue(ret);
                }
            }
        }
    };

    ////////////////////////////////////////////////////////////////////////////////

    TAsyncIOService::TAsyncIOService(ui32 numThreads, ui32 maxEvents)
        : Impl(new TImpl(maxEvents))
    {
        Y_UNUSED(numThreads);
    }

    TAsyncIOService::~TAsyncIOService() = default;

    void TAsyncIOService::Start() {
        Impl->Start();
    }

    void TAsyncIOService::Stop() {
        Impl->Stop();
    }

    NThreading::TFuture<ui32> TAsyncIOService::Read(
        TFileHandle& file,
        void* buffer,
        ui32 count,
        ui64 offset) {
        auto result = NThreading::NewPromise<ui32>();

        TAutoPtr<TAsyncIORequest> request = new TAsyncIORequest();
        request->Result = result;

        io_prep_pread(
            request.Get(),
            static_cast<FHANDLE>(file),
            buffer,
            count,
            offset);

        Impl->Submit(request);
        return result;
    }

    NThreading::TFuture<ui32> TAsyncIOService::Write(
        TFileHandle& file,
        const void* buffer,
        ui32 count,
        ui64 offset) {
        auto result = NThreading::NewPromise<ui32>();

        TAutoPtr<TAsyncIORequest> request = new TAsyncIORequest();
        request->Result = result;

        io_prep_pwrite(
            request.Get(),
            static_cast<FHANDLE>(file),
            const_cast<void*>(buffer),
            count,
            offset);

        Impl->Submit(request);
        return result;
    }

}
