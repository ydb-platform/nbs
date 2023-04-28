#include "aio.h"

#include <library/cpp/threading/future/async.h>

#include <util/thread/pool.h>

namespace NAsyncIO {
    namespace {
        ////////////////////////////////////////////////////////////////////////////////

        class TNotOwningFileHandle: public TFileHandle {
        public:
            TNotOwningFileHandle(FHANDLE handle)
                : TFileHandle(handle)
            {
            }

            ~TNotOwningFileHandle() {
                Release();
            }
        };

    }

    ////////////////////////////////////////////////////////////////////////////////

    class TAsyncIOService::TImpl: private TNonCopyable {
    private:
        const ui32 NumThreads;
        const ui32 MaxEvents;
        TAutoPtr<IThreadPool> Queue;

    public:
        TImpl(ui32 numThreads, ui32 maxEvents)
            : NumThreads(numThreads)
            , MaxEvents(maxEvents)
        {
        }

        void Start() {
            Queue = CreateThreadPool(NumThreads, MaxEvents);
        }

        void Stop() {
            Queue->Stop();
        }

        NThreading::TFuture<ui32> Read(
            TFileHandle& file,
            void* buffer,
            ui32 count,
            ui64 offset) {
            FHANDLE handle = static_cast<FHANDLE>(file);
            return NThreading::Async([=] {
                TNotOwningFileHandle file(handle);
                return (ui32)file.Pread(buffer, count, offset);
            },
                                     *Queue);
        }

        NThreading::TFuture<ui32> Write(
            TFileHandle& file,
            const void* buffer,
            ui32 count,
            ui64 offset) {
            FHANDLE handle = static_cast<FHANDLE>(file);
            return NThreading::Async([=] {
                TNotOwningFileHandle file(handle);
                return (ui32)file.Pwrite(buffer, count, offset);
            },
                                     *Queue);
        }
    };

    ////////////////////////////////////////////////////////////////////////////////

    TAsyncIOService::TAsyncIOService(ui32 numThreads, ui32 maxEvents)
        : Impl(new TImpl(numThreads, maxEvents))
    {
    }

    TAsyncIOService::~TAsyncIOService() {
    }

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
        return Impl->Read(file, buffer, count, offset);
    }

    NThreading::TFuture<ui32> TAsyncIOService::Write(
        TFileHandle& file,
        const void* buffer,
        ui32 count,
        ui64 offset) {
        return Impl->Write(file, buffer, count, offset);
    }

}
