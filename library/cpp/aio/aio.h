#pragma once

#include <library/cpp/threading/future/future.h>

#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>
#include <util/system/file.h>

namespace NAsyncIO {
    ////////////////////////////////////////////////////////////////////////////////

    class TAsyncIOService: private TNonCopyable {
    private:
        class TImpl;
        THolder<TImpl> Impl;

    public:
        TAsyncIOService(ui32 numThreads = 4, ui32 maxEvents = 1024);
        ~TAsyncIOService();

        void Start();
        void Stop();

        NThreading::TFuture<ui32> Read(
            TFileHandle& file,
            void* buffer,
            ui32 count,
            ui64 offset);

        NThreading::TFuture<ui32> Write(
            TFileHandle& file,
            const void* buffer,
            ui32 count,
            ui64 offset);
    };

}
