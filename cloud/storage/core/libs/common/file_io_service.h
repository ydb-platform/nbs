#pragma once

#include "public.h"

#include "error.h"
#include "startable.h"

#include <library/cpp/threading/future/future.h>

#include <util/generic/noncopyable.h>

#include <functional>
#include <type_traits>

class TFileHandle;

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct TFileIOCompletion
    : TNonCopyable
{
    using TFunc = void (*)(
        TFileIOCompletion* obj,
        const NProto::TError& error,
        ui32 bytesTransferred);

    TFunc Func = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

struct IFileIOService
    : IStartable
{
    virtual void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        TFileIOCompletion* completion) = 0;

    virtual void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) = 0;

    virtual void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) = 0;

    virtual void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion) = 0;

    // conveniences: callbacks

    template <typename F>
        requires std::is_invocable_v<F, NProto::TError, ui32>
    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        F&& callback)
    {
        auto cb = std::make_unique<TCallbackCompletion<std::decay_t<F>>>(
            std::forward<F>(callback));

        AsyncWrite(file, offset, buffer, cb.get());

        Y_UNUSED(cb.release());  // ownership transferred
    }

    template <typename F>
        requires std::is_invocable_v<F, NProto::TError, ui32>
    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        F&& callback)
    {
        auto cb = std::make_unique<TCallbackCompletion<std::decay_t<F>>>(
            std::forward<F>(callback));

        AsyncWriteV(file, offset, buffers, cb.get());

        Y_UNUSED(cb.release());  // ownership transferred
    }

    template <typename F>
        requires std::is_invocable_v<F, NProto::TError, ui32>
    void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        F&& callback)
    {
        auto cb = std::make_unique<TCallbackCompletion<std::decay_t<F>>>(
            std::forward<F>(callback));

        AsyncRead(file, offset, buffer, cb.get());

        Y_UNUSED(cb.release());  // ownership transferred
    }

    template <typename F>
        requires std::is_invocable_v<F, NProto::TError, ui32>
    void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        F&& callback)
    {
        auto cb = std::make_unique<TCallbackCompletion<std::decay_t<F>>>(
            std::forward<F>(callback));

        AsyncReadV(file, offset, buffers, cb.get());

        Y_UNUSED(cb.release());  // ownership transferred
    }

    // conveniences: futures

    NThreading::TFuture<ui32> AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer);

    NThreading::TFuture<ui32> AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers);

    NThreading::TFuture<ui32> AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer);

    NThreading::TFuture<ui32> AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers);

private:
    template <typename F>
    struct TCallbackCompletion
        : TFileIOCompletion
    {
        F Func;

        template <typename T>
        explicit TCallbackCompletion(T&& func)
            : TFileIOCompletion {.Func = &TCallbackCompletion::Complete}
            , Func {std::forward<T>(func)}
        {}

        static void Complete(
            TFileIOCompletion* self,
            const NProto::TError& error,
            ui32 bytes)
        {
            std::unique_ptr<TCallbackCompletion> ptr {
                static_cast<TCallbackCompletion*>(self)
            };

            std::invoke(std::move(ptr->Func), error, bytes);
        }
    };
};

////////////////////////////////////////////////////////////////////////////////

IFileIOServicePtr CreateFileIOServiceStub();

IFileIOServicePtr CreateRoundRobinFileIOService(
    TVector<IFileIOServicePtr> fileIOs);

}   // namespace NCloud
