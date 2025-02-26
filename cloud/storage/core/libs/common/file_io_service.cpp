#include "file_io_service.h"

#include <exception>

namespace NCloud {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFileIOServiceStub final
    : public IFileIOService
{
    // IStartable

    void Start() override
    {}

    void Stop() override
    {}

    // IFileIOService

    void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        TFileIOCompletion* completion) override
    {
        Y_UNUSED(file, offset);

        std::invoke(
            completion->Func,
            completion,
            NProto::TError {},
            buffer.size());
    }

    void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) override
    {
        Y_UNUSED(file, offset);

        ui32 size = 0;
        for (auto& buffer: buffers) {
            size += buffer.size();
        }

        std::invoke(
            completion->Func,
            completion,
            NProto::TError {},
            size);
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) override
    {
        Y_UNUSED(file, offset);

        std::invoke(
            completion->Func,
            completion,
            NProto::TError {},
            buffer.size());
    }

    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion) override
    {
        Y_UNUSED(file, offset);

        ui32 size = 0;
        for (auto& buffer: buffers) {
            size += buffer.size();
        }

        std::invoke(
            completion->Func,
            completion,
            NProto::TError {},
            size);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TFuture<ui32> IFileIOService::AsyncWrite(
    TFileHandle& file,
    i64 offset,
    TArrayRef<const char> buffer)
{
    auto p = NewPromise<ui32>();

    AsyncWrite(
        file,
        offset,
        buffer,
        [=] (const auto& error, ui32 bytes) mutable {
            if (HasError(error)) {
                auto ex = std::make_exception_ptr(TServiceError{error});
                p.SetException(std::move(ex));
            } else {
                p.SetValue(bytes);
            }
        }
    );

    return p.GetFuture();
}

TFuture<ui32> IFileIOService::AsyncWriteV(
    TFileHandle& file,
    i64 offset,
    const TVector<TArrayRef<const char>>& buffers)
{
    auto p = NewPromise<ui32>();

    AsyncWriteV(
        file,
        offset,
        buffers,
        [=] (const auto& error, ui32 bytes) mutable {
            if (HasError(error)) {
                auto ex = std::make_exception_ptr(TServiceError{error});
                p.SetException(std::move(ex));
            } else {
                p.SetValue(bytes);
            }
        }
    );

    return p.GetFuture();
}

TFuture<ui32> IFileIOService::AsyncRead(
    TFileHandle& file,
    i64 offset,
    TArrayRef<char> buffer)
{
    auto p = NewPromise<ui32>();

    AsyncRead(
        file,
        offset,
        buffer,
        [=] (const auto& error, ui32 bytes) mutable {
            if (HasError(error)) {
                auto ex = std::make_exception_ptr(TServiceError{error});
                p.SetException(std::move(ex));
            } else {
                p.SetValue(bytes);
            }
        }
    );

    return p.GetFuture();
}

TFuture<ui32> IFileIOService::AsyncReadV(
    TFileHandle& file,
    i64 offset,
    const TVector<TArrayRef<char>>& buffers)
{
    auto p = NewPromise<ui32>();

    AsyncReadV(
        file,
        offset,
        buffers,
        [=] (const auto& error, ui32 bytes) mutable {
            if (HasError(error)) {
                auto ex = std::make_exception_ptr(TServiceError{error});
                p.SetException(std::move(ex));
            } else {
                p.SetValue(bytes);
            }
        }
    );

    return p.GetFuture();
}

////////////////////////////////////////////////////////////////////////////////

IFileIOServicePtr CreateFileIOServiceStub()
{
    return std::make_shared<TFileIOServiceStub>();
}

}   // namespace NCloud
