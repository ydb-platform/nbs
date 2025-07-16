#include "file_io_service.h"

#include "task_queue.h"
#include "thread_pool.h"

#include <util/generic/scope.h>
#include <util/system/file.h>

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

////////////////////////////////////////////////////////////////////////////////

class TRoundRobinFileIOService final
    : public IFileIOService
{
private:
    TVector<IFileIOServicePtr> FileIOs;
    std::atomic<ui32> NextIndex = 0;

public:
    explicit TRoundRobinFileIOService(TVector<IFileIOServicePtr> fileIOs)
        : FileIOs(std::move(fileIOs))
    {}

    void Start() final
    {
        for (auto& fileIO: FileIOs) {
            fileIO->Start();
        }
    }

    void Stop() final
    {
        for (auto& fileIO: FileIOs) {
            fileIO->Stop();
        }
    }

    void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        TFileIOCompletion* completion) final
    {
        const size_t index = PickIndex();
        FileIOs[index]->AsyncRead(file, offset, buffer, completion);
    }

    void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) final
    {
        const size_t index = PickIndex();
        FileIOs[index]->AsyncReadV(file, offset, buffers, completion);
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) final
    {
        const size_t index = PickIndex();
        FileIOs[index]->AsyncWrite(file, offset, buffer, completion);
    }

    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion) final
    {
        const size_t index = PickIndex();
        FileIOs[index]->AsyncWriteV(file, offset, buffers, completion);
    }

private:
    ui32 PickIndex()
    {
        return NextIndex++ % FileIOs.size();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TConcurrentFileIOService final
    : public IFileIOService
{
private:
    IFileIOServicePtr FileIO;
    ITaskQueuePtr SQ;

public:
    TConcurrentFileIOService(
            IFileIOServicePtr fileIO,
            ITaskQueuePtr submissionQueue)
        : FileIO(std::move(fileIO))
        , SQ(std::move(submissionQueue))
    {}

    void Start() final
    {
        FileIO->Start();
        SQ->Start();
    }

    void Stop() final
    {
        SQ->Stop();
        FileIO->Stop();
    }

    void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        TFileIOCompletion* completion) final
    {
        using TFunc = void (IFileIOService::*)(
            TFileHandle&,
            i64,
            TArrayRef<char>,
            TFileIOCompletion*);

        Enqueue<static_cast<TFunc>(&IFileIOService::AsyncRead)>(
            file,
            offset,
            buffer,
            completion);
    }

    void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) final
    {
        using TFunc = void (IFileIOService::*)(
            TFileHandle&,
            i64,
            const TVector<TArrayRef<char>>&,
            TFileIOCompletion*);

        Enqueue<static_cast<TFunc>(&IFileIOService::AsyncReadV)>(
            file,
            offset,
            buffers,
            completion);
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) final
    {
        using TFunc = void (IFileIOService::*)(
            TFileHandle&,
            i64,
            TArrayRef<const char>,
            TFileIOCompletion*);

        Enqueue<static_cast<TFunc>(&IFileIOService::AsyncWrite)>(
            file,
            offset,
            buffer,
            completion);
    }

    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion) final
    {
        using TFunc = void (IFileIOService::*)(
            TFileHandle&,
            i64,
            const TVector<TArrayRef<const char>>&,
            TFileIOCompletion*);

        Enqueue<static_cast<TFunc>(&IFileIOService::AsyncWriteV)>(
            file,
            offset,
            buffers,
            completion);
    }

private:
    template <auto Func, typename ... TArgs>
    void Enqueue(TFileHandle& file, TArgs... args)
    {
        const int fd = file;
        SQ->ExecuteSimple(
            [this, fileRef = TFileHandle{fd}, args...]() mutable
            {
                Y_DEFER {
                    fileRef.Release();
                };
                std::invoke(Func, FileIO, fileRef, args...);
            });
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

IFileIOServicePtr CreateRoundRobinFileIOService(
    TVector<IFileIOServicePtr> fileIOs)
{
    return std::make_shared<TRoundRobinFileIOService>(std::move(fileIOs));
}

IFileIOServicePtr CreateRoundRobinFileIOService(
    ui32 threads,
    IFileIOServiceFactory& factory)
{
    TVector<IFileIOServicePtr> fileIOs;
    fileIOs.reserve(threads);
    for (ui32 i = 0; i != threads; ++i) {
        fileIOs.push_back(factory.CreateFileIOService());
    }

    return CreateRoundRobinFileIOService(std::move(fileIOs));
}

IFileIOServicePtr CreateConcurrentFileIOService(
    const TString& submissionThreadName,
    IFileIOServicePtr fileIO)
{
    return std::make_shared<TConcurrentFileIOService>(
        std::move(fileIO),
        CreateThreadPool(submissionThreadName, 1));
}

}   // namespace NCloud
