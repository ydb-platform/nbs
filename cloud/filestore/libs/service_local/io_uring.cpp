#include "io_uring.h"

#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/io_uring/context.h>

#include <util/string/builder.h>
#include <util/system/file.h>

namespace NCloud::NFileStore {

using namespace NIoUring;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCompoundCompletion: TFileIOCompletion
{
    TFileIOCompletion* OriginalCompletion;

    ui32 PendingSubRequestsCount;
    ui32 TotalTransferredBytes = 0;
    NProto::TError Error;

    TCompoundCompletion(
            ui32 subRequestsCount,
            TFileIOCompletion* originalCompletion)
        : TFileIOCompletion{.Func = &TCompoundCompletion::CompletionFunc}
        , OriginalCompletion(originalCompletion)
        , PendingSubRequestsCount(subRequestsCount)
    {}

    static void CompletionFunc(
        TFileIOCompletion* obj,
        const NProto::TError& error,
        ui32 bytesTransferred)
    {
        std::unique_ptr<TCompoundCompletion> self(
            static_cast<TCompoundCompletion*>(obj));
        if (!self->HandleCompletion(error, bytesTransferred)) {
            auto* ptr = self.release();
            Y_UNUSED(ptr);
        }
    }

    bool HandleCompletion(const NProto::TError& error, ui32 bytesTransferred)
    {
        Y_ABORT_UNLESS(PendingSubRequestsCount > 0);
        --PendingSubRequestsCount;

        TotalTransferredBytes += bytesTransferred;

        if (!HasError(Error)) {
            Error = error;
        }

        if (PendingSubRequestsCount == 0) {
            OriginalCompletion->Func(
                OriginalCompletion,
                Error,
                TotalTransferredBytes);

            return true;
        }

        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TIoUringServiceBase
    : public IFileIOService
{
    TContext Context;
    const ui32 SqeFlags;

    TIoUringServiceBase(TContext::TParams params, ui32 sqeFlags)
        : Context(std::move(params))
        , SqeFlags(sqeFlags)
    {}

    void Start() final
    {
        Context.Start();
    }

    void Stop() final
    {
        Context.Stop();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TIoUringSplitBufService final
    : public TIoUringServiceBase
{
    using TIoUringServiceBase::TIoUringServiceBase;

    void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        TFileIOCompletion* completion) final
    {
        Context.AsyncRead(file, buffer, offset, completion, SqeFlags);
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) final
    {
        Context.AsyncWrite(file, buffer, offset, completion, SqeFlags);
    }

     void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) final
    {
        if (buffers.size() < 2) {
            Context.AsyncReadV(file, buffers, offset, completion, SqeFlags);
            return;
        }

        auto cb =
            std::make_unique<TCompoundCompletion>(buffers.size(), completion);

        for (const auto& buf: buffers) {
            Context.AsyncReadV(file, {&buf, 1}, offset, cb.get(), SqeFlags);
            offset += buf.size();
        }

        auto* ptr = cb.release();
        Y_UNUSED(ptr);
    }

    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion) final
    {
        if (buffers.size() < 2) {
            Context.AsyncWriteV(file, buffers, offset, completion, SqeFlags);
            return;
        }

        auto cb =
            std::make_unique<TCompoundCompletion>(buffers.size(), completion);

        for (const auto& buf: buffers) {
            Context.AsyncWriteV(file, {&buf, 1}, offset, cb.get(), SqeFlags);
            offset += buf.size();
        }

        auto* ptr = cb.release();
        Y_UNUSED(ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TIoUringService final
    : public TIoUringServiceBase
{
    using TIoUringServiceBase::TIoUringServiceBase;

    void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        TFileIOCompletion* completion) final
    {
        Context.AsyncRead(file, buffer, offset, completion, SqeFlags);
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) final
    {
        Context.AsyncWrite(file, buffer, offset, completion, SqeFlags);
    }

     void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) final
    {
        Context.AsyncReadV(file, buffers, offset, completion, SqeFlags);
    }

    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion) final
    {
        Context.AsyncWriteV(file, buffers, offset, completion, SqeFlags);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TService>
class TIoUringServiceFactory final
    : public IFileIOServiceFactory
{
private:
    const TIoUringServiceParams Params;

    std::shared_ptr<TContext> WqOwner;
    ui32 Index = 0;

public:
    explicit TIoUringServiceFactory(TIoUringServiceParams params)
        : Params(std::move(params))
    {}

    IFileIOServicePtr CreateFileIOService() final
    {
        const ui32 index = Index++;

        TContext::TParams params{
            .SubmissionThreadName = TStringBuilder()
                                    << Params.SubmissionThreadName << index,
            .CompletionThreadName = TStringBuilder()
                                    << Params.CompletionThreadName << index,
            .SubmissionQueueEntries = Params.SubmissionQueueEntries,
            .MaxKernelWorkersCount = Params.MaxKernelWorkersCount,
            .WqOwner = WqOwner.get(),
        };

        const ui32 sqeFlags = Params.ForceAsyncIO ? IOSQE_ASYNC : 0;

        auto service = std::make_shared<TService>(std::move(params), sqeFlags);

        if (Params.ShareKernelWorkers && !WqOwner) {
            WqOwner = std::shared_ptr<TContext>(service, &service->Context);
        }

        return service;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileIOServiceFactoryPtr CreateIoUringServiceFactory(
    TIoUringServiceParams params)
{
    if (params.ForceSingleBuffer) {
        return std::make_shared<
            TIoUringServiceFactory<TIoUringSplitBufService>>(std::move(params));
    }

    return std::make_shared<TIoUringServiceFactory<TIoUringService>>(
        std::move(params));
}

}   // namespace NCloud::NFileStore
