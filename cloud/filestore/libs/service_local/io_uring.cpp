#include "io_uring.h"

#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/io_uring/context.h>
#include <cloud/storage/core/libs/io_uring/factory.h>

#include <util/system/file.h>

namespace NCloud::NFileStore {

using namespace NIoUring;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCompoundCompletion: TFileIOCompletion
{
    TFileIOCompletion* OriginalCompletion;

    ui32 SubRequestsCount;
    ui32 TotalTransferredBytes = 0;
    NProto::TError Error;

    TCompoundCompletion(
            ui32 subRequestsCount,
            TFileIOCompletion* originalCompletion)
        : TFileIOCompletion{.Func = &TCompoundCompletion::CompletionFunc}
        , OriginalCompletion(originalCompletion)
        , SubRequestsCount(subRequestsCount)
    {
        Y_ABORT_UNLESS(OriginalCompletion);
    }

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
        Y_ABORT_UNLESS(SubRequestsCount > 0);
        --SubRequestsCount;

        TotalTransferredBytes += bytesTransferred;

        if (!HasError(Error)) {
            Error = error;
        }

        if (SubRequestsCount == 0) {
            OriginalCompletion->Func(
                OriginalCompletion,
                Error,
                TotalTransferredBytes);
        }

        return SubRequestsCount == 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TIoUringSplitBufService final
    : public IFileIOService
{
    TContext Context;
    const ui32 SqeFlags;

    TIoUringSplitBufService(TContext::TParams params, ui32 sqeFlags)
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileIOServiceFactoryPtr CreateIoUringServiceFactory(
    TIoUringServiceParams params)
{
    NCloud::TIoUringServiceParams ioUringParams{
        .SubmissionQueueEntries = params.SubmissionQueueEntries,
        .MaxKernelWorkersCount = params.MaxKernelWorkersCount,
        .ForceAsyncIO = params.ForceAsyncIO,
    };

    if (params.ForceSingleBuffer) {
        return std::make_shared<
            TIoUringServiceFactory<TIoUringSplitBufService>>(
            std::move(ioUringParams));
    }

    return NCloud::CreateIoUringServiceFactory({
        .SubmissionQueueEntries = params.SubmissionQueueEntries,
        .MaxKernelWorkersCount = params.MaxKernelWorkersCount,
        .ForceAsyncIO = params.ForceAsyncIO,
    });
}

}   // namespace NCloud::NFileStore
