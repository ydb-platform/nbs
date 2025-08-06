#include "service.h"

#include "context.h"

#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/common/thread_pool.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/system/error.h>
#include <util/system/file.h>
#include <util/system/sanitizers.h>
#include <util/system/thread.h>

#include <liburing.h>
#include <sys/eventfd.h>

namespace NCloud {

using namespace NIoUring;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TIoUringServiceBase
    : public IFileIOService
{
    TContext Context;
    const ui32 SqeFlags;

    explicit TIoUringServiceBase(TContext::TParams params, ui32 sqeFlags)
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

    void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) final
    {
        Context.AsyncReadV(file, buffers, offset, completion, SqeFlags);
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) final
    {
        Context.AsyncWrite(file, buffer, offset, completion, SqeFlags);
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

struct TIoUringServiceNull final
    : public TIoUringServiceBase
{
    using TIoUringServiceBase::TIoUringServiceBase;

    void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset);

        Context.PostCompletion(completion, buffer.size());
    }

    void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset);

        ui32 len = 0;
        for (const auto& buf: buffers) {
            len += static_cast<ui32>(buf.size());
        }

        Context.PostCompletion(completion, len);
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset);

        Context.PostCompletion(completion, buffer.size());
    }

    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset);

        ui32 len = 0;
        for (const auto& buf: buffers) {
            len += static_cast<ui32>(buf.size());
        }

        Context.PostCompletion(completion, len);
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
    using TFactory = TIoUringServiceFactory<TIoUringService>;

    return std::make_shared<TFactory>(std::move(params));
}

IFileIOServiceFactoryPtr CreateIoUringServiceNullFactory(
    TIoUringServiceParams params)
{
    using TFactory = TIoUringServiceFactory<TIoUringServiceNull>;

    return std::make_shared<TFactory>(std::move(params));
}

}   // namespace NCloud
