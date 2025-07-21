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

    TIoUringServiceBase(TContext::TParams params, TContext* wqOwner)
        : Context(std::move(params), wqOwner)
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
        Context.AsyncRead(file, buffer, offset, completion);
    }

    void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) final
    {
        Context.AsyncReadV(file, buffers, offset, completion);
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) final
    {
        Context.AsyncWrite(file, buffer, offset, completion);
    }

    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion) final
    {
        Context.AsyncWriteV(file, buffers, offset, completion);
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
        Y_UNUSED(file, offset, buffer);

        Context.AsyncNOP(completion);
    }

    void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset, buffers);

        Context.AsyncNOP(completion);
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset, buffer);

        Context.AsyncNOP(completion);
    }

    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion) final
    {
        Y_UNUSED(file, offset, buffers);

        Context.AsyncNOP(completion);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TService>
class TIoUringServiceFactory final
    : public IFileIOServiceFactory
{
private:
    const TContext::TParams Params;

    std::shared_ptr<TContext> WqOwner;
    ui32 Index = 0;

public:
    explicit TIoUringServiceFactory(TIoUringServiceParams params)
        : Params({
              .SubmissionThreadName = std::move(params.SubmissionThreadName),
              .CompletionThreadName = std::move(params.CompletionThreadName),
              .SubmissionQueueEntries = params.SubmissionQueueEntries,
              .MaxKernelWorkersCount = params.MaxKernelWorkersCount,
              .ShareKernelWorkers = params.ShareKernelWorkers,
          })
    {}

    IFileIOServicePtr CreateFileIOService() final
    {
        const ui32 index = Index++;

        TContext::TParams params = Params;
        params.SubmissionThreadName = TStringBuilder()
                                      << Params.SubmissionThreadName << index;
        params.CompletionThreadName = TStringBuilder()
                                      << Params.CompletionThreadName << index;

        auto service =
            std::make_shared<TService>(std::move(params), WqOwner.get());

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
