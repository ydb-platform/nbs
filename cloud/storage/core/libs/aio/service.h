#pragma once

#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/common/public.h>

#include <memory>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct TAioServiceParams
{
    static constexpr ui32 DefaultMaxEvents = 1024;

    ui32 MaxEvents = DefaultMaxEvents;

    TString CompletionThreadName = "AIO";
};

IFileIOServicePtr CreateAIOService(TAioServiceParams params = {});

// PROTOTYPE: an AIO service without a completion thread. Every iocb carries
// an eventfd; the owner waits for the fd to become readable and reaps the
// completions on its own thread with ProcessCompletions().
struct IExternalCompletionFileIOService
    : IFileIOService
{
    virtual int GetCompletionFd() = 0;
    // returns the number of completions reaped
    virtual size_t ProcessCompletions() = 0;
    virtual size_t InFlight() = 0;
};

std::shared_ptr<IExternalCompletionFileIOService>
CreateAIOServiceExternalCompletion(TAioServiceParams params = {});
IFileIOServiceFactoryPtr CreateAIOServiceFactory(TAioServiceParams params = {});

IFileIOServicePtr CreateThreadedAIOService(
    ui32 threadCount,
    TAioServiceParams params = {});

}   // namespace NCloud
