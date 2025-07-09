#pragma once

#include <cloud/storage/core/libs/common/public.h>

#include <functional>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct TIoUringServiceParams
{
    static constexpr ui32 DefaultSubmissionQueueEntries = 1024;

    TString SubmissionThreadName = "IO.SQ";
    TString CompletionThreadName = "IO.CQ";

    ui32 SubmissionQueueEntries = DefaultSubmissionQueueEntries;

    ui32 BoundWorkers = 0;
    ui32 UnboundWorkers = 0;
};

////////////////////////////////////////////////////////////////////////////////

IFileIOServicePtr CreateIoUringService(TIoUringServiceParams params);
IFileIOServicePtr CreateIoUringServiceNull(TIoUringServiceParams params);

// To share kernel worker threads, all io_uring services must be created by
// calling a corresponding factory function.

std::function<IFileIOServicePtr()> CreateIoUringServiceFactory(
    TIoUringServiceParams params);

std::function<IFileIOServicePtr()> CreateIoUringServiceNullFactory(
    TIoUringServiceParams params);

}   // namespace NCloud
