#pragma once

#include <cloud/storage/core/libs/common/public.h>

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

// To share kernel worker threads, all io_uring services must be created via a
// corresponding factory.

IFileIOServiceFactoryPtr CreateIoUringServiceFactory(
    TIoUringServiceParams params);

IFileIOServiceFactoryPtr CreateIoUringServiceNullFactory(
    TIoUringServiceParams params);

}   // namespace NCloud
