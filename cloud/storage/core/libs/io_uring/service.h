#pragma once

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud::NIoUring {

////////////////////////////////////////////////////////////////////////////////

struct TServiceParams
{
    static constexpr ui32 DefaultSubmissionQueueEntries = 1024;

    TString SubmissionThreadName = "IO.SQ";
    TString CompletionThreadName = "IO.CQ";

    ui32 SubmissionQueueEntries = DefaultSubmissionQueueEntries;

    ui32 MaxKernelWorkersCount = 0;
    bool ShareKernelWorkers = false;
};

////////////////////////////////////////////////////////////////////////////////

IFileIOServiceFactoryPtr CreateServiceFactory(TServiceParams params);
IFileIOServiceFactoryPtr CreateServiceNullFactory(TServiceParams params);

}   // namespace NCloud::NIoUring
