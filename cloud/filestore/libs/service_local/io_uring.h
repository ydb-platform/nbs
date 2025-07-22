#pragma once

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct TIoUringServiceParams
{
    static constexpr ui32 DefaultSubmissionQueueEntries = 1024;

    ui32 SubmissionQueueEntries = DefaultSubmissionQueueEntries;

    ui32 MaxKernelWorkersCount = 0;
    bool ShareKernelWorkers = false;
    bool ForceAsyncIO = false;
    bool ForceSingleBuffer = false;
};

////////////////////////////////////////////////////////////////////////////////

IFileIOServiceFactoryPtr CreateIoUringServiceFactory(
    TIoUringServiceParams params);

}   // namespace NCloud::NFileStore
