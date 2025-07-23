#pragma once

#include <cloud/storage/core/libs/common/public.h>

#include <util/generic/string.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct TIoUringServiceParams
{
    static constexpr ui32 DefaultSubmissionQueueEntries = 1024;

    TString SubmissionThreadName = "IO.SQ";
    TString CompletionThreadName = "IO.CQ";

    ui32 SubmissionQueueEntries = DefaultSubmissionQueueEntries;
};

////////////////////////////////////////////////////////////////////////////////

IFileIOServiceFactoryPtr CreateIoUringServiceFactory(
    TIoUringServiceParams params);

IFileIOServiceFactoryPtr CreateIoUringServiceNullFactory(
    TIoUringServiceParams params);

}   // namespace NCloud
