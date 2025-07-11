#pragma once

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct TAioServiceParams
{
    static constexpr ui32 DefaultMaxEvents = 1024;

    ui32 MaxEvents = DefaultMaxEvents;

    TString CompletionThreadName = "AIO";
};

IFileIOServicePtr CreateAIOService(TAioServiceParams params = {});
IFileIOServiceFactoryPtr CreateAIOServiceFactory(TAioServiceParams params = {});

IFileIOServicePtr CreateThreadedAIOService(
    ui32 threadCount,
    TAioServiceParams params = {});

}   // namespace NCloud
