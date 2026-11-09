#pragma once

#include <cloud/storage/core/libs/actors/public.h>

#include <util/datetime/base.h>
#include <util/system/types.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TTabletProxyConfig
{
    int LogComponent = 0;

    ui32 PipeClientRetryCount = 0;
    TDuration PipeClientMinRetryTime;
    TDuration PipeClientMaxRetryTime;

    TDuration PipeInactivityTimeout = TDuration::Minutes(5);
};

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateTabletProxy(TTabletProxyConfig config);

}   // namespace NCloud::NStorage
