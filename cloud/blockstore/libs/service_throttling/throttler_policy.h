#pragma once

#include <cloud/blockstore/libs/throttling/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TThrottlingServiceConfig
{
    const ui64 MaxReadBandwidth;
    const ui64 MaxWriteBandwidth;
    const ui32 MaxReadIops;
    const ui32 MaxWriteIops;
    const TDuration MaxBurstTime;

    TThrottlingServiceConfig(
            ui64 maxReadBandwidth,
            ui64 maxWriteBandwidth,
            ui32 maxReadIops,
            ui32 maxWriteIops,
            TDuration maxBurstTime)
        : MaxReadBandwidth(maxReadBandwidth)
        , MaxWriteBandwidth(maxWriteBandwidth)
        , MaxReadIops(maxReadIops)
        , MaxWriteIops(maxWriteIops)
        , MaxBurstTime(maxBurstTime)
    {}
};

////////////////////////////////////////////////////////////////////////////////

IThrottlerPolicyPtr CreateServiceThrottlerPolicy(
    const TThrottlingServiceConfig& config);

}   // namespace NCloud::NBlockStore
