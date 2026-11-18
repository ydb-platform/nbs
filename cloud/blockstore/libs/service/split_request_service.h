#pragma once

#include "public.h"

#include <util/datetime/base.h>

#include <optional>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// All finish times are relative to the same fork on the caller's clock.
class TParallelRequestLatency
{
private:
    TDuration LastFinish;
    TDuration LastFinishWithoutThrottling;
    bool Complete = true;

public:
    void AddPart(TDuration start, TDuration finish,
                 std::optional<TDuration> throttleDelay);

    std::optional<TDuration> GetThrottleDelay() const;
};

// A service for splitting requests by stripe boundaries or DiskRegistry-based
// disk devices.
IBlockStorePtr CreateSplitRequestService(IBlockStorePtr service);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
