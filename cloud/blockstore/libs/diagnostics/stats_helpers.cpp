#include "stats_helpers.h"

#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/request_helpers.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TRequestCounters MakeRequestCounters(
    ITimerPtr timer,
    TRequestCounters::EOptions options,
    EHistogramCounterOptions histogramCounterOptions)
{
    return TRequestCounters(
        std::move(timer),
        BlockStoreRequestsCount,
        [] (TRequestCounters::TRequestType t) {
            Y_DEBUG_ABORT_UNLESS(t < BlockStoreRequestsCount);
            const auto bt = static_cast<EBlockStoreRequest>(t);
            return GetBlockStoreRequestName(bt);
        },
        [] (TRequestCounters::TRequestType t) {
            Y_DEBUG_ABORT_UNLESS(t < BlockStoreRequestsCount);
            const auto bt = static_cast<EBlockStoreRequest>(t);
            return IsNonLocalReadWriteRequest(bt);
        },
        options,
        histogramCounterOptions
    );
}

}   // namespace NCloud::NBlockStore
