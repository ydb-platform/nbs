#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/context.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TCallContext final: public TCallContextBase
{
private:
    TAtomic SilenceRetriableErrors = false;
    TAtomic HasUncountableRejects = false;

    // Latency accounting cannot reconstruct wall-clock execution time when
    // several subrequests add overlapping waits to this shared context. This
    // marker is deliberately separate from the legacy timing fields: existing
    // request metrics keep their current summed-wait semantics.
    TAtomic HasParallelSubRequests = false;

public:
    TCallContext(ui64 requestId = 0);

    bool GetSilenceRetriableErrors() const;
    void SetSilenceRetriableErrors(bool silence);

    bool GetHasUncountableRejects() const;
    void SetHasUncountableRejects();

    bool GetHasParallelSubRequests() const;
    void SetHasParallelSubRequests();
};

////////////////////////////////////////////////////////////////////////////////

inline TCallContextPtr CreateCallContext(ui64 requestId = 0)
{
    return MakeIntrusive<TCallContext>(requestId);
}

////////////////////////////////////////////////////////////////////////////////

TCallContextPtr ToBlockStoreCallContext(TCallContextBasePtr callContext);

}   // namespace NCloud::NBlockStore
