#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/context.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TRequestTime
{
    TDuration TotalTime;
    TDuration ExecutionTime;

    explicit operator bool() const
    {
        return TotalTime || ExecutionTime;
    }
};

struct TCallContext final
    : public TCallContextBase
{
private:
    TAtomic PossiblePostponeMicroSeconds = 0;
    TAtomic SilenceRetriableErrors = false;

public:
    TCallContext(ui64 requestId = 0);

    TDuration GetPossiblePostponeDuration() const;
    void SetPossiblePostponeDuration(TDuration d);

    bool GetSilenceRetriableErrors() const;
    void SetSilenceRetriableErrors(bool silence);

    TRequestTime CalcRequestTime(ui64 nowCycles) const;
};

}   // namespace NCloud::NBlockStore
