#pragma once

#include "public.h"

#include <util/datetime/base.h>
#include <util/generic/maybe.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TRetryPolicy
{
public:
    const TDuration TimeoutIncrement;
    const TDuration TimeoutMax;

    TDuration CurrentTimeout = TDuration::Zero();
    TInstant CurrentDeadline = TInstant::Zero();

public:
    TRetryPolicy(TDuration increment, TDuration max);

    TDuration GetCurrentTimeout() const;
    TInstant GetCurrentDeadline() const;

    void Reset(TInstant now);
    void Update(TInstant now);
};

}   // namespace NCloud::NBlockStore::NStorage
