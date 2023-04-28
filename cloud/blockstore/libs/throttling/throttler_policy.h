#pragma once

#include "public.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IThrottlerPolicy
{
    virtual ~IThrottlerPolicy() = default;

    virtual TDuration SuggestDelay(
        TInstant now,
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        size_t byteCount) = 0;

    virtual double CalculateCurrentSpentBudgetShare(TInstant ts) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

IThrottlerPolicyPtr CreateThrottlerPolicyStub();

}   // namespace NCloud::NBlockStore
