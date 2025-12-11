#include "throttler_policy.h"

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TThrottlerPolicyStub final: public IThrottlerPolicy
{
public:
    TDuration SuggestDelay(
        TInstant now,
        NCloud::NProto::EStorageMediaKind mediaKind,
        EBlockStoreRequest requestType,
        size_t byteCount) override
    {
        Y_UNUSED(now);
        Y_UNUSED(mediaKind);
        Y_UNUSED(requestType);
        Y_UNUSED(byteCount);

        return TDuration::Zero();
    }

    double CalculateCurrentSpentBudgetShare(TInstant ts) const override
    {
        Y_UNUSED(ts);

        return 0.0;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IThrottlerPolicyPtr CreateThrottlerPolciyStub()
{
    return std::make_shared<TThrottlerPolicyStub>();
}

}   // namespace NCloud::NBlockStore
