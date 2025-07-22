#include "helpers.h"

#include <cloud/blockstore/libs/storage/api/throttling_manager.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool RuleHasInvalidCoefficients(const NProto::TThrottlingRule& rule)
{
    if (!rule.HasCoefficients()) {
        return false;
    }

    const auto& coefficients = rule.GetCoefficients();
    if (coefficients.HasMaxReadBandwidth() &&
        (coefficients.GetMaxReadBandwidth() < 0.0 ||
         coefficients.GetMaxReadBandwidth() > 1.0))
    {
        return true;
    }
    if (coefficients.HasMaxPostponedWeight() &&
        (coefficients.GetMaxPostponedWeight() < 0.0 ||
         coefficients.GetMaxPostponedWeight() > 1.0))
    {
        return true;
    }
    if (coefficients.HasMaxReadIops() &&
        (coefficients.GetMaxReadIops() < 0.0 ||
         coefficients.GetMaxReadIops() > 1.0))
    {
        return true;
    }
    if (coefficients.HasBoostTime() &&
        (coefficients.GetBoostTime() < 0.0 ||
         coefficients.GetBoostTime() > 1.0))
    {
        return true;
    }
    if (coefficients.HasBoostRefillTime() &&
        (coefficients.GetBoostRefillTime() < 0.0 ||
         coefficients.GetBoostRefillTime() > 1.0))
    {
        return true;
    }
    if (coefficients.HasBoostPercentage() &&
        (coefficients.GetBoostPercentage() < 0.0 ||
         coefficients.GetBoostPercentage() > 1.0))
    {
        return true;
    }
    if (coefficients.HasMaxWriteBandwidth() &&
        (coefficients.GetMaxWriteBandwidth() < 0.0 ||
         coefficients.GetMaxWriteBandwidth() > 1.0))
    {
        return true;
    }
    if (coefficients.HasMaxWriteIops() &&
        (coefficients.GetMaxWriteIops() < 0.0 ||
         coefficients.GetMaxWriteIops() > 1.0))
    {
        return true;
    }
    if (coefficients.HasBurstPercentage() &&
        (coefficients.GetBurstPercentage() < 0.0 ||
         coefficients.GetBurstPercentage() > 1.0))
    {
        return true;
    }
    return false;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateThrottlingConfig(const NProto::TThrottlingConfig& config)
{
    auto it = FindIf(
        config.GetRules().begin(),
        config.GetRules().end(),
        RuleHasInvalidCoefficients);
    if (it != config.GetRules().end()) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Received throttler config with a rule "
                                "with invalid coefficient(s): "
                             << it->AsJSON());
    }
    return {};
}

}   // namespace NCloud::NBlockStore::NStorage
