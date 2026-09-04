#pragma once

#include <util/datetime/base.h>

#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

/**
 * Source of artificial response delays for storage fakes.
 */
class IDelayPolicy
{
public:
    virtual ~IDelayPolicy() = default;

    /**
     * Samples the delay for the next response.
     *
     * @return - The sampled delay.
     */
    [[nodiscard]] virtual TDuration NextDelay() = 0;
};

using IDelayPolicyPtr = std::shared_ptr<IDelayPolicy>;

////////////////////////////////////////////////////////////////////////////////

/**
 * Returns a policy which samples delays from a lognormal distribution.
 * The distribution parameters are derived so that the sampled delays
 * have the requested mean and standard deviation.
 *
 * @param mean - Mean of the sampled delays.
 * @param stddev - Standard deviation of the sampled delays.
 * @return - The constructed policy.
 */
IDelayPolicyPtr CreateLognormalDelayPolicy(TDuration mean, TDuration stddev);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
