#pragma once

#include "delay_policy.h"

// XXX will refactor this out of naive_mirrored separately very soon
#include <cloud/filestore/libs/storage/fastshard/impl/naive_mirrored/shard.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

/**
 * Returns a factory which builds storage groups that do no IO: each
 * request waits for a delay sampled from the policy and succeeds. Reads
 * return zero-filled pages. Must be used from a fiber - the delays are
 * implemented via fiber sleeps.
 *
 * @param delayPolicy - Source of the per-request delays.
 * @return - The constructed factory.
 */
IStorageGroupFactoryPtr CreateNullStorageGroupFactory(
    IDelayPolicyPtr delayPolicy);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
