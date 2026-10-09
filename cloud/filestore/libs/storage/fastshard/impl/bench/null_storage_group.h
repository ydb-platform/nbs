#pragma once

#include <cloud/fastshard/testlib/delay_policy.h>

#include <cloud/filestore/libs/storage/fastshard/impl/factory/public.h>

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
    NCloud::NFastShard::IDelayPolicyPtr delayPolicy);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
