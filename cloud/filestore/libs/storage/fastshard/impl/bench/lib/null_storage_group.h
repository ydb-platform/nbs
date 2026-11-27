#pragma once

#include <cloud/fastshard/libs/sn/testlib/delay_policy.h>
#include <cloud/fastshard/libs/storage_group/storage_group.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

/**
 * Returns a storage group that does no IO: each request waits for a delay
 * sampled from the policy and succeeds. Reads return zero-filled pages.
 * Must be used from a fiber - the delays are implemented via fiber sleeps.
 *
 * @param delayPolicy - Source of the per-request delays.
 * @param pageSize - Size of the pages in a ReadPages response.
 * @return - The constructed group.
 */
NCloud::NFastShard::IStorageGroupPtr CreateNullStorageGroup(
    NCloud::NFastShard::IDelayPolicyPtr delayPolicy,
    ui32 pageSize);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
