#pragma once

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/partition_common/model/fresh_blob.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_PARTITION_COMMON_REQUESTS_PRIVATE(xxx, ...)                 \
    xxx(TrimFreshLog,           __VA_ARGS__)                                   \
// BLOCKSTORE_PARTITION_COMMON_REQUESTS_PRIVATE

////////////////////////////////////////////////////////////////////////////////

struct TEvPartitionCommonPrivate
{
    //
    // TrimFreshLog
    //

    struct TTrimFreshLogRequest
    {
    };

    struct TTrimFreshLogResponse
    {
    };

    //
    // OperationCompleted
    //

    struct TOperationCompleted
    {
        ui64 TotalCycles = 0;
        ui64 ExecCycles = 0;

        ui64 CommitId = 0;
    };

    //
    // LoadFreshBlobsCompleted
    //

    struct TLoadFreshBlobsCompleted
    {
        TVector<TFreshBlob> Blobs;

        TLoadFreshBlobsCompleted(TVector<TFreshBlob> blobs)
            : Blobs(std::move(blobs))
        {}
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStorePrivateEvents::PARTITION_COMMON_START,

        BLOCKSTORE_PARTITION_COMMON_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENT_IDS)

        EvLoadFreshBlobsCompleted,
        EvTrimFreshLogCompleted,

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStorePrivateEvents::PARTITION_COMMON_END,
        "EvEnd expected to be < TBlockStorePrivateEvents::PARTITION_COMMON_END");

    BLOCKSTORE_PARTITION_COMMON_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENTS)

    using TEvLoadFreshBlobsCompleted = TResponseEvent<TLoadFreshBlobsCompleted, EvLoadFreshBlobsCompleted>;
    using TEvTrimFreshLogCompleted = TResponseEvent<TOperationCompleted, EvTrimFreshLogCompleted>;
};

}   // namespace NCloud::NBlockStore::NStorage
