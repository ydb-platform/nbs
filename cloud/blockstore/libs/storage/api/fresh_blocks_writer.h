#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>

namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_FRESH_BLOCKS_WRITER_REQUESTS(xxx, ...)                      \
    xxx(WaitReady,               __VA_ARGS__)                                  \
    xxx(WaitCommit,              __VA_ARGS__)                                  \

////////////////////////////////////////////////////////////////////////////////

struct TEvFreshBlocksWriter
{
    //
    // WaitReady
    //

    struct TWaitReadyRequest
    {
    };

    struct TWaitReadyResponse
    {
    };

    //
    // WaitCommit
    //

    struct TWaitCommitRequest
    {
        ui64 CommitId;
    };

    struct TWaitCommitResponse
    {
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStoreEvents::FRESH_BLOCKS_WRITER_START,

        BLOCKSTORE_FRESH_BLOCKS_WRITER_REQUESTS(BLOCKSTORE_DECLARE_EVENT_IDS)

        EvEnd
    };

    static_assert(
        EvEnd < (int)TBlockStoreEvents::FRESH_BLOCKS_WRITER_END,
        "EvEnd expected to be < TBlockStoreEvents::FRESH_BLOCKS_WRITER_END");

    BLOCKSTORE_FRESH_BLOCKS_WRITER_REQUESTS(BLOCKSTORE_DECLARE_EVENTS)
};

}   // namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter
