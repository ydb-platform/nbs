#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/service/request.h>

namespace NCloud::NBlockStore::NProto {

////////////////////////////////////////////////////////////////////////////////

struct TReadFreshBlocksRequest
    : public TReadBlocksRequest
{
    ui64 CommitId = Max<ui64>();
};

struct TReadFreshBlocksResponse: public TReadBlocksResponse
{
    TVector<ui64> BlockMarkCommitIds;
};

}   // namespace NCloud::NBlockStore::NProto

namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_FRESH_BLOCKS_WRITER_SIMPLE_REQUESTS(xxx, ...)               \
    xxx(WaitReady,               __VA_ARGS__)                                  \
// BLOCKSTORE_FRESH_BLOCKS_WRITER_REQUESTS

#define BLOCKSTORE_FRESH_BLOCKS_WRITER_PROTO_REQUESTS(xxx, ...)                \
    xxx(ReadFreshBlocks,         __VA_ARGS__)                                  \
// BLOCKSTORE_FRESH_BLOCKS_WRITER_PROTO_REQUESTS

#define BLOCKSTORE_FRESH_BLOCKS_WRITER_REQUESTS(xxx, ...)                     \
    BLOCKSTORE_FRESH_BLOCKS_WRITER_SIMPLE_REQUESTS(xxx,  __VA_ARGS__)         \
    BLOCKSTORE_FRESH_BLOCKS_WRITER_PROTO_REQUESTS(xxx,   __VA_ARGS__)         \
// BLOCKSTORE_FRESH_BLOCKS_WRITER_REQUESTS

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

    BLOCKSTORE_FRESH_BLOCKS_WRITER_SIMPLE_REQUESTS(BLOCKSTORE_DECLARE_EVENTS)
    BLOCKSTORE_FRESH_BLOCKS_WRITER_PROTO_REQUESTS(
        BLOCKSTORE_DECLARE_PROTO_EVENTS)
};

}   // namespace NCloud::NBlockStore::NStorage::NFreshBlocksWriter
