#pragma once

#include "public.h"

#include "request_buffer.h"
#include "request_info.h"

#include <cloud/blockstore/libs/common/block_range.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TWriteBufferRequestData
{
    TRequestInfoPtr RequestInfo;
    TBlockRange32 Range;
    IWriteBlocksHandlerPtr Handler;
    bool ReplyLocal;

    TWriteBufferRequestData(
        TRequestInfoPtr requestInfo,
        TBlockRange32 range,
        IWriteBlocksHandlerPtr handler,
        bool replyLocal)
        : RequestInfo(std::move(requestInfo))
        , Range(range)
        , Handler(std::move(handler))
        , ReplyLocal(replyLocal)
    {}
};

struct TRequestGroup
{
    TVector<TRequestInBuffer<TWriteBufferRequestData>*> Requests;
    ui32 Weight = 0;
};

struct TRequestGrouping
{
    TVector<TRequestGroup> Groups;
    TRequestInBuffer<TWriteBufferRequestData>* FirstUngrouped = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

TRequestGrouping GroupRequests(
    TVector<TRequestInBuffer<TWriteBufferRequestData>>& requests,
    ui32 totalWeight,
    ui32 minWeight,
    ui32 maxWeight,
    ui32 maxRange);

}   // namespace NCloud::NBlockStore::NStorage
