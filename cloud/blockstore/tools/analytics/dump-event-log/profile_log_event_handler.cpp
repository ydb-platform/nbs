#include "profile_log_event_handler.h"

#include <cloud/blockstore/libs/service/request.h>

namespace NCloud::NBlockStore {

///////////////////////////////////////////////////////////////////////////////

void TInflightCounter::Add(ui64 bytes)
{
    InflightCount++;
    InflightBytes += bytes;
}

void TInflightCounters::Add(ui32 requestType, ui64 bytes)
{
    if (IsReadRequestType(requestType)) {
        Read.Add(bytes);
    } else if (IsWriteRequestType(requestType)) {
        Write.Add(bytes);
    }
}

void TInflightData::Add(
    NCloud::NProto::EStorageMediaKind mediaKind,
    ui32 requestType,
    bool sameDisk,
    ui64 bytes)
{
    if (IsDiskRegistryMediaKind(mediaKind)) {
        HostDiskRegistryBased.Add(requestType, bytes);
    } else {
        HostBlobStorageBased.Add(requestType, bytes);
    }
    if (sameDisk) {
        Disk.Add(requestType, bytes);
    }
}

///////////////////////////////////////////////////////////////////////////////

bool IsReadRequestType(ui32 requestType)
{
    return requestType == static_cast<ui32>(EBlockStoreRequest::ReadBlocks) ||
           requestType ==
               static_cast<ui32>(EBlockStoreRequest::ReadBlocksLocal);
}

bool IsWriteRequestType(ui32 requestType)
{
    return requestType == static_cast<ui32>(EBlockStoreRequest::WriteBlocks) ||
           requestType ==
               static_cast<ui32>(EBlockStoreRequest::WriteBlocksLocal);
}

bool IsZeroRequestType(ui32 requestType)
{
    return requestType == static_cast<ui32>(EBlockStoreRequest::ZeroBlocks);
}

bool IsReadWriteZeroRequestType(ui32 requestType)
{
    return IsReadRequestType(requestType) || IsWriteRequestType(requestType) ||
           IsZeroRequestType(requestType);
}

///////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
