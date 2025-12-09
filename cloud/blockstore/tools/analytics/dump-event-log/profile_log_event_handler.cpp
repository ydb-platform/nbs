#include "profile_log_event_handler.h"

#include <cloud/blockstore/libs/service/request.h>

namespace NCloud::NBlockStore {

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

///////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
