#pragma once

#include "public.h"

#include <cloud/filestore/public/api/protos/data.pb.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TZeroBlockStats
{
    // Payload size.
    ui64 Bytes = 0;

    // Bytes in the blocks which are fully covered by the request and contain
    // only zeroes.
    ui64 ZeroBlockBytes = 0;

    // The whole payload, unaligned head and tail included, is zeroes.
    bool AllZeroes = true;
};

TZeroBlockStats CalculateZeroBlockStats(
    const NProto::TWriteDataRequest& request,
    ui32 blockSize);

}   // namespace NCloud::NFileStore::NStorage
