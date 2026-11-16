#pragma once

#include "public.h"

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <cloud/storage/core/libs/common/byte_range.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TZeroBlockStats
{
    // Bytes in the blocks which are fully covered by the request and contain
    // only zeroes.
    ui64 ZeroBlockBytes = 0;

    // The whole payload, unaligned head and tail included, is zeroes.
    bool AllZeroes = true;
};

// range is the byte range written by the request.
TZeroBlockStats CalculateZeroBlockStats(
    const NProto::TWriteDataRequest& request,
    TByteRange range);

}   // namespace NCloud::NFileStore::NStorage
