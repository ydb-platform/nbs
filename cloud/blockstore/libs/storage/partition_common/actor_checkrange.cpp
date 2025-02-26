#include "actor_checkrange.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/error.pb.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

std::optional<NProto::TError> ValidateBlocksCount(
    ui64 blocksCount,
    ui64 bytesPerStripe,
    ui64 blockSize,
    ui64 checkRangeMaxRangeSize)
{
    ui64 maxBlocksPerRequest = Min(
        bytesPerStripe / blockSize,
        checkRangeMaxRangeSize / blockSize);

    if (blocksCount > maxBlocksPerRequest) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Too many blocks requested: "
                             << blocksCount
                             << " Max blocks per request: "
                             << maxBlocksPerRequest);
    }
    return std::nullopt;
}

}   // namespace NCloud::NBlockStore::NStorage
