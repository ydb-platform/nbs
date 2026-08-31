#pragma once

#include <cloud/blockstore/libs/storage/protos/volume.pb.h>

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] inline NProto::TError ValidateRebuildMetadataCpuLimit(
    const NProto::TRebuildMetadataRequest& request)
{
    if (request.GetAllowedCpuTimePerSecond() &&
        request.GetMetadataType() != NProto::COMPACTION_MAP)
    {
        return MakeError(
            E_ARGUMENT,
            "AllowedCpuTimePerSecond is supported only for COMPACTION_MAP");
    }
    return {};
}

}   // namespace NCloud::NBlockStore::NStorage
