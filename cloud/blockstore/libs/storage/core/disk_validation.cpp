#include "disk_validation.h"

#include <cloud/blockstore/libs/common/constants.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NCloud::NProto::TError ValidateBlockSize(
    ui64 blockSize,
    NProto::EStorageMediaKind mediaKind)
{
    const auto minBlockSize = mediaKind != NProto::STORAGE_MEDIA_SSD_LOCAL
                                  ? DefaultBlockSize
                                  : DefaultLocalSSDBlockSize;

    if (blockSize < minBlockSize || blockSize > MaxBlockSize) {
        return MakeError(NCloud::E_ARGUMENT,
            TStringBuilder() << "block size should be >= " << minBlockSize
            << " and <= " << MaxBlockSize);
    }

    if ((blockSize & (blockSize - 1)) != 0) {
        return MakeError(E_ARGUMENT, "block size should be a power of 2");
    }

    return {};
}

NProto::TError ValidateMediaKindForDiskFromDevices(
    NProto::EStorageMediaKind mediaKind)
{
    if (!IsDiskRegistryMediaKind(mediaKind) ||
        IsReliableDiskRegistryMediaKind(mediaKind))
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "storage media kind " << MediaKindToString(mediaKind)
                << " is not allowed, expected one of: ssd_nonrepl, "
                   "hdd_nonrepl, ssd_local, hdd_local");
    }

    return {};
}

}   // namespace NCloud::NBlockStore::NStorage
