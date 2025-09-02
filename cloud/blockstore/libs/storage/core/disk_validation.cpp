#include "disk_validation.h"

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

}   // namespace NCloud::NBlockStore::NStorage
