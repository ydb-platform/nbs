#include "volume_id.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TVolumeIdPtr MakeVolumeId(
    const TString& diskId,
    const TString& cloudId,
    const TString& folderId)
{
    return std::make_shared<TVolumeId>(TVolumeId{
        .DiskId = diskId,
        .CloudId = cloudId,
        .FolderId = folderId});
}

}   // namespace NCloud::NBlockStore
