#include "volume_labels.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TVolumeLabelsPtr MakeVolumeLabels(
    const TString& diskId,
    const TString& cloudId,
    const TString& folderId)
{
    return std::make_shared<TVolumeLabels>(TVolumeLabels{
        .DiskId = diskId,
        .CloudId = cloudId,
        .FolderId = folderId});
}

}   // namespace NCloud::NBlockStore
