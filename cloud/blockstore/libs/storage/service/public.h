#pragma once

#include <memory>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct IVolumesStatsUploader;
using IVolumesStatsUploaderPtr = std::shared_ptr<IVolumesStatsUploader>;

}   // namespace NCloud::NBlockStore::NStorage
