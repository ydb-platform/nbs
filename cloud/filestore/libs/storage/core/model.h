#include "config.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

namespace NKikimrFileStore {
    class TConfig;
}

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void SetupFileStorePerformanceAndChannels(
    bool allocateMixed0Channel,
    const TStorageConfig& config,
    NKikimrFileStore::TConfig& fileStore,
    const NProto::TFileStorePerformanceProfile& clientPerformanceProfile);

}   // namespace NCloud::NFileStore::NStorage
