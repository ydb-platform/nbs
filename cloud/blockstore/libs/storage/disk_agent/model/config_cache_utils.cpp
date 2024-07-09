#include "config_cache_utils.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/string/builder.h>
#include <util/system/fs.h>

namespace NCloud::NBlockStore::NStorage {

NProto::TError SaveDiskAgentConfig(
    const TString& path,
    NProto::TDiskAgentConfig proto)
{
    if (path.empty()) {
        return MakeError(S_FALSE, "Path is empty");
    }

    const TString tmpPath{path + ".tmp"};
    SerializeToTextFormat(proto, tmpPath);

    if (!NFs::Rename(tmpPath, path)) {
        const auto ec = errno;
        return MakeError(
            MAKE_SYSTEM_ERROR(ec),
            TStringBuilder() << strerror(ec));
    }
    return MakeError(S_OK);
}

}   // namespace NCloud::NBlockStore::NStorage
