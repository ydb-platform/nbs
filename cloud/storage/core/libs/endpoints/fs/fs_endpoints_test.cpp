#include "fs_endpoints_test.h"

#include <cloud/storage/core/libs/endpoints/iface/endpoints_test.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <util/datetime/base.h>
#include <util/folder/path.h>
#include <util/generic/hash.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/file.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

NProto::TError CreateEndpointsDirectory(const TString& dirPath)
{
    TFsPath fsPath(dirPath);
    fsPath.MkDir();

    if (!fsPath.IsDirectory()) {
        return MakeError(
            E_FAIL,
            TStringBuilder()
                << "Failed to create directory " << fsPath.GetPath());
    }

    return {};
}

NProto::TError CleanUpEndpointsDirectory(const TString& dirPath)
{
    try {
        TFsPath fsPath(dirPath);
        fsPath.ForceDelete();
        return {};
    } catch (...) {
        return MakeError(E_IO, CurrentExceptionMessage());
    }
}

}   // namespace NCloud
