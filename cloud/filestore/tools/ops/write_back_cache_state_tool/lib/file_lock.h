#pragma once

#include <cloud/storage/core/libs/common/error.h>

#include <util/string/printf.h>
#include <util/system/error.h>
#include <util/system/file.h>
#include <util/system/flock.h>

#include <cerrno>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

////////////////////////////////////////////////////////////////////////////////

// The lock is associated with the open file and is released when its last
// TFile owner is destroyed. Keeping the file alive provides the RAII guard for
// the required lock lifetime.
inline TResultOrError<bool> TryLock(TFile& file, bool exclusive)
{
    const auto mode = (exclusive ? LOCK_EX : LOCK_SH) | LOCK_NB;
    if (::Flock(file.GetHandle(), mode) == 0) {
        return true;
    }

    const int error = LastSystemError();
    if (error == EWOULDBLOCK) {
        return false;
    }

    return MakeError(
        E_IO,
        Sprintf(
            "Failed to lock state file '%s': %s",
            file.GetName().c_str(),
            LastSystemErrorText(error)));
}

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
