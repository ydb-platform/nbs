#pragma once

#include <util/system/file.h>
#include <util/system/file_lock.h>
#include <util/system/flock.h>

#include <cerrno>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

////////////////////////////////////////////////////////////////////////////////

// The lock is associated with the open file and is released when its last
// TFile owner is destroyed. Keeping the file alive provides the RAII guard for
// the required lock lifetime.
inline bool TryLock(TFile& file, EFileLockType type)
{
    try {
        file.Flock(
            (type == EFileLockType::Exclusive ? LOCK_EX : LOCK_SH) | LOCK_NB);
        return true;
    } catch (const TSystemError& e) {
        if (e.Status() != EWOULDBLOCK) {
            throw;
        }
        return false;
    }
}

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
