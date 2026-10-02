#pragma once

#include <util/generic/yexception.h>
#include <util/system/file.h>
#include <util/system/file_lock.h>
#include <util/system/flock.h>

#include <cerrno>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

////////////////////////////////////////////////////////////////////////////////

// TFileLock opens files with OpenAlways. Recovery tooling must not recreate a
// state file which disappeared between discovery and locking, so keep the
// existing-file semantics local to the tool.
class TExistingFileLock
{
private:
    TFile File;
    EFileLockType Type;

public:
    TExistingFileLock(
        const TString& path,
        EFileLockType type,
        EOpenModeFlag accessMode = EOpenModeFlag::RdOnly)
        : File(path, EOpenModeFlag::OpenExisting | accessMode)
        , Type(type)
    {}

    bool TryAcquire()
    {
        try {
            File.Flock(
                (Type == EFileLockType::Exclusive ? LOCK_EX : LOCK_SH) |
                LOCK_NB);
            return true;
        } catch (const TSystemError& e) {
            if (e.Status() != EWOULDBLOCK) {
                throw;
            }
            return false;
        }
    }

    i64 GetLength() const
    {
        return File.GetLength();
    }

    const TFile& GetFile() const
    {
        return File;
    }
};

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
