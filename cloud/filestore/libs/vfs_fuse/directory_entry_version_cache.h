#pragma once

#include "public.h"

#include "fuse.h"

#include <util/generic/hash.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/system/mutex.h>

namespace NCloud::NFileStore::NFuse {

// Tracks per-name changes while a directory handle is open.
// ReadDir uses these versions to avoid returning cacheable entries when
// a listing races with unlink or rename of the same child.
//
// DirectoryHandleCache registers handles here, so when we don't have
// handles pointing to directory, we erase all information regarding
// entry changes to avoid unbound memory usage.
class TDirectoryEntryVersionCache
{
private:
    struct TDirectoryState
    {
        ui64 RefCount = 0;
        THashMap<TString, ui64> ChildVersions;
    };

    mutable TMutex Lock;
    THashMap<fuse_ino_t, TDirectoryState> Directories;

public:
    void RegisterHandle(fuse_ino_t directory);
    void UnregisterHandle(fuse_ino_t directory);
    void AdvanceVersion(fuse_ino_t directory, const TString& name, ui64 version);
    ui64 GetVersion(fuse_ino_t directory, TStringBuf name) const;
};

}   // namespace NCloud::NFileStore::NFuse
