#include "directory_entry_version_cache.h"

namespace NCloud::NFileStore::NFuse {

void TDirectoryEntryVersionCacheShard::RegisterHandle(fuse_ino_t directory)
{
    with_lock (Lock) {
        ++Directories[directory].RefCount;
    }
}

void TDirectoryEntryVersionCacheShard::UnregisterHandle(fuse_ino_t directory)
{
    with_lock (Lock) {
        auto it = Directories.find(directory);
        if (it == Directories.end()) {
            return;
        }

        auto& state = it->second;
        if (--state.RefCount == 0) {
            Directories.erase(it);
        }
    }
}

void TDirectoryEntryVersionCacheShard::AdvanceVersion(
    fuse_ino_t directory,
    const TString& name,
    ui64 version)
{
    with_lock (Lock) {
        auto it = Directories.find(directory);
        if (it == Directories.end()) {
            return;
        }

        auto& oldVersion = it->second.ChildVersions[name];
        if (oldVersion < version) {
            oldVersion = version;
        }
    }
}

ui64 TDirectoryEntryVersionCacheShard::GetVersion(
    fuse_ino_t directory,
    TStringBuf name) const
{
    with_lock (Lock) {
        auto it = Directories.find(directory);
        if (it == Directories.end()) {
            return 0;
        }

        auto versionIt = it->second.ChildVersions.find(name);
        return versionIt != it->second.ChildVersions.end()
                   ? versionIt->second
                   : 0;
    }
}

}   // namespace NCloud::NFileStore::NFuse
