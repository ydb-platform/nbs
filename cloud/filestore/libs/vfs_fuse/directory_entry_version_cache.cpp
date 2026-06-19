#include "directory_entry_version_cache.h"

namespace NCloud::NFileStore::NFuse {

TDirectoryEntryVersionCacheShard::~TDirectoryEntryVersionCacheShard()
{
    i64 removedCount = 0;

    for (const auto& [_, state]: Directories) {
        removedCount += static_cast<i64>(state.ChildVersions.size());
    }

    if (removedCount != 0) {
        Stats->ChangeEntryVersionCacheEntryCount(-removedCount);
    }
}

void TDirectoryEntryVersionCacheShard::RegisterHandle(fuse_ino_t directory)
{
    with_lock (Lock) {
        ++Directories[directory].RefCount;
    }
}

void TDirectoryEntryVersionCacheShard::UnregisterHandle(fuse_ino_t directory)
{
    i64 removedCount = 0;

    with_lock (Lock) {
        auto it = Directories.find(directory);
        if (it == Directories.end()) {
            return;
        }

        auto& state = it->second;
        if (--state.RefCount == 0) {
            removedCount = static_cast<i64>(state.ChildVersions.size());
            Directories.erase(it);
        }
    }

    if (removedCount != 0) {
        Stats->ChangeEntryVersionCacheEntryCount(-removedCount);
    }
}

void TDirectoryEntryVersionCacheShard::AdvanceVersion(
    fuse_ino_t directory,
    const TString& name,
    ui64 version)
{
    bool inserted = false;

    with_lock (Lock) {
        auto it = Directories.find(directory);
        if (it == Directories.end()) {
            return;
        }

        auto& childVersions = it->second.ChildVersions;
        auto [versionIt, newEntry] = childVersions.try_emplace(name, version);
        inserted = newEntry;

        if (versionIt->second < version) {
            versionIt->second = version;
        }
    }

    if (inserted) {
        Stats->ChangeEntryVersionCacheEntryCount(1);
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
