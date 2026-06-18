#pragma once

#include "public.h"

#include "fuse.h"

#include <util/generic/hash.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

namespace NCloud::NFileStore::NFuse {

// Tracks per-name changes while a directory handle is open.
// ReadDir uses these versions to avoid returning cacheable entries when
// a listing races with unlink or rename of the same child.
//
// DirectoryHandleCache registers handles here, so when we don't have
// handles pointing to directory, we erase all information regarding
// entry changes to avoid unbound memory usage.
class TDirectoryEntryVersionCacheShard
{
private:
    struct TDirectoryState
    {
        ui64 RefCount = 0;
        THashMap<TString, ui64> ChildVersions;
    };

    mutable TAdaptiveLock Lock;
    THashMap<fuse_ino_t, TDirectoryState> Directories;

public:
    void RegisterHandle(fuse_ino_t directory);
    void UnregisterHandle(fuse_ino_t directory);
    void AdvanceVersion(fuse_ino_t directory, const TString& name, ui64 version);
    ui64 GetVersion(fuse_ino_t directory, TStringBuf name) const;
};

class TDirectoryEntryVersionCache
{
private:
    TVector<TDirectoryEntryVersionCacheShard> Shards;

public:
    TDirectoryEntryVersionCache()
        : TDirectoryEntryVersionCache(1)
    {}

    explicit TDirectoryEntryVersionCache(ui32 shardCount)
    {
        for (ui32 i = 0; i < shardCount; ++i) {
            Shards.emplace_back();
        }
    }

public:
    void RegisterHandle(fuse_ino_t directory)
    {
        Shards[directory % Shards.size()].RegisterHandle(directory);
    }

    void UnregisterHandle(fuse_ino_t directory)
    {
        Shards[directory % Shards.size()].UnregisterHandle(directory);
    }

    void AdvanceVersion(
        fuse_ino_t directory,
        const TString& name,
        ui64 version)
    {
        Shards[directory % Shards.size()].AdvanceVersion(
            directory,
            name,
            version);
    }

    ui64 GetVersion(fuse_ino_t directory, TStringBuf name) const
    {
        return Shards[directory % Shards.size()].GetVersion(directory, name);
    }
};

}   // namespace NCloud::NFileStore::NFuse
