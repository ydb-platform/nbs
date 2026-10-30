#pragma once

#include "public.h"

#include <library/cpp/cache/cache.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/utility.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

/*
 * One shard of a cache of the metadata returned by the backend along with a
 * TTL. The backend sends an invalidation event upon a change of the cached
 * data, the local changes invalidate the data directly.
 *
 * The data fetched by the requests started before an invalidation of the
 * same key is not cached - the invalidations are remembered as tombstones
 * with the cache version of the invalidation. The number of the tombstones is
 * limited: upon eviction of a tombstone its version raises the watermark
 * below which nothing is cached.
 */
template <typename TKey, typename TValue>
class TTTLCacheShard
{
private:
    struct TEntry
    {
        TValue Value;
        TInstant Deadline;
    };

    struct TTombstone
    {
        TKey Key;
        ui64 Version = 0;
    };

    TLRUCache<TKey, TEntry> Entries;
    THashMap<TKey, ui64> Tombstones;
    TDeque<TTombstone> TombstoneQueue;
    const ui32 MaxTombstoneCount;
    ui64 Watermark = 0;
    TAdaptiveLock Lock;

public:
    explicit TTTLCacheShard(ui32 maxEntryCount)
        : Entries(maxEntryCount)
        , MaxTombstoneCount(maxEntryCount)
    {}

    void Add(const TKey& key, TValue value, ui64 version, TInstant deadline)
    {
        auto g = Guard(Lock);

        if (version < Watermark) {
            return;
        }

        if (const ui64* tombstone = Tombstones.FindPtr(key);
            tombstone && version < *tombstone)
        {
            return;
        }

        Entries.Update(
            key,
            TEntry{.Value = std::move(value), .Deadline = deadline});
    }

    TMaybe<TValue> Get(const TKey& key, TInstant now)
    {
        auto g = Guard(Lock);

        auto it = Entries.Find(key);
        if (it == Entries.End()) {
            return Nothing();
        }

        if (now >= it.Value().Deadline) {
            Entries.Erase(it);
            return Nothing();
        }

        return it.Value().Value;
    }

    TMaybe<TValue> Invalidate(const TKey& key, ui64 version)
    {
        auto g = Guard(Lock);

        AddTombstone(key, version);

        auto it = Entries.Find(key);
        if (it == Entries.End()) {
            return Nothing();
        }

        TValue value = std::move(it.Value().Value);
        Entries.Erase(it);
        return value;
    }

    void InvalidateAll(ui64 version)
    {
        auto g = Guard(Lock);

        //
        // The cleared tombstones may have versions greater than version: the
        // versions are generated before taking the lock, so a concurrent
        // invalidation with a greater version may be applied first. Their
        // versions are kept in the watermark.
        //

        Watermark = Max(Watermark, version);
        for (const auto& [key, tombstoneVersion]: Tombstones) {
            Watermark = Max(Watermark, tombstoneVersion);
        }

        Entries.Clear();
        Tombstones.clear();
        TombstoneQueue.clear();
    }

    size_t GetEntryCount()
    {
        auto g = Guard(Lock);

        return Entries.Size();
    }

private:
    void AddTombstone(const TKey& key, ui64 version)
    {
        ui64& tombstoneVersion = Tombstones[key];
        tombstoneVersion = Max(tombstoneVersion, version);
        TombstoneQueue.push_back({.Key = key, .Version = version});

        //
        // The oldest tombstones are evicted first. A key may be present in
        // the queue several times - the map entry is erased only by its
        // latest queue entry. The evicted versions raise the watermark so that
        // the requests started before the evicted invalidations still don't
        // cache anything.
        //

        while (TombstoneQueue.size() > MaxTombstoneCount) {
            const auto& oldest = TombstoneQueue.front();
            Watermark = Max(Watermark, oldest.Version);
            auto it = Tombstones.find(oldest.Key);
            if (it != Tombstones.end() && it->second == oldest.Version) {
                Tombstones.erase(it);
            }
            TombstoneQueue.pop_front();
        }
    }
};

}   // namespace NCloud::NFileStore::NFuse
