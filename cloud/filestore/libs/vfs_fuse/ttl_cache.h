#pragma once

#include "public.h"

#include <library/cpp/cache/cache.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/utility.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

/*
 * Remembers the invalidations of the cached metadata so that the data fetched
 * by the requests started before an invalidation of the same key is not
 * cached. The invalidations are remembered as tombstones with the cache
 * version of the invalidation, one tombstone per key. The number of the
 * tombstones is limited: the least recently invalidated key is evicted first
 * and its version raises the watermark below which nothing is cached.
 *
 * Not thread-safe - the users protect it with their own locks.
 */
template <typename TKey>
class TInvalidations
{
private:
    // Tombstone versions by key, the least recently invalidated key first.
    TLRUCache<TKey, ui64> Tombstones;
    const ui32 MaxTombstoneCount;
    // Max version among the tombstones added since the last InvalidateAll.
    ui64 MaxTombstoneVersion = 0;
    ui64 Watermark = 0;

public:
    /**
     * @param maxTombstoneCount - Max number of the keys whose invalidations
     *  are remembered.
     */
    explicit TInvalidations(ui32 maxTombstoneCount)
        : Tombstones(static_cast<size_t>(maxTombstoneCount) + 1)
        , MaxTombstoneCount(maxTombstoneCount)
    {}

    /**
     * Checks whether the data fetched by a request may be cached.
     *
     * @param key - Key of the data.
     * @param version - Cache version at which the request has started.
     * @return - False if the key has been invalidated after version.
     */
    bool IsCacheable(const TKey& key, ui64 version) const
    {
        if (version < Watermark) {
            return false;
        }

        const auto it = Tombstones.FindWithoutPromote(key);
        return it == Tombstones.End() || version >= it.Value();
    }

    /**
     * Remembers an invalidation.
     *
     * @param key - Key of the invalidated data.
     * @param version - Cache version of the invalidation.
     */
    void Invalidate(const TKey& key, ui64 version)
    {
        MaxTombstoneVersion = Max(MaxTombstoneVersion, version);

        //
        // The tombstone of a key keeps the max version of its invalidations,
        // the invalidated key becomes the most recently invalidated one.
        //

        auto it = Tombstones.Find(key);
        if (it != Tombstones.End()) {
            *it = Max(*it, version);
        } else {
            const size_t size = Tombstones.Size();
            const bool inserted = Tombstones.Insert(key, version);
            Y_DEBUG_ABORT_UNLESS(inserted && Tombstones.Size() == size + 1);
        }

        //
        // The capacity of Tombstones is one more than MaxTombstoneCount, so
        // the overflow is handled here: the least recently invalidated key is
        // evicted and its version raises the watermark so that the requests
        // started before the evicted invalidation still don't cache
        // anything.
        //

        if (Tombstones.Size() > MaxTombstoneCount) {
            auto oldest = Tombstones.FindOldest();
            Watermark = Max(Watermark, oldest.Value());
            Tombstones.Erase(oldest);
        }
    }

    /**
     * Remembers an invalidation of all the keys.
     *
     * @param version - Cache version of the invalidation.
     */
    void InvalidateAll(ui64 version)
    {
        //
        // The cleared tombstones may have versions greater than version: the
        // versions are generated before taking the users' locks, so a
        // concurrent invalidation with a greater version may be applied
        // first. Their versions are kept in the watermark.
        //

        Watermark = Max(Watermark, version, MaxTombstoneVersion);

        Tombstones.Clear();
        MaxTombstoneVersion = 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

/*
 * One shard of a cache of the metadata returned by the backend along with a
 * TTL. The backend sends an invalidation event upon a change of the cached
 * data, the local changes invalidate the data directly. The data fetched by
 * the requests started before an invalidation of the same key is not cached,
 * see TInvalidations.
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

    TLRUCache<TKey, TEntry> Entries;
    TInvalidations<TKey> Invalidations;
    TAdaptiveLock Lock;

public:
    explicit TTTLCacheShard(ui32 maxEntryCount)
        : Entries(maxEntryCount)
        , Invalidations(maxEntryCount)
    {}

    void Add(const TKey& key, TValue value, ui64 version, TInstant deadline)
    {
        auto g = Guard(Lock);

        if (!Invalidations.IsCacheable(key, version)) {
            return;
        }

        Entries.Update(
            key,
            TEntry{.Value = std::move(value), .Deadline = deadline});
    }

    bool IsCacheable(const TKey& key, ui64 version)
    {
        auto g = Guard(Lock);

        return Invalidations.IsCacheable(key, version);
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

        Invalidations.Invalidate(key, version);

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

        Invalidations.InvalidateAll(version);
        Entries.Clear();
    }

    size_t GetEntryCount()
    {
        auto g = Guard(Lock);

        return Entries.Size();
    }
};

}   // namespace NCloud::NFileStore::NFuse
