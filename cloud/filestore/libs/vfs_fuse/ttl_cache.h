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

// Source of the data received from a backend which doesn't report the source
// (see NodeCacheSourceId and the like in the responses).
constexpr ui64 UnknownSourceId = 0;

////////////////////////////////////////////////////////////////////////////////

/*
 * Remembers the invalidations of the cached metadata so that the data fetched
 * by the requests started before an invalidation of the same key is not
 * cached. The invalidations are remembered as tombstones with the cache
 * version of the invalidation, one tombstone per key. The number of the
 * tombstones is limited: the least recently invalidated key is evicted first
 * and its version raises the watermark below which nothing is cached.
 *
 * The invalidations of all the data received from a source (e.g. upon the loss
 * of the connection to it) are remembered as the per-source watermarks below
 * which the data of the source is neither cached nor used.
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
    ui64 Watermark = 0;
    // Watermarks by source id, see InvalidateSource. The disconnects of all
    // the sources are delivered to all the filesystems, so this map gets an
    // entry for each source disconnected during the lifetime of the
    // process - even for the sources this cache has no data from. It is
    // bounded by the number of the sources (tablets) and never pruned: an
    // entry could be dropped only once no request started before its
    // watermark is in flight, which is not tracked.
    THashMap<ui64, ui64> SourceWatermarks;

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
     * Checks whether the data fetched from a source by a request is valid:
     * the source hasn't been invalidated after the request had started.
     *
     * @param sourceId - Source of the data.
     * @param version - Cache version at which the request has started.
     * @return - False if the source has been invalidated after version.
     */
    bool IsSourceValid(ui64 sourceId, ui64 version) const
    {
        const ui64* watermark = SourceWatermarks.FindPtr(sourceId);
        return !watermark || version >= *watermark;
    }

    /**
     * Remembers an invalidation of all the data received from a source. The
     * data received from an unknown source (UnknownSourceId) is invalidated
     * by the invalidation of any source.
     *
     * @param sourceId - Source of the invalidated data.
     * @param version - Cache version of the invalidation.
     */
    void InvalidateSource(ui64 sourceId, ui64 version)
    {
        for (const ui64 id: {sourceId, UnknownSourceId}) {
            ui64& watermark = SourceWatermarks[id];
            watermark = Max(watermark, version);
        }
    }

    /**
     * Remembers an invalidation.
     *
     * @param key - Key of the invalidated data.
     * @param version - Cache version of the invalidation.
     */
    void Invalidate(const TKey& key, ui64 version)
    {
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
};

////////////////////////////////////////////////////////////////////////////////

/*
 * A cache of the metadata returned by the backend along with a TTL. The
 * backend sends an invalidation event upon a change of the cached data, the
 * local changes invalidate the data directly. The data fetched by the
 * requests started before an invalidation of the same key is not cached, see
 * TInvalidations. Each entry remembers its source so that all the data of a
 * source can be invalidated at once, see InvalidateSource. The number of the
 * entries is limited: the least recently used entry is evicted first.
 *
 * Not thread-safe - see TTTLCacheShard.
 */
template <typename TKey, typename TValue>
class TTTLCache
{
public:
    struct TEntry
    {
        TValue Value;
        TInstant Deadline;
        ui64 SourceId = UnknownSourceId;
        // Cache version at which the request that has fetched the entry
        // started.
        ui64 Version = 0;
    };

private:
    TLRUCache<TKey, TEntry> Entries;
    TInvalidations<TKey> Invalidations;

public:
    /**
     * @param maxEntryCount - Max number of the cached entries (and of the
     *  remembered invalidations).
     */
    explicit TTTLCache(ui32 maxEntryCount)
        : Entries(maxEntryCount)
        , Invalidations(maxEntryCount)
    {}

    /**
     * Caches a value until deadline unless the key or the source has been
     * invalidated after version.
     *
     * @param key - Key of the data.
     * @param value - Value.
     * @param version - Cache version at which the request that has fetched
     *  the value has started.
     * @param sourceId - Source of the value.
     * @param deadline - Deadline after which the value is not used.
     */
    void Add(
        const TKey& key,
        TValue value,
        ui64 version,
        ui64 sourceId,
        TInstant deadline)
    {
        if (!Invalidations.IsCacheable(key, version) ||
            !Invalidations.IsSourceValid(sourceId, version))
        {
            return;
        }

        Entries.Update(
            key,
            TEntry{
                .Value = std::move(value),
                .Deadline = deadline,
                .SourceId = sourceId,
                .Version = version});
    }

    /**
     * Checks whether the data fetched by a request may be cached.
     *
     * @param key - Key of the data.
     * @param version - Cache version at which the request has started.
     * @return - False if the key has been invalidated after version.
     */
    bool IsCacheable(const TKey& key, ui64 version) const
    {
        return Invalidations.IsCacheable(key, version);
    }

    /**
     * Checks whether the data fetched from a source by a request is valid.
     *
     * @param sourceId - Source of the data.
     * @param version - Cache version at which the request has started.
     * @return - False if the source has been invalidated after version.
     */
    bool IsSourceValid(ui64 sourceId, ui64 version) const
    {
        return Invalidations.IsSourceValid(sourceId, version);
    }

    /**
     * Looks up an entry. The entry is erased if its deadline has passed or
     * its source has been invalidated.
     *
     * @param key - Key of the data.
     * @param now - Current time.
     * @return - The entry or nullptr. The pointer is valid until the next
     *  modification of the cache.
     */
    const TEntry* Get(const TKey& key, TInstant now)
    {
        auto it = Entries.Find(key);
        if (it == Entries.End()) {
            return nullptr;
        }

        const auto& entry = it.Value();
        if (now >= entry.Deadline ||
            !Invalidations.IsSourceValid(entry.SourceId, entry.Version))
        {
            Entries.Erase(it);
            return nullptr;
        }

        return &entry;
    }

    /**
     * Erases an entry and prevents caching the data fetched by the requests
     * started before version.
     *
     * @param key - Key of the data.
     * @param version - Cache version of the invalidation.
     * @return - The erased value or Nothing.
     */
    TMaybe<TValue> Invalidate(const TKey& key, ui64 version)
    {
        Invalidations.Invalidate(key, version);

        auto it = Entries.Find(key);
        if (it == Entries.End()) {
            return Nothing();
        }

        TValue value = std::move(it.Value().Value);
        Entries.Erase(it);
        return value;
    }

    /**
     * Prevents caching the data fetched by the requests started before
     * version. Unlike Invalidate, keeps the cached entry.
     *
     * @param key - Key of the data.
     * @param version - Cache version of the invalidation.
     */
    void AddTombstone(const TKey& key, ui64 version)
    {
        Invalidations.Invalidate(key, version);
    }

    /**
     * Erases an entry without remembering an invalidation.
     *
     * @param key - Key of the data.
     */
    void Erase(const TKey& key)
    {
        auto it = Entries.Find(key);
        if (it != Entries.End()) {
            Entries.Erase(it);
        }
    }

    /**
     * Invalidates all the entries received from a source. The entries are
     * erased lazily, upon access or eviction.
     *
     * @param sourceId - Source of the invalidated entries.
     * @param version - Cache version of the invalidation.
     */
    void InvalidateSource(ui64 sourceId, ui64 version)
    {
        Invalidations.InvalidateSource(sourceId, version);
    }

    /**
     * @return - Number of the cached entries.
     */
    size_t GetEntryCount() const
    {
        return Entries.Size();
    }
};

////////////////////////////////////////////////////////////////////////////////

/*
 * One shard of a TTTLCache protected by a lock.
 */
template <typename TKey, typename TValue>
class TTTLCacheShard
{
private:
    TTTLCache<TKey, TValue> Cache;
    TAdaptiveLock Lock;

public:
    explicit TTTLCacheShard(ui32 maxEntryCount)
        : Cache(maxEntryCount)
    {}

    void Add(
        const TKey& key,
        TValue value,
        ui64 version,
        ui64 sourceId,
        TInstant deadline)
    {
        auto g = Guard(Lock);

        Cache.Add(key, std::move(value), version, sourceId, deadline);
    }

    bool IsCacheable(const TKey& key, ui64 version)
    {
        auto g = Guard(Lock);

        return Cache.IsCacheable(key, version);
    }

    TMaybe<TValue> Get(const TKey& key, TInstant now)
    {
        auto g = Guard(Lock);

        const auto* entry = Cache.Get(key, now);
        if (!entry) {
            return Nothing();
        }

        return entry->Value;
    }

    TMaybe<TValue> Invalidate(const TKey& key, ui64 version)
    {
        auto g = Guard(Lock);

        return Cache.Invalidate(key, version);
    }

    /**
     * Checks whether the data fetched by a request may be cached and then
     * invalidates the key, atomically.
     *
     * @param key - Key of the data.
     * @param version - Cache version at which the request has started.
     * @param invalidationVersion - Cache version of the invalidation.
     * @return - False if the key has been invalidated after version.
     */
    bool CheckAndInvalidate(
        const TKey& key,
        ui64 version,
        ui64 invalidationVersion)
    {
        auto g = Guard(Lock);

        const bool cacheable = Cache.IsCacheable(key, version);

        //
        // The erased value is not needed by the callers.
        //

        Y_UNUSED(Cache.Invalidate(key, invalidationVersion));

        return cacheable;
    }

    /**
     * Invalidates all the entries received from a source. The entries are
     * erased lazily, upon access or eviction.
     *
     * @param sourceId - Source of the invalidated entries.
     * @param version - Cache version of the invalidation.
     */
    void InvalidateSource(ui64 sourceId, ui64 version)
    {
        auto g = Guard(Lock);

        Cache.InvalidateSource(sourceId, version);
    }

    size_t GetEntryCount()
    {
        auto g = Guard(Lock);

        return Cache.GetEntryCount();
    }
};

}   // namespace NCloud::NFileStore::NFuse
