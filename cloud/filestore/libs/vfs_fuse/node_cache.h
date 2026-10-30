#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/cache/cache.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/intrlist.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TNode
{
private:
    NProto::TNodeAttr Attrs;
    ui64 RefCount = 1;
    ui64 LastUpdateVersion = 1;

    //
    // The state of the backend-driven caching (see NodeCacheTTL in
    // GetNodeAttr responses): Attrs may be used for responding until
    // CacheDeadline. CacheVersion is the version at which the request that
    // has fetched Attrs has started. CacheInvalidationVersion is the version
    // of the last invalidation of the cached attrs - the attrs fetched by the
    // requests started before it are not cached.
    //

    TInstant CacheDeadline;
    ui64 CacheVersion = 0;
    ui64 CacheInvalidationVersion = 0;

private:
    explicit TNode(NProto::TNodeAttr attrs) noexcept
        : Attrs(std::move(attrs))
    {
        Y_ABORT_UNLESS(Attrs.GetId() != InvalidNodeId);
    }

public:
    const auto& GetAttrs() const
    {
        return Attrs;
    }

    bool IsValid() const
    {
        return Attrs.GetType() != NProto::E_INVALID_NODE;
    }

    ui64 GetVersion() const
    {
        return LastUpdateVersion;
    }

private:
    ui64 Ref()
    {
        return ++RefCount;
    }

    ui64 UnRef(ui64 count = 1)
    {
        // we lose actual RefCount values after restart, so we should expect
        // forget requests that try to subtract values greater than RefCount
        // see NBS-2102
        RefCount -= Min(RefCount, count);
        return RefCount;
    }

    void UpdateAttrs(const NProto::TNodeAttr& attrs, ui64 version)
    {
        Y_ABORT_UNLESS(Attrs.GetId() == attrs.GetId());
        Attrs.CopyFrom(attrs);
        LastUpdateVersion = version;
    }

    void ResetCache()
    {
        CacheDeadline = TInstant::Zero();
        CacheVersion = 0;
    }

    friend class TNodeCacheShard;
};

////////////////////////////////////////////////////////////////////////////////

/*
 * This cache is used to be able to detect races between the operations that
 * fetch node attrs from the backend and the operations that can modify those
 * attrs.
 *
 * When an attr-reading operation starts, it should get current fs "version".
 * Upon each attr-modifying operation completion we bump this global version and
 * invalidate the affected node with this version. If an attr-reading operation
 * finds out upon its completion that the node version in the node (or nodes
 * in case of ListNodes) is already higher than at the start of the operation,
 * it should tell the guest not to cache these attributes because they might
 * already be stale.
 *
 * Additionally, the attrs may be used for responding to the guest until the
 * deadline set via the TTL supplied by the backend (NodeCacheTTL in the
 * GetNodeAttr responses). The backend sends an invalidation event upon a
 * change of the node. The local changes invalidate the cached attrs via
 * InvalidateCachedAttrs. The node entries are retained after the deadline
 * passes - they are still needed for the race detection described above.
 */
class TNodeCacheShard
{
private:
    const TString FileSystemId;
    THashMap<ui64, TNode> Id2Node;
    TAdaptiveLock Lock;

public:
    explicit TNodeCacheShard(TString fileSystemId)
        : FileSystemId(std::move(fileSystemId))
    {}

    //
    // The version of the last invalidation of the cached attrs of the nodes
    // that were absent from Id2Node at that moment - such nodes may be
    // inserted by the requests started before the invalidation.
    //

    ui64 AbsentNodeInvalidationVersion = 0;

    //
    // The version of the last invalidation of all the cached attrs.
    //

    ui64 AllNodesInvalidationVersion = 0;

public:
    bool UpdateNode(
        const NProto::TNodeAttr& attrs,
        ui64 version,
        TInstant cacheDeadline);
    void InvalidateNode(ui64 ino, ui64 version);
    void ForgetNode(ui64 ino, size_t count);
    ui64 GetNodeVersion(ui64 ino) const;

    void UpdateCachedAttrs(
        const NProto::TNodeAttr& attrs,
        ui64 version,
        TInstant cacheDeadline);
    TMaybe<NProto::TNodeAttr> GetCachedAttrs(ui64 ino, TInstant now);
    TMaybe<NProto::TNodeAttr> RefCachedNode(ui64 ino, TInstant now);
    void InvalidateCachedAttrs(ui64 ino, ui64 version);
    void InvalidateAllCachedAttrs(ui64 version);

private:
    bool IsCacheable(const TNode& node, ui64 version) const;
    void SetCachedAttrs(TNode& node, ui64 version, TInstant cacheDeadline);
    bool IsCached(TNode& node, TInstant now);
};

class TNodeCache
{
private:
    TVector<TNodeCacheShard> Shards;

public:
    explicit TNodeCache(const TString& fileSystemId, ui32 shardCount = 1)
    {
        for (ui32 i = 0; i < shardCount; ++i) {
            Shards.emplace_back(fileSystemId);
        }
    }

public:
    /**
     * Updates the node attrs and increments the node ref count, see
     * TNodeCacheShard. Additionally makes the attrs usable for responding
     * until cacheDeadline if no invalidation of the node has happened after
     * version.
     *
     * @param attrs - Node attrs.
     * @param version - Cache version at which the request that has fetched
     *  the attrs has started.
     * @param cacheDeadline - Zero if the attrs are not cacheable.
     * @return - False if the node has a newer version.
     */
    bool UpdateNode(
        const NProto::TNodeAttr& attrs,
        ui64 version,
        TInstant cacheDeadline)
    {
        return Shards[attrs.GetId() % Shards.size()].UpdateNode(
            attrs,
            version,
            cacheDeadline);
    }

    void InvalidateNode(ui64 ino, ui64 version)
    {
        Shards[ino % Shards.size()].InvalidateNode(ino, version);
    }

    void ForgetNode(ui64 ino, size_t count)
    {
        Shards[ino % Shards.size()].ForgetNode(ino, count);
    }

    ui64 GetNodeVersion(ui64 ino) const
    {
        return Shards[ino % Shards.size()].GetNodeVersion(ino);
    }

    /**
     * Same as UpdateNode but doesn't change the ref count and doesn't insert
     * the absent nodes. Used for the responses that don't change the guest's
     * lookup count (GetAttr).
     *
     * @param attrs - Node attrs.
     * @param version - Cache version at which the request that has fetched
     *  the attrs has started.
     * @param cacheDeadline - Zero if the attrs are not cacheable.
     */
    void UpdateCachedAttrs(
        const NProto::TNodeAttr& attrs,
        ui64 version,
        TInstant cacheDeadline)
    {
        Shards[attrs.GetId() % Shards.size()].UpdateCachedAttrs(
            attrs,
            version,
            cacheDeadline);
    }

    /**
     * Returns the cached node attrs if they may be used for responding. If
     * the deadline has passed, the cached attrs are reset, the node itself is
     * retained.
     *
     * @param ino - Node id.
     * @param now - Current time.
     * @return - The attrs or Nothing.
     */
    TMaybe<NProto::TNodeAttr> GetCachedAttrs(ui64 ino, TInstant now)
    {
        return Shards[ino % Shards.size()].GetCachedAttrs(ino, now);
    }

    /**
     * Same as GetCachedAttrs but also increments the node ref count if the
     * attrs are returned - for the responses that increment the guest's
     * lookup count (Lookup).
     *
     * @param ino - Node id.
     * @param now - Current time.
     * @return - The attrs or Nothing.
     */
    TMaybe<NProto::TNodeAttr> RefCachedNode(ui64 ino, TInstant now)
    {
        return Shards[ino % Shards.size()].RefCachedNode(ino, now);
    }

    /**
     * Makes the cached attrs of the node unusable for responding, including
     * the attrs fetched by the requests started before version. Doesn't
     * affect the race detection state (LastUpdateVersion).
     *
     * @param ino - Node id.
     * @param version - Cache version of the invalidation.
     */
    void InvalidateCachedAttrs(ui64 ino, ui64 version)
    {
        Shards[ino % Shards.size()].InvalidateCachedAttrs(ino, version);
    }

    /**
     * Same as InvalidateCachedAttrs but for all the nodes.
     *
     * @param version - Cache version of the invalidation.
     */
    void InvalidateAllCachedAttrs(ui64 version)
    {
        for (auto& shard: Shards) {
            shard.InvalidateAllCachedAttrs(version);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TXAttr
{
    TString Name;
    TMaybe<TString> Value;
    ui64 Version;
    TInstant UpdateTime;
};

class TXAttrCache
{
private:
    struct TWeighter {
        static TInstant Weight(const TXAttr& value)
        {
            return value.UpdateTime;
        }
    };
    using TKey = std::pair<ui64, TString>;
    using TCache = TLWCache<TKey, TXAttr, TInstant, TWeighter>;

private:
    ITimerPtr Timer;
    TCache Cache;
    TDuration Timeout;

public:
    TXAttrCache(
            ITimerPtr timer,
            ui32 maxSize,
            TDuration timeout)
        : Timer{std::move(timer)}
        , Cache{maxSize}
        , Timeout{timeout}
    {}

    void Add(
        ui64 ino,
        const TString& name,
        const TString& value,
        ui64 version);
    void AddAbsent(ui64 ino, const TString& name);
    const TXAttr* Get(ui64 ino, const TString& name);
    void Invalidate(ui64 ino);
    void Forget(ui64 ino, const TString& name);
};

}   // namespace NCloud::NFileStore::NFuse
