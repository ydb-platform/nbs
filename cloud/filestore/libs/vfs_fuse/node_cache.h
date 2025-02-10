#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/cache/cache.h>

#include <util/generic/hash_set.h>
#include <util/generic/intrlist.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

struct TNode
    : private TNonCopyable
{
    NProto::TNodeAttr Attrs;
    ui64 RefCount = 1;

    TNode(const NProto::TNodeAttr& attrs) noexcept
        : Attrs(attrs)
    {
        Y_ABORT_UNLESS(attrs.GetId() != InvalidNodeId);
    }

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

    void UpdateAttrs(const NProto::TNodeAttr& attrs)
    {
        Y_ABORT_UNLESS(Attrs.GetId() == attrs.GetId());
        Attrs.CopyFrom(attrs);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TNodeOps
{
    struct THash
    {
        template <typename T>
        size_t operator ()(const T& value) const
        {
            return IntHash(GetNodeId(value));
        }
    };

    struct TEqual
    {
        template <typename T1, typename T2>
        bool operator ()(const T1& l, const T2& r) const
        {
            return GetNodeId(l) == GetNodeId(r);
        }
    };

    static auto GetNodeId(const TNode& node)
    {
        return node.Attrs.GetId();
    }

    template <typename T>
    static auto GetNodeId(const T& value)
    {
        return value;
    }
};

using TNodeMap = THashSet<TNode, TNodeOps::THash, TNodeOps::TEqual>;

////////////////////////////////////////////////////////////////////////////////

class TNodeCache
{
private:
    TNodeMap Nodes;

    // TODO: keep track of session re-creation
    ui64 LastGen = 0;

public:
    ui64 Generation() const
    {
        return LastGen;
    }

    TNode* AddNode(const NProto::TNodeAttr& node);
    TNode* TryAddNode(const NProto::TNodeAttr& node);
    TNode* FindNode(ui64 ino);
    void ForgetNode(ui64 ino, size_t count);
};

////////////////////////////////////////////////////////////////////////////////

struct TXAttr {
    TString Name;
    TMaybe<TString> Value;
    ui64 Version;
    TInstant UpdateTime;
};

class TXAttrCache {
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
    void Forget(ui64 ino, const TString& name);
};

}   // namespace NCloud::NFileStore::NFuse
