#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/cache/cache.h>

#include <util/generic/hash.h>
#include <util/generic/intrlist.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TNode
{
private:
    NProto::TNodeAttr Attrs;
    ui64 RefCount = 1;
    ui64 LastUpdateVersion = 1;

public:
    explicit TNode(const NProto::TNodeAttr& attrs) noexcept
        : Attrs(attrs)
    {
        Y_ABORT_UNLESS(attrs.GetId() != InvalidNodeId);
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

    friend class TNodeCache;
};

////////////////////////////////////////////////////////////////////////////////

class TNodeCache
{
private:
    const TString FileSystemId;
    THashMap<ui64, TNode> Id2Node;

public:
    explicit TNodeCache(TString fileSystemId)
        : FileSystemId(std::move(fileSystemId))
    {}

public:
    TNode* AddNode(const NProto::TNodeAttr& attrs);
    TNode* TryAddNode(const NProto::TNodeAttr& attrs, ui64 version);
    void InvalidateNode(ui64 ino, ui64 version);
    TNode* FindNode(ui64 ino);
    void ForgetNode(ui64 ino, size_t count);
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
    void Forget(ui64 ino, const TString& name);
};

}   // namespace NCloud::NFileStore::NFuse
