#pragma once

#include "public.h"
#include "subsessions.h"

#include <cloud/filestore/libs/storage/tablet/protos/tablet.pb.h>

#include <library/cpp/actors/core/actorid.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/hash_multi_map.h>
#include <util/generic/intrlist.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TSessionHandle
    : public TIntrusiveListItem<TSessionHandle>
    , public NProto::TSessionHandle
{
    TSession* const Session;

    TSessionHandle(TSession* session, const NProto::TSessionHandle& proto)
        : NProto::TSessionHandle(proto)
        , Session(session)
    {}
};

using TSessionHandleList = TIntrusiveListWithAutoDelete<TSessionHandle, TDelete>;
using TSessionHandleMap = THashMap<ui64, TSessionHandle*>;

using TNodeRefsByHandle = THashMap<ui64, ui64>;

////////////////////////////////////////////////////////////////////////////////

struct TSessionLock
    : public TIntrusiveListItem<TSessionLock>
    , public NProto::TSessionLock
{
    TSession* const Session;

    TSessionLock(TSession* session, const NProto::TSessionLock& proto)
        : NProto::TSessionLock(proto)
        , Session(session)
    {}
};

using TSessionLockList = TIntrusiveListWithAutoDelete<TSessionLock, TDelete>;
using TSessionLockMap = THashMap<ui64, TSessionLock*>;
using TSessionLockMultiMap = THashMultiMap<ui64, TSessionLock*>;

////////////////////////////////////////////////////////////////////////////////

struct TDupCacheEntry
    : public TIntrusiveListItem<TDupCacheEntry>
    , public NProto::TDupCacheEntry
{
    bool Commited = false;

    TDupCacheEntry(const NProto::TDupCacheEntry& proto, bool commited)
        : NProto::TDupCacheEntry(proto)
        , Commited(commited)
    {}
};

using TDupCacheEntryList = TIntrusiveListWithAutoDelete<TDupCacheEntry, TDelete>;
using TDupCacheEntryMap = THashMap<ui64, TDupCacheEntry*>;

////////////////////////////////////////////////////////////////////////////////

struct TSession
    : public TIntrusiveListItem<TSession>
    , public NProto::TSession
{
    TSessionHandleList Handles;
    TSessionLockList Locks;

    TInstant Deadline;

    // TODO: notify event stream
    ui32 LastEvent = 0;
    bool NotifyEvents = false;

    ui64 LastDupCacheEntryId = 1;
    TDupCacheEntryList DupCacheEntries;
    TDupCacheEntryMap DupCache;

    TSubSessions SubSessions;

public:
    TSession(const NProto::TSession& proto)
        : NProto::TSession(proto)
        , SubSessions(GetMaxSeqNo(), GetMaxRwSeqNo())
    {}

    bool IsValid() const
    {
        return SubSessions.IsValid();
    }

    bool HasSeqNo(ui64 seqNo)
    {
        return SubSessions.HasSeqNo(seqNo);
    }

    void UpdateSeqNo()
    {
        SetMaxSeqNo(SubSessions.GetMaxSeenSeqNo());
        SetMaxRwSeqNo(SubSessions.GetMaxSeenRwSeqNo());
    }

    NActors::TActorId UpdateSubSession(ui64 seqNo, bool readOnly, const NActors::TActorId& owner)
    {
        auto result = SubSessions.UpdateSubSession(seqNo, readOnly, owner);
        UpdateSeqNo();
        return result;
    }

    ui32 DeleteSubSession(const NActors::TActorId& owner)
    {
        auto result = SubSessions.DeleteSubSession(owner);
        UpdateSeqNo();
        return result;
    }

    ui32 DeleteSubSession(ui64 sessionSeqNo)
    {
        auto result = SubSessions.DeleteSubSession(sessionSeqNo);
        UpdateSeqNo();
        return result;
    }

    TVector<NActors::TActorId> GetSubSessions() const
    {
        return SubSessions.GetSubSessions();
    }

    void LoadDupCacheEntry(NProto::TDupCacheEntry entry)
    {
        LastDupCacheEntryId = Max(LastDupCacheEntryId, entry.GetEntryId());
        AddDupCacheEntry(std::move(entry), true);
    }

    const TDupCacheEntry* LookupDupEntry(ui64 requestId)
    {
        if (!requestId) {
            return nullptr;
        }

        auto it = DupCache.find(requestId);
        if (it != DupCache.end()) {
            return it->second;
        }

        return nullptr;
    }

    void AddDupCacheEntry(NProto::TDupCacheEntry proto, bool commited)
    {
        Y_ABORT_UNLESS(proto.GetRequestId());
        Y_ABORT_UNLESS(proto.GetEntryId());

        DupCacheEntries.PushBack(new TDupCacheEntry(std::move(proto), commited));

        auto entry = DupCacheEntries.Back();
        auto [_, inserted] = DupCache.emplace(entry->GetRequestId(), entry);
        Y_ABORT_UNLESS(inserted);
    }

    void CommitDupCacheEntry(ui64 requestId)
    {
        if (auto it = DupCache.find(requestId); it != DupCache.end()) {
            it->second->Commited = true;
        }
    }

    ui64 PopDupCacheEntry(ui64 maxEntries)
    {
        if (DupCacheEntries.Size() <= maxEntries) {
            return 0;
        }

        std::unique_ptr<TDupCacheEntry> entry(DupCacheEntries.PopFront());
        DupCache.erase(entry->GetRequestId());

        return entry->GetEntryId();
    }

    ui64 GetSessionSeqNo() const
    {
        return SubSessions.GetMaxSeenSeqNo();
    }

    ui64 GetSessionRwSeqNo() const
    {
        return SubSessions.GetMaxSeenRwSeqNo();
    }
};

using TSessionList = TIntrusiveListWithAutoDelete<TSession, TDelete>;
using TSessionMap = THashMap<TString, TSession*>;
using TSessionOwnerMap = THashMap<NActors::TActorId, TSession*>;
using TSessionClientMap = THashMap<TString, TSession*>;

}   // namespace NCloud::NFileStore::NStorage
