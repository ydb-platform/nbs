#pragma once

#include "public.h"
#include "subsessions.h"

#include <cloud/filestore/libs/storage/tablet/protos/tablet.pb.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/hash_multi_map.h>
#include <util/generic/intrlist.h>
#include <util/string/cast.h>

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

using TSessionHandleList =
    TIntrusiveListWithAutoDelete<TSessionHandle, TDelete>;
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

struct TDupCacheEntry: NProto::TDupCacheEntry
{
    bool Committed = false;
    bool Dropped = false;

    TDupCacheEntry(const NProto::TDupCacheEntry& proto, bool committed)
        : NProto::TDupCacheEntry(proto)
        , Committed(committed)
    {}
};

using TDupCacheEntryList = TDeque<TDupCacheEntry>;
using TDupCacheEntryMap = THashMap<ui64, TDupCacheEntry*>;

////////////////////////////////////////////////////////////////////////////////

struct TMonSessionInfo
{
    TString ClientId;
    NProto::TSession ProtoInfo;
    TVector<TSubSession> SubSessions;
    TInstant Deadline;
};

////////////////////////////////////////////////////////////////////////////////

struct TSession
    : public TIntrusiveListItem<TSession>
    , public NProto::TSession
{
    // TODO: change visibility of the stuff below to private
    TSessionHandleList Handles;
    TSessionLockList Locks;

    TInstant Deadline;

    // TODO: notify event stream
    ui32 LastEvent = 0;
    bool NotifyEvents = false;

    TSubSessions SubSessions;

private:
    ui64 LastDupCacheEntryId = 1;
    TDupCacheEntryList DupCacheEntries;
    TDupCacheEntryMap DupCache;

public:
    explicit TSession(const NProto::TSession& proto)
        : NProto::TSession(proto)
        , SubSessions(GetMaxSeqNo(), GetMaxRwSeqNo())
    {}

    bool IsValid() const
    {
        return SubSessions.IsValid();
    }

    bool HasSeqNo(ui64 seqNo) const
    {
        return SubSessions.HasSeqNo(seqNo);
    }

    void UpdateSeqNo()
    {
        SetMaxSeqNo(SubSessions.GetMaxSeenSeqNo());
        SetMaxRwSeqNo(SubSessions.GetMaxSeenRwSeqNo());
    }

    NActors::TActorId UpdateSubSession(
        ui64 seqNo,
        bool readOnly,
        const NActors::TActorId& owner)
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

    ui64 GenerateDupCacheEntryId()
    {
        return ++LastDupCacheEntryId;
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

    void DropDupEntry(ui64 requestId)
    {
        if (!requestId) {
            return;
        }

        auto it = DupCache.find(requestId);
        if (it == DupCache.end()) {
            return;
        }

        it->second->Dropped = true;
        DupCache.erase(it);
    }

    TDupCacheEntry* AccessDupEntry(ui64 requestId)
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

    void AddDupCacheEntry(NProto::TDupCacheEntry proto, bool committed)
    {
        Y_ABORT_UNLESS(proto.GetRequestId());
        Y_ABORT_UNLESS(proto.GetEntryId());

        DupCacheEntries.emplace_back(std::move(proto), committed);

        auto& entry = DupCacheEntries.back();
        auto [_, inserted] = DupCache.emplace(entry.GetRequestId(), &entry);
        Y_ABORT_UNLESS(inserted);
    }

    void CommitDupCacheEntry(ui64 requestId)
    {
        if (auto it = DupCache.find(requestId); it != DupCache.end()) {
            it->second->Committed = true;
        }
    }

    ui64 PopDupCacheEntry(ui64 maxEntries)
    {
        if (DupCacheEntries.size() <= maxEntries) {
            return 0;
        }

        auto entry = DupCacheEntries.front();
        if (!entry.Dropped) {
            const auto erased = DupCache.erase(entry.GetRequestId());
            Y_DEBUG_ABORT_UNLESS(erased);
        }
        DupCacheEntries.pop_front();

        return entry.GetEntryId();
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

////////////////////////////////////////////////////////////////////////////////

struct TSessionHistoryEntry
    : public NProto::TSessionHistoryEntry
{
    enum EUpdateType
    {
        CREATE = 0,
        RESET = 1,
        DELETE = 2
    };

    explicit TSessionHistoryEntry(const NProto::TSessionHistoryEntry& proto)
        : NProto::TSessionHistoryEntry(proto)
    {}

    /**
     * Construct a new TSessionHistoryEntry object. Infers common fields
     * from a passed `proto` argument. Timestamp of an entry is set from the
     * current system time. Type of an entry is set from `type`. EntryId (key)
     * is set as a first unused integer.
     */

    TSessionHistoryEntry(
        const NProto::TSession& proto,
        ui64 entryId,
        EUpdateType type)
    {
        SetEntryId(entryId);
        SetClientId(proto.GetClientId());
        SetSessionId(proto.GetSessionId());
        SetOriginFqdn(proto.GetOriginFqdn());
        SetTimestampUs(Now().MicroSeconds());
        SetType(type);
    }

    TString GetEntryTypeString() const
    {
        return ToString(static_cast<EUpdateType>(GetType()));
    }
};

////////////////////////////////////////////////////////////////////////////////

using TSessionList = TIntrusiveListWithAutoDelete<TSession, TDelete>;
using TSessionMap = THashMap<TString, TSession*>;
using TSessionOwnerMap = THashMap<NActors::TActorId, TSession*>;
using TSessionClientMap = THashMap<TString, TSession*>;
using TSessionHistoryList = TDeque<TSessionHistoryEntry>;

struct TSessionsStats
{
    ui32 StatefulSessionsCount = 0;
    ui32 StatelessSessionsCount = 0;
    ui32 ActiveSessionsCount = 0;
    ui32 OrphanSessionsCount = 0;
};

}   // namespace NCloud::NFileStore::NStorage
