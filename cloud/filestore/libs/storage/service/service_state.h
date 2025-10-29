#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/libs/diagnostics/incomplete_requests.h>
#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/service/public.h>
#include <cloud/filestore/libs/storage/core/request_info.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/filestore/public/api/protos/session.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/media.pb.h>

#include <ydb/library/actors/core/actorid.h>
#include <library/cpp/threading/atomic/bool.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/intrlist.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TInFlightRequest
    : public TRequestInfo
{
public:
    NProto::TProfileLogRequestInfo ProfileLogRequest;

private:
    const NCloud::NProto::EStorageMediaKind MediaKind;
    const IRequestStatsPtr RequestStats;
    const IProfileLogPtr ProfileLog;

    NAtomic::TBool Completed = false;

public:
    TInFlightRequest(
            const TRequestInfo& info,
            IProfileLogPtr profileLog,
            NCloud::NProto::EStorageMediaKind mediaKind,
            IRequestStatsPtr requestStats)
        : TRequestInfo(info.Sender, info.Cookie, info.CallContext)
        , MediaKind(mediaKind)
        , RequestStats(std::move(requestStats))
        , ProfileLog(std::move(profileLog))
    {}

    void Start(TInstant currentTs);
    void Complete(TInstant currentTs, const NCloud::NProto::TError& error);
    bool IsCompleted() const;

    TIncompleteRequest ToIncompleteRequest(ui64 nowCycles) const;
};

////////////////////////////////////////////////////////////////////////////////

using TClientFileSystemKey = std::tuple<TString, TString>;

////////////////////////////////////////////////////////////////////////////////

enum class ESessionCreateDestroyState
{
    STATE_NONE,
    STATE_CREATE_SESSION,
    STATE_DESTROY_SESSION
};

////////////////////////////////////////////////////////////////////////////////

struct TSessionInfo
    : public TIntrusiveListItem<TSessionInfo>
{
    TString ClientId;
    NProto::TFileStore FileStore;
    TString SessionId;
    TString SessionState;
    NCloud::NProto::EStorageMediaKind MediaKind;
    IRequestStatsPtr RequestStats;

    NActors::TActorId SessionActor;
    ui64 TabletId = 0;

    THashMap<ui64, std::pair<bool, TInstant>> SubSessions;
    ESessionCreateDestroyState CreateDestroyState =
        ESessionCreateDestroyState::STATE_NONE;

    bool ShouldStop = false;

    ui32 ShardSelector = 0;

    void GetInfo(NProto::TSessionInfo& info, ui64 seqNo)
    {
        info.SetSessionId(SessionId);
        info.SetSessionState(SessionState);
        info.SetSessionSeqNo(seqNo);
        auto it = SubSessions.find(seqNo);
        if (it != SubSessions.end()) {
            info.SetReadOnly(it->second.first);
        }
    }

    void UpdateSessionState(TString state, NProto::TFileStore fileStore)
    {
        SessionState = std::move(state);
        FileStore = std::move(fileStore);
    }

    void AddSubSession(ui64 seqNo, bool readOnly)
    {
        SubSessions[seqNo] = std::make_pair(readOnly, TInstant::Now());
    }

    bool RemoveSubSession(ui64 seqNo)
    {
        SubSessions.erase(seqNo);
        return !SubSessions.empty();
    }

    bool HasSubSession(ui64 seqNo) const
    {
        return SubSessions.count(seqNo);
    }

    void UpdateSubSession(ui64 seqNo, TInstant now)
    {
        if (auto it = SubSessions.find(seqNo); it != SubSessions.end()) {
            it->second.second = now;
        }
    }

    const TString& SelectShard()
    {
        const auto& shards = FileStore.GetShardFileSystemIds();
        if (shards.empty()) {
            return Default<TString>();
        }

        return shards[ShardSelector++ % shards.size()];
    }
};

using TSessionList = TIntrusiveListWithAutoDelete<TSessionInfo, TDelete>;
using TSessionMap = THashMap<TString, TSessionInfo*>;
using TSessionVisitor = std::function<void(const TSessionInfo&)>;

////////////////////////////////////////////////////////////////////////////////

struct TLocalFileStore
{
    const TString FileStoreId;
    const ui64 TabletId;
    const ui32 Generation;
    const bool IsShard;

    NProtoPrivate::TFileSystemConfig Config;

    TLocalFileStore(
            TString id,
            ui64 tablet,
            ui32 generation,
            bool isShard,
            NProtoPrivate::TFileSystemConfig config)
        : FileStoreId(std::move(id))
        , TabletId(tablet)
        , Generation(generation)
        , IsShard(isShard)
        , Config(std::move(config))
    {}
};

using TLocalFileStoreMap = THashMap<TString, TLocalFileStore>;

////////////////////////////////////////////////////////////////////////////////

class TStorageServiceState
{
private:
    TSessionList Sessions;
    TSessionMap SessionById;

    TLocalFileStoreMap LocalFileStores;

public:
    TSessionInfo* CreateSession(
        TString clientId,
        NProto::TFileStore fileStore,
        TString sessionId,
        TString sessionState,
        ui64 seqNo,
        bool readOnly,
        NCloud::NProto::EStorageMediaKind mediaKind,
        IRequestStatsPtr requestStats,
        const NActors::TActorId& sessionActor,
        ui64 tabletId);

    TSessionInfo* FindSession(
        const TString& sessionId,
        ui64 sessionSeqNo) const;

    TSessionInfo* FindSession(const TString& sessionId) const;

    // removes session with sessionid and seqno
    // returns false if there are no more sessions
    // with given sessionid remain
    bool RemoveSession(
        const TString& sessionId,
        ui64 seqNo);

    void RemoveSession(const TString& sessionId);

    bool IsLastSubSession(
        const TString& sessionId,
        ui64 seqNo);

    void VisitSessions(const TSessionVisitor& visitor) const;

    void RegisterLocalFileStore(
        const TString& id,
        ui64 tablet,
        ui32 generation,
        bool isShard,
        NProtoPrivate::TFileSystemConfig config);
    void UnregisterLocalFileStore(
        const TString& id,
        ui32 generation);

    const TLocalFileStoreMap& GetLocalFileStores() const
    {
        return LocalFileStores;
    }
};

}   // namespace NCloud::NFileStore::NStorage
