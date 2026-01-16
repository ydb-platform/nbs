#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/libs/diagnostics/incomplete_requests.h>
#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/service/public.h>
#include <cloud/filestore/libs/storage/core/request_info.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/filestore/public/api/protos/session.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/protos/media.pb.h>

#include <contrib/ydb/library/actors/core/actorid.h>
#include <library/cpp/threading/atomic/bool.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/intrlist.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TChecksumCalcInfo
{
    ui32 BlockSize;
    bool BlockChecksumsEnabled;
    using TIovecs = google::protobuf::RepeatedPtrField<NProto::TIovec>;
    TIovecs Iovecs;

    TChecksumCalcInfo()
        : BlockSize(0)
        , BlockChecksumsEnabled(false)
    {}

    TChecksumCalcInfo(ui32 blockSize, TIovecs iovecs)
        : BlockSize(blockSize)
        , BlockChecksumsEnabled(true)
        , Iovecs(std::move(iovecs))
    {}
};

struct TInFlightRequest
    : public TRequestInfo
{
private:
    const NCloud::NProto::EStorageMediaKind MediaKind;
    const TChecksumCalcInfo ChecksumCalcInfo;
    const IRequestStatsPtr RequestStats;
    const IProfileLogPtr ProfileLog;

    NProto::TProfileLogRequestInfo ProfileLogRequest;
    std::atomic_bool Completed = false;

public:
    TInFlightRequest(
            NActors::TActorId sender,
            ui64 cookie,
            TCallContextPtr callContext,
            IProfileLogPtr profileLog,
            NCloud::NProto::EStorageMediaKind mediaKind,
            IRequestStatsPtr requestStats)
        : TInFlightRequest(
            sender,
            cookie,
            std::move(callContext),
            std::move(profileLog),
            mediaKind,
            {} /* checksumCalcInfo */,
            std::move(requestStats))
    {}

    TInFlightRequest(
            NActors::TActorId sender,
            ui64 cookie,
            TCallContextPtr callContext,
            IProfileLogPtr profileLog,
            NCloud::NProto::EStorageMediaKind mediaKind,
            TChecksumCalcInfo checksumCalcInfo,
            IRequestStatsPtr requestStats)
        : TRequestInfo(sender, cookie, std::move(callContext))
        , MediaKind(mediaKind)
        , ChecksumCalcInfo(std::move(checksumCalcInfo))
        , RequestStats(std::move(requestStats))
        , ProfileLog(std::move(profileLog))
    {}

    // deprecated
    TInFlightRequest(
            const TRequestInfo& info,
            IProfileLogPtr profileLog,
            NCloud::NProto::EStorageMediaKind mediaKind,
            IRequestStatsPtr requestStats)
        : TInFlightRequest(
            info.Sender,
            info.Cookie,
            info.CallContext,
            std::move(profileLog),
            mediaKind,
            {} /* checksumCalcInfo */,
            std::move(requestStats))
    {}

    // deprecated
    TInFlightRequest(
            const TRequestInfo& info,
            IProfileLogPtr profileLog,
            NCloud::NProto::EStorageMediaKind mediaKind,
            TChecksumCalcInfo checksumCalcInfo,
            IRequestStatsPtr requestStats)
        : TInFlightRequest(
            info.Sender,
            info.Cookie,
            info.CallContext,
            std::move(profileLog),
            mediaKind,
            std::move(checksumCalcInfo),
            std::move(requestStats))
    {}

    void Start(TInstant currentTs);
    void Complete(TInstant currentTs, const NCloud::NProto::TError& error);
    bool HasLogData() const;
    bool IsCompleted() const;

    const auto& GetProfileLogRequest() const
    {
        STORAGE_VERIFY_DEBUG(
            !IsCompleted(),
            CallContext->FileSystemId,
            TWellKnownEntityTypes::FILESYSTEM);
        return ProfileLogRequest;
    }

    auto& AccessProfileLogRequest()
    {
        STORAGE_VERIFY_DEBUG(
            !IsCompleted(),
            CallContext->FileSystemId,
            TWellKnownEntityTypes::FILESYSTEM);
        return ProfileLogRequest;
    }

    const TChecksumCalcInfo& GetChecksumCalcInfo() const
    {
        return ChecksumCalcInfo;
    }

    //
    // Thread-safe.
    //

    std::unique_ptr<TIncompleteRequest> ToIncompleteRequest(
        ui64 nowCycles) const;
};

////////////////////////////////////////////////////////////////////////////////

class TInFlightRequestStorage: public TAtomicRefCount<TInFlightRequestStorage>
{
private:
    IProfileLogPtr ProfileLog;
    THashMap<ui64, TInFlightRequest> Requests;
    mutable TAdaptiveLock Lock;
    std::atomic_uint64_t CompletedRequestCountWithLogData;
    std::atomic_uint64_t CompletedRequestCountWithError;
    std::atomic_uint64_t CompletedRequestCountWithoutErrorOrLogData;

    static constexpr auto StatsMemOrder = std::memory_order_relaxed;

public:
    explicit TInFlightRequestStorage(IProfileLogPtr profileLog);

public:
    class TLockedRequest
    {
    private:
        TAdaptiveLock* Lock;
        TInFlightRequest* Request;

    private:
        TLockedRequest(TAdaptiveLock& l, TInFlightRequest* r)
            : Lock(&l)
            , Request(r)
        {
        }

        friend class TInFlightRequestStorage;

    public:
        ~TLockedRequest()
        {
            if (Request) {
                Lock->unlock();
            }
        }

        TLockedRequest(const TLockedRequest& other) = delete;
        TLockedRequest& operator=(const TLockedRequest& other) = delete;

        TLockedRequest(TLockedRequest&& other) noexcept
            : Lock(other.Lock)
            , Request(other.Request)
        {
            other.Request = nullptr;
        }

        TLockedRequest& operator=(TLockedRequest&& other) noexcept
        {
            Lock = other.Lock;
            Request = other.Request;
            other.Request = nullptr;
            return *this;
        }

    public:
        TInFlightRequest* Get()
        {
            return Request;
        }
    };

    struct TStats
    {
        ui64 CompletedRequestCountWithLogData = 0;
        ui64 CompletedRequestCountWithError = 0;
        ui64 CompletedRequestCountWithoutErrorOrLogData = 0;
    };

public:
    TInFlightRequest* Register(
        NActors::TActorId sender,
        ui64 cookie,
        TCallContextPtr callContext,
        NProto::EStorageMediaKind mediaKind,
        TChecksumCalcInfo checksumCalcInfo,
        IRequestStatsPtr requestStats,
        TInstant start,
        ui64 key);

    TInFlightRequest* Find(ui64 key);

    TLockedRequest FindAndLock(ui64 key);

    void CompleteAndErase(
        TInstant currentTs,
        const NCloud::NProto::TError& error,
        TInFlightRequest& request,
        ui64 key);

    [[nodiscard]] TVector<ui64> GetKeys() const;

    TStats GetStats() const
    {
        return {
            .CompletedRequestCountWithLogData =
                CompletedRequestCountWithLogData.load(StatsMemOrder),
            .CompletedRequestCountWithError =
                CompletedRequestCountWithError.load(StatsMemOrder),
            .CompletedRequestCountWithoutErrorOrLogData =
                CompletedRequestCountWithoutErrorOrLogData.load(StatsMemOrder),
        };
    }
};

using TInFlightRequestStoragePtr = TIntrusivePtr<TInFlightRequestStorage>;

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

class TShardState: public TAtomicRefCount<TShardState>
{
private:
    std::atomic_bool IsOverloaded = false;

public:
    void SetIsOverloaded(bool isOverloaded)
    {
        IsOverloaded.store(isOverloaded, std::memory_order_relaxed);
    }

    bool GetIsOverloaded() const
    {
        return IsOverloaded.load(std::memory_order_relaxed);
    }
};

using TShardStatePtr = TIntrusivePtr<TShardState>;

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

    TVector<TShardStatePtr> ShardStates;
    TShardStatePtr MainTabletState;

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
        return SubSessions.count(seqNo) > 0;
    }

    void UpdateSubSession(ui64 seqNo, TInstant now)
    {
        if (auto it = SubSessions.find(seqNo); it != SubSessions.end()) {
            it->second.second = now;
        }
    }

    TShardStatePtr AccessShardState(ui32 shardIdx)
    {
        if (shardIdx >= FileStore.ShardFileSystemIdsSize()) {
            ReportInvalidShardIdx(TStringBuilder() << "ShardIdx: " << shardIdx
                << ", ShardCount: " << FileStore.ShardFileSystemIdsSize());
            return MakeIntrusive<TShardState>();
        }

        if (shardIdx >= ShardStates.size()) {
            ShardStates.reserve(shardIdx + 1);
            while (ShardStates.size() <= shardIdx) {
                ShardStates.emplace_back(MakeIntrusive<TShardState>());
            }
        }

        return ShardStates[shardIdx];
    }

    TShardStatePtr AccessMainTabletState()
    {
        if (!MainTabletState) {
            MainTabletState = MakeIntrusive<TShardState>();
        }

        return MainTabletState;
    }

    const TString& SelectShard()
    {
        const auto& shards = FileStore.GetShardFileSystemIds();
        if (shards.empty()) {
            return Default<TString>();
        }

        const ui32 shardIdx = ShardSelector++ % shards.size();
        return shards[static_cast<int>(shardIdx)];
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
