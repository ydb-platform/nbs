#include "service_state.h"

#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/service/context.h>

#include <util/datetime/cputimer.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TInFlightRequest::Start(TInstant currentTs)
{
    RequestStats->RequestStarted(*CallContext);

    ProfileLogRequest.SetTimestampMcs(currentTs.MicroSeconds());
    ProfileLogRequest.SetRequestType(static_cast<ui32>(CallContext->RequestType));
}

void TInFlightRequest::Complete(
    TInstant currentTs,
    const NCloud::NProto::TError& error)
{
    RequestStats->RequestCompleted(*CallContext, error);

    ProfileLogRequest.SetDurationMcs(
        currentTs.MicroSeconds() - ProfileLogRequest.GetTimestampMcs());
    ProfileLogRequest.SetErrorCode(error.GetCode());

    if (ProfileLogRequest.HasLockInfo() ||
        ProfileLogRequest.HasNodeInfo() ||
        !ProfileLogRequest.GetRanges().empty())
    {
        ProfileLog->Write({CallContext->FileSystemId, std::move(ProfileLogRequest)});
    }

    //
    // Signalling request completion - after this line this request may be
    // deallocated and no one should touch it apart from the cleanup code
    // in service_actor_update_stats.cpp
    //

    Completed.store(true, std::memory_order_release);
}

bool TInFlightRequest::IsCompleted() const
{
    return Completed.load(std::memory_order_acquire);
}

TIncompleteRequest TInFlightRequest::ToIncompleteRequest(ui64 nowCycles) const
{
    const auto time = CallContext->CalcRequestTime(nowCycles);
    return TIncompleteRequest(
        MediaKind,
        CallContext->RequestType,
        time.ExecutionTime,
        time.TotalTime);
}

////////////////////////////////////////////////////////////////////////////////

TInFlightRequestStorage::TInFlightRequestStorage(IProfileLogPtr profileLog)
    : ProfileLog(std::move(profileLog))
{
}

TInFlightRequest* TInFlightRequestStorage::Register(
    NActors::TActorId sender,
    ui64 cookie,
    TCallContextPtr callContext,
    NProto::EStorageMediaKind mediaKind,
    TChecksumCalcInfo checksumCalcInfo,
    IRequestStatsPtr requestStats,
    TInstant start,
    ui64 key)
{
    auto g = Guard(Lock);

    auto [it, inserted] = Requests.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(key),
        std::forward_as_tuple(
            sender,
            cookie,
            std::move(callContext),
            ProfileLog,
            mediaKind,
            std::move(checksumCalcInfo),
            std::move(requestStats)));

    Y_ABORT_UNLESS(inserted);
    it->second.Start(start);

    it->second.ProfileLogRequest.SetLoopThreadId(
        it->second.CallContext->LoopThreadId);

    return &it->second;
}

TInFlightRequest* TInFlightRequestStorage::Find(ui64 key)
{
    auto g = Guard(Lock);

    return Requests.FindPtr(key);
}

void TInFlightRequestStorage::Erase(ui64 key)
{
    auto g = Guard(Lock);

    Requests.erase(key);
}

TVector<ui64> TInFlightRequestStorage::GetKeys() const
{
    auto g = Guard(Lock);

    TVector<ui64> keys;
    keys.reserve(Requests.size());
    for (const auto& [key, _]: Requests) {
        keys.push_back(key);
    }

    return keys;
}

////////////////////////////////////////////////////////////////////////////////

TSessionInfo* TStorageServiceState::CreateSession(
    TString clientId,
    NProto::TFileStore fileStore,
    TString sessionId,
    TString sessionState,
    ui64 seqNo,
    bool readOnly,
    NCloud::NProto::EStorageMediaKind mediaKind,
    IRequestStatsPtr requestStats,
    const TActorId& sessionActor,
    ui64 tabletId)
{
    TSessionInfo* sessionInfo;
    auto it = SessionById.find(sessionId);
    if (it == SessionById.end()) {
        auto session = std::make_unique<TSessionInfo>();
        session->ClientId = std::move(clientId);
        session->FileStore = std::move(fileStore);
        session->SessionId = std::move(sessionId);
        session->SessionState = std::move(sessionState);
        session->MediaKind = mediaKind,
        session->RequestStats = std::move(requestStats);
        session->SessionActor = sessionActor;
        session->TabletId = tabletId;

        Sessions.PushBack(session.get());
        SessionById.emplace(
            session->SessionId,
            session.get());

        sessionInfo = session.release();
    } else {
        sessionInfo = it->second;
    }

    sessionInfo->AddSubSession(seqNo, readOnly);

    return sessionInfo;
}

TSessionInfo* TStorageServiceState::FindSession(
    const TString& sessionId,
    ui64 seqNo) const
{
    auto it = SessionById.find(sessionId);
    if (it != SessionById.end()) {
        if (it->second->HasSubSession(seqNo)) {
            return it->second;
        }
    }
    return nullptr;
}

TSessionInfo* TStorageServiceState::FindSession(const TString& sessionId) const
{
    auto it = SessionById.find(sessionId);
    if (it != SessionById.end()) {
        return it->second;
    }
    return nullptr;
}

bool TStorageServiceState::RemoveSession(
    const TString& sessionId,
    ui64 seqNo)
{
    auto it = SessionById.find(sessionId);
    if (it != SessionById.end()) {
        auto* session = it->second;
        if (!session->RemoveSubSession(seqNo)) {
            std::unique_ptr<TSessionInfo> holder(session);
            SessionById.erase(session->SessionId);
            session->Unlink();
            return false;
        }
    }
    return true;
}

void TStorageServiceState::RemoveSession(const TString& sessionId)
{
    auto it = SessionById.find(sessionId);
    if (it != SessionById.end()) {
        auto* session = it->second;
        std::unique_ptr<TSessionInfo> holder(session);
        SessionById.erase(session->SessionId);
        session->Unlink();
    }
}

bool TStorageServiceState::IsLastSubSession(
    const TString& sessionId,
    ui64 seqNo)
{
    auto it = SessionById.find(sessionId);
    if (it == SessionById.end()) {
        return false;
    }
    auto* session = it->second;
    if (!it->second->HasSubSession(seqNo)) {
        return false;
    }
    return session->SubSessions.size() == 1;
}

void TStorageServiceState::VisitSessions(const TSessionVisitor& visitor) const
{
    for (const auto& session: Sessions) {
        visitor(session);
    }
}

void TStorageServiceState::RegisterLocalFileStore(
    const TString& id,
    ui64 tablet,
    ui32 generation,
    bool isShard,
    NProtoPrivate::TFileSystemConfig config)
{
    // in case new instance registered before old unregistered or config was updated
    if (auto it = LocalFileStores.find(id); it != LocalFileStores.end()) {
        if (generation < it->second.Generation) {
            return;
        }

        LocalFileStores.erase(it);
    }

    LocalFileStores.emplace(
        std::piecewise_construct,
        std::forward_as_tuple((id)),
        std::forward_as_tuple(
            id,
            tablet,
            generation,
            isShard,
            std::move(config)));
}

void TStorageServiceState::UnregisterLocalFileStore(
    const TString& id,
    ui32 generation)
{
    auto it = LocalFileStores.find(id);
    if (it != LocalFileStores.end()) {
        if (it->second.Generation == generation) {
            LocalFileStores.erase(it);
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
