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
    Completed = true;

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
}

bool TInFlightRequest::IsCompleted() const
{
    return Completed;
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
