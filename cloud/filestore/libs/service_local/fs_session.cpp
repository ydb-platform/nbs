#include "fs.h"

#include <util/string/builder.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

NProto::TCreateSessionResponse TLocalFileSystem::CreateSession(
    const NProto::TCreateSessionRequest& request)
{
    STORAGE_INFO("CreateSession " << DumpMessage(request));

    const auto& clientId = GetClientId(request);
    const auto& sessionId = GetSessionId(request);
    const auto sessionSeqNo = request.GetMountSeqNumber();
    const auto readOnly = request.GetReadOnly();

    TWriteGuard guard(SessionsLock);

    auto session = FindSession(clientId, sessionId, sessionSeqNo);

    const auto makeResponse = [&sessionSeqNo, this](const TSessionPtr& session)
    {
        NProto::TCreateSessionResponse response;
        session->GetInfo(*response.MutableSession(), sessionSeqNo);

        *response.MutableFileStore() = Store;

        auto* features = response.MutableFileStore()->MutableFeatures();
        features->SetDirectIoEnabled(Config->GetDirectIoEnabled());
        features->SetDirectIoAlign(Config->GetDirectIoAlign());
        features->SetGuestWriteBackCacheEnabled(
            Config->GetGuestWriteBackCacheEnabled());
        features->SetAsyncDestroyHandleEnabled(
            Config->GetAsyncDestroyHandleEnabled());
        features->SetAsyncHandleOperationPeriod(
            Config->GetAsyncHandleOperationPeriod().MilliSeconds());
        features->SetZeroCopyEnabled(Config->GetZeroCopyEnabled());
        features->SetGuestPageCacheDisabled(Config->GetGuestPageCacheDisabled());
        features->SetExtendedAttributesDisabled(Config->GetExtendedAttributesDisabled());
        features->SetServerWriteBackCacheEnabled(
            Config->GetServerWriteBackCacheEnabled());
        features->SetMaxBackground(
            Config->GetMaxBackground());
        features->SetMaxFuseLoopThreads(
            Config->GetMaxFuseLoopThreads());
        features->SetZeroCopyWriteEnabled(Config->GetZeroCopyWriteEnabled());
        return response;
    };

    if (session) {
        if (session->ClientId != clientId) {
            return TErrorResponse(E_FS_INVALID_SESSION, TStringBuilder()
                << "cannot restore session: " << sessionId.Quote());
        }

        session->AddSubSession(sessionSeqNo, readOnly);

        return makeResponse(session);
    }

    if (sessionId) {
        return TErrorResponse(E_FS_INVALID_SESSION, TStringBuilder()
            << "invalid session: " << sessionId.Quote());
    }

    auto it = SessionsByClient.find(clientId);
    if (it != SessionsByClient.end()) {
        (*it->second)->AddSubSession(sessionSeqNo, readOnly);

        return makeResponse(*it->second);
    }

    auto clientSessionStatePath = StatePath / ("client_" + clientId);
    clientSessionStatePath.MkDir();

    session = std::make_shared<TSession>(
        Store.GetFileSystemId(),
        RootPath,
        clientSessionStatePath,
        clientId,
        Config->GetMaxNodeCount(),
        Config->GetMaxHandlePerSessionCount(),
        Config->GetOpenNodeByHandleEnabled(),
        Config->GetNodeCleanupBatchSize(),
        Logging);

    session->Init(request.GetRestoreClientSession());
    session->AddSubSession(sessionSeqNo, readOnly);

    SessionsList.push_front(session);

    auto [_, inserted1] =
        SessionsByClient.emplace(clientId, SessionsList.begin());
    Y_ABORT_UNLESS(inserted1);

    auto [dummyIt, inserted2] =
        SessionsById.emplace(session->SessionId, SessionsList.begin());
    Y_ABORT_UNLESS(inserted2);

    return makeResponse(session);
}

NProto::TPingSessionResponse TLocalFileSystem::PingSession(
    const NProto::TPingSessionRequest& request)
{
    TWriteGuard guard(SessionsLock);

    auto session = GetSession(request);

    auto sessionSeqNo = GetSessionSeqNo(request);
    session->Ping(sessionSeqNo);

    return {};
}

NProto::TDestroySessionResponse TLocalFileSystem::DestroySession(
    const NProto::TDestroySessionRequest& request)
{
    STORAGE_TRACE("DestroySession " << DumpMessage(request));

    const auto& sessionId = request.GetHeaders().GetSessionId();
    const auto sessionSeqNo = request.GetHeaders().GetSessionSeqNo();

    TWriteGuard guard(SessionsLock);

    RemoveSession(sessionId, sessionSeqNo);

    return {};
}

NProto::TResetSessionResponse TLocalFileSystem::ResetSession(
    const NProto::TResetSessionRequest& request)
{
    STORAGE_TRACE("ResetSession " << DumpMessage(request));

    TWriteGuard guard(SessionsLock);
    auto session = GetSession(request);
    session->ResetState(request.GetSessionState());

    return {};
}

////////////////////////////////////////////////////////////////////////////////

TSessionPtr TLocalFileSystem::GetSession(
    const TString& clientId,
    const TString& sessionId,
    ui64 seqNo)
{
    auto session = FindSession(clientId, sessionId, seqNo);
    Y_ENSURE_EX(session, TServiceError(E_FS_INVALID_SESSION)
        << "invalid session: " << sessionId.Quote());

    return session;
}

TSessionPtr TLocalFileSystem::FindSession(
    const TString& clientId,
    const TString& sessionId,
    ui64 seqNo)
{
    auto it = SessionsById.find(sessionId);
    if(it == SessionsById.end()) {
        return {};
    };

    TSessionPtr session = *it->second;
    if (session->ClientId != clientId || !session->HasSubSession(seqNo)) {
        return {};
    }

    return session;
}

void TLocalFileSystem::RemoveSession(
    const TString& sessionId,
    ui64 seqNo)
{
    auto it = SessionsById.find(sessionId);
    Y_ENSURE_EX(it != SessionsById.end(), TServiceError(E_FS_INVALID_SESSION)
        << "invalid session: " << sessionId.Quote());

    TSessionPtr session = *it->second;

    Y_ENSURE_EX(session->HasSubSession(seqNo), TServiceError(E_FS_INVALID_SESSION)
        << "invalid session: " << sessionId.Quote());

    if(!session->RemoveSubSession(seqNo)) {
        SessionsByClient.erase(session->ClientId);
        SessionsList.erase(it->second);
        SessionsById.erase(it);
    }

    session->StatePath.ForceDelete();
}

void TLocalFileSystem::ScheduleCleanupSessions()
{
    Scheduler->Schedule(
        Timer->Now() + TDuration::Seconds(1),  // TODO
        [weakPtr = weak_from_this()] () {
            if (auto self = weakPtr.lock()) {
                self->CleanupSessions();
            }
        });
}

void TLocalFileSystem::CleanupSessions()
{
    TWriteGuard guard(SessionsLock);

    auto deadline = TInstant::Now() - Config->GetIdleSessionTimeout();
    for (auto it = SessionsList.begin(); it != SessionsList.end();) {
        TSessionPtr session = *it;
        if (session->RemoveStaleSubSessions(deadline)) {
            SessionsById.erase(session->SessionId);
            SessionsByClient.erase(session->ClientId);
            it = SessionsList.erase(it);
        } else {
            ++it;
        }
    }

    ScheduleCleanupSessions();
}

}   // namespace NCloud::NFileStore
