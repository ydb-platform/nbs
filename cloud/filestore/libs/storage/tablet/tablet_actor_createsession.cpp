#include "tablet_actor.h"
#include "shard_request_actor.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <util/string/join.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

void FillFeatures(
    const NProto::TFileSystem& fileSystem,
    const NProto::TFileSystemStats& fileSystemStats,
    const TStorageConfig& config,
    NProto::TFileStore& fileStore)
{
    auto* features = fileStore.MutableFeatures();
    features->SetTwoStageReadEnabled(config.GetTwoStageReadEnabled());
    features->SetTwoStageReadThreshold(config.GetTwoStageReadThreshold());
    features->SetThreeStageWriteEnabled(config.GetThreeStageWriteEnabled());
    features->SetTwoStageReadDisabledForHDD(
        config.GetTwoStageReadDisabledForHDD());
    features->SetThreeStageWriteDisabledForHDD(
        config.GetThreeStageWriteDisabledForHDD());
    features->SetEntryTimeout(config.GetEntryTimeout().MilliSeconds());
    features->SetRegularFileEntryTimeout(
        config.GetRegularFileEntryTimeout().MilliSeconds());
    features->SetNegativeEntryTimeout(
        config.GetNegativeEntryTimeout().MilliSeconds());
    features->SetAttrTimeout(config.GetAttrTimeout().MilliSeconds());
    features->SetXAttrCacheTimeout(
        config.GetXAttrCacheTimeout().MilliSeconds());
    features->SetXAttrCacheLimit(config.GetXAttrCacheLimit());
    features->SetNodeRefCacheLimit(config.GetNodeRefCacheLimit());
    features->SetNodeAttrsCacheLimit(config.GetNodeAttrsCacheLimit());
    features->SetThreeStageWriteThreshold(config.GetThreeStageWriteThreshold());

    auto preferredBlockSizeMultiplier =
        config.GetPreferredBlockSizeMultiplier();
    if (preferredBlockSizeMultiplier) {
        features->SetPreferredBlockSize(
            fileStore.GetBlockSize() * preferredBlockSizeMultiplier);
    } else {
        features->SetPreferredBlockSize(fileStore.GetBlockSize());
    }
    features->SetAsyncDestroyHandleEnabled(
        config.GetAsyncDestroyHandleEnabled());
    features->SetAsyncDestroyReadOnlyHandleEnabled(
        config.GetAsyncDestroyReadOnlyHandleEnabled());
    features->SetAsyncCreateHandleEnabled(
        config.GetAsyncCreateHandleEnabled());
    features->SetAsyncHandleOperationIdlePeriod(
        config.GetAsyncHandleOperationIdlePeriod().MilliSeconds());
    features->SetAsyncHandleOperationDrainPeriod(
        config.GetAsyncHandleOperationDrainPeriod().MilliSeconds());
    features->SetAsyncHandleOperationBatchSize(
        config.GetAsyncHandleOperationBatchSize());

    features->SetGuestWriteBackCacheEnabled(
        config.GetGuestWriteBackCacheEnabled());

    features->SetGuestPageCacheDisabled(config.GetGuestPageCacheDisabled());
    features->SetExtendedAttributesDisabled(
        config.GetExtendedAttributesDisabled());

    features->SetServerWriteBackCacheEnabled(
        config.GetServerWriteBackCacheEnabled());
    features->SetServerWriteBackCacheStateFileSize(
        config.GetServerWriteBackCacheStateFileSize());
    features->SetServerWriteBackCacheFlushWritesInParallelEnabled(
        config.GetServerWriteBackCacheFlushWritesInParallelEnabled());

    features->SetParentlessFilesOnly(config.GetParentlessFilesOnly());
    features->SetAllowHandlelessIO(config.GetAllowHandlelessIO());

    features->SetDirectoryHandlesStorageEnabled(
        config.GetDirectoryHandlesStorageEnabled());

    if (config.GetDirectoryHandlesStorageEnabled()) {
        features->SetDirectoryHandlesTableSize(
            config.GetDirectoryHandlesTableSize());
        features->SetDirectoryHandlesPersistentHandleMaxSize(
            config.GetDirectoryHandlesPersistentHandleMaxSize());
    }

    features->SetDirectoryCreationInShardsEnabled(
        fileSystem.GetDirectoryCreationInShardsEnabled());

    // HasXAttrs is false only if LazyXAttrsEnabled == true and we know for sure that
    // there are no XAttrs in the filesystem
    const bool hasXAttrs =
        !config.GetLazyXAttrsEnabled() ||
        fileSystemStats.GetHasXAttrs() !=
            static_cast<ui64>(TIndexTabletActor::EHasXAttrs::False);
    features->SetHasXAttrs(hasXAttrs);

    features->SetMaxBackground(config.GetMaxBackground());
    features->SetMaxFuseLoopThreads(config.GetMaxFuseLoopThreads());
    features->SetMaxRequestSize(config.GetMaxRequestSize());

    features->SetZeroCopyWriteEnabled(config.GetZeroCopyWriteEnabled());

    features->SetFSyncQueueDisabled(config.GetFSyncQueueDisabled());

    features->SetGuestHandleKillPrivV2Enabled(
        config.GetGuestHandleKillPrivV2Enabled());

    features->SetTabletDirectRdmaEnabled(config.GetTabletDirectRdmaEnabled());

    features->SetGuestPosixAclEnabled(fileSystem.GetGuestPosixAclEnabled());

    features->SetZeroCopyReadEnabled(config.GetZeroCopyReadEnabled());

    features->SetBlockChecksumsInProfileLogEnabled(
        config.GetBlockChecksumsInProfileLogEnabled());

    features->SetReadBlobDisabled(config.GetReadBlobDisabled());
    features->SetWriteBlobDisabled(config.GetWriteBlobDisabled());

    features->SetUnconfirmedFlowEnabled(
        config.GetAddingUnconfirmedDataEnabled());

    features->SetUseListNodesInternal(config.GetUseListNodesInternal());

    features->SetUseCustomReadDataResponseParser(
        config.GetUseCustomReadDataResponseParser());

    features->SetAvailabilityTrackingEnabled(
        config.GetAvailabilityTrackingEnabled());
    features->SetAvailabilityTrackingInterval(
        config.GetAvailabilityTrackingInterval().MilliSeconds());

    features->SetStatFileStoreCacheTTL(
        config.GetStatFileStoreCacheTTL().MilliSeconds());

    features->SetExternalReadDataPayload(config.GetExternalReadDataPayload());
    features->SetExternalWriteDataPayloadEnabled(
        config.GetExternalWriteDataPayloadEnabled());
    features->SetEnableRdmaForWrites(config.GetEnableRdmaForWrites());
    features->SetWriteDataZeroBlockStatsEnabled(
        config.GetWriteDataZeroBlockStatsEnabled());
}

////////////////////////////////////////////////////////////////////////////////

TActorId DoRecoverSession(
    IIndexTabletDatabase& db,
    TIndexTabletState& state,
    TSession* session,
    const TString& clientId,
    const TString& sessionId,
    const TString& checkpointId,
    ui64 sessionSeqNo,
    bool readOnly,
    const TActorId& owner,
    const TActorContext& ctx)
{
    auto oldSessionSeqNo = session->GetSessionSeqNo();

    auto oldOwner =
        state.RecoverSession(session, sessionSeqNo, readOnly, owner);
    if (oldOwner) {
        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "[s:%s][n:%lu] kill from tablet %s self %s",
            sessionId.Quote().c_str(),
            sessionSeqNo,
            ToString(oldOwner).c_str(),
            ToString(ctx.SelfID).c_str());

        NCloud::Send(ctx, oldOwner, std::make_unique<TEvents::TEvPoisonPill>());
    }

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "DoRecoverSession c:%s, s:%s, session seqno:%lu new seqno:%lu",
        clientId.c_str(),
        session->GetSessionId().c_str(),
        session->GetSessionSeqNo(),
        sessionSeqNo);

    if (oldSessionSeqNo < session->GetSessionSeqNo()) {
        NProto::TSession proto;
        proto.SetClientId(clientId);
        proto.SetSessionId(sessionId);
        proto.SetCheckpointId(checkpointId);
        proto.SetSessionState(session->GetSessionState());
        proto.SetMaxSeqNo(session->GetSessionSeqNo());
        proto.SetMaxRwSeqNo(session->GetSessionRwSeqNo());
        db.WriteSession(proto);
    }

    return oldOwner;
}

void Convert(
    const NProto::TFileSystem& fileSystem,
    const ui64 nodesCount,
    NProto::TFileStore& fileStore)
{
    fileStore.SetFileSystemId(fileSystem.GetFileSystemId());
    fileStore.SetProjectId(fileSystem.GetProjectId());
    fileStore.SetFolderId(fileSystem.GetFolderId());
    fileStore.SetCloudId(fileSystem.GetCloudId());
    fileStore.SetBlockSize(fileSystem.GetBlockSize());
    fileStore.SetBlocksCount(fileSystem.GetBlocksCount());
    // TODO need set ConfigVersion?
    fileStore.SetNodesCount(nodesCount);
    fileStore.SetStorageMediaKind(fileSystem.GetStorageMediaKind());
    // TODO(#8034): switch to shard topology
    fileStore.MutableShardFileSystemIds()->CopyFrom(
        fileSystem.GetShardFileSystemIds());
    fileStore.SetShardNo(fileSystem.GetShardNo());
}

void ProcessAdapterModeFeatures(NProto::TFileStoreFeatures& f)
{
    //
    // Adapter mode doesn't support multiple-stage IO yet. Disabling
    // multiple-stage IO for the whole FS for simplicity if at least one shard
    // is configured in adapter mode.
    //

    f.SetTwoStageReadEnabled(false);
    f.SetThreeStageWriteEnabled(false);

    // Adapter mode doesn't support external read/write data payloads yet.
    // Disabling external read/write data payloads for the whole FS for
    // simplicity if at least one shard is configured in adapter mode.
    f.SetExternalReadDataPayload(false);
    f.SetExternalWriteDataPayloadEnabled(false);
}

////////////////////////////////////////////////////////////////////////////////

using TCreateShardSessionsActorBase = TShardRequestActor<
    TEvIndexTablet::TEvCreateSessionRequest,
    TEvIndexTablet::TEvCreateSessionResponse>;

class TCreateShardSessionsActor: public TCreateShardSessionsActorBase
{
public:
    using TCreateShardSessionsActorBase::TCreateShardSessionsActorBase;

protected:
    void OnResponse(const NProtoPrivate::TCreateSessionResponse& record)
        override
    {
        if (record.GetAdapterModeEnabled()
                && Response
                && Response->Record.HasFileStore())
        {
            auto* f = Response->Record.MutableFileStore()->MutableFeatures();
            ProcessAdapterModeFeatures(*f);
        }
    }
};

using TDestroyStaleShardSessionActor = TShardRequestActor<
    TEvIndexTablet::TEvDestroySessionRequest,
    TEvIndexTablet::TEvDestroySessionResponse>;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleCreateSession(
    const TEvIndexTablet::TEvCreateSessionRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s CreateSession: %s",
        LogTag.c_str(),
        ProtoMessagePrinter.ToString(msg->Record).c_str());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    // TODO(#6310): Replace with a proper check. We need to test whether
    // filesystem create operation has completed, expected-shard-count
    // heuristic is too fragile.
    const auto expectedShardCount =
        CalculateMinExpectedShardCount(Config->GetMaxShardCount());
    const auto actualShardCount = GetFileSystem().ShardFileSystemIdsSize();
    if (actualShardCount < expectedShardCount) {
        auto message = TStringBuilder() << "Shard count smaller than expected: "
            << actualShardCount << " < " << expectedShardCount;
        const bool shouldReject =
            Config->GetEnforceCorrectFileSystemShardCountUponSessionCreation();
        if (shouldReject) {
            LOG_INFO(ctx, TFileStoreComponents::TABLET,
                "%s CreateSession rejected: %s",
                LogTag.c_str(),
                message.c_str());

            using TResponse = TEvIndexTablet::TEvCreateSessionResponse;
            auto response = std::make_unique<TResponse>(
                MakeError(E_REJECTED, std::move(message)));
            NCloud::Reply(ctx, *requestInfo, std::move(response));
            return;
        }

        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s CreateSession: %s",
            LogTag.c_str(),
            message.c_str());
    }

    AddInFlightRequest<TEvIndexTablet::TCreateSessionMethod>(*requestInfo);

    ExecuteTx<TCreateSession>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_CreateSession(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCreateSession& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_CreateSession(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCreateSession& args)
{
    const auto& clientId = GetClientId(args.Request);
    const auto& sessionId = GetSessionId(args.Request);
    const auto& checkpointId = args.Request.GetCheckpointId();
    const auto& originFqdn = GetOriginFqdn(args.Request);
    const auto seqNo = args.Request.GetMountSeqNumber();
    const auto readOnly = args.Request.GetReadOnly();

    const auto owner = args.RequestInfo->Sender;

    auto db = CreateIndexTabletDatabase(tx.DB);

    //
    // ResetSession or DestroySession for the session is being delivered to
    // the shards (see PendingShardSessionRequests). The shards make their
    // decisions against the current subsessions of the session and this
    // tablet is going to make the same decision after them, so the session
    // can't get a new subsession until then - the client should retry.
    //
    // CreateSession which is waiting for the shards is not an obstacle: this
    // request takes its place, see CompleteTx_CreateSession.
    //

    auto isBeingChanged = [&] (const TString& id) {
        const auto* request = PendingShardSessionRequests.FindPtr(id);
        if (!request || request->Type == EShardSessionRequestType::Create) {
            return false;
        }

        args.Error = MakeError(E_REJECTED, TStringBuilder()
            << "session " << id.Quote() << " is being reset or destroyed");
        return true;
    };

    if (isBeingChanged(sessionId)) {
        return;
    }

    // check if client reconnecting with known session id
    auto* session = FindSession(sessionId);
    if (session) {
        if (session->GetClientId() == clientId) {
            args.SessionId = session->GetSessionId();
            // the session the way the shards know it, see
            // CompleteTx_CreateSession
            const auto shardSessionInfo =
                BuildShardSessionInfo(args.SessionId);
            auto toKill = DoRecoverSession(
                *db,
                *this,
                session,
                clientId,
                args.SessionId,
                checkpointId,
                seqNo,
                readOnly,
                owner,
                ctx);
            args.SessionInterrupted = true;
            args.ShardSessionChanged = !shardSessionInfo
                || !IsShardSessionInfoActual(*shardSessionInfo);
            if (toKill != owner) {
                const auto subSession =
                    session->SubSessions.GetSubSessionBySeqNo(seqNo);
                if (subSession) {
                    args.OwnerGeneration = subSession->OwnerGeneration;
                } else {
                    LOG_ERROR(ctx, TFileStoreComponents::TABLET,
                        "%s CreateSession c:%s, s:%s, seqno:%lu recovered "
                        "by session, but subsession was not found",
                        LogTag.c_str(),
                        clientId.c_str(),
                        session->GetSessionId().c_str(),
                        seqNo);
                }
                LOG_INFO(ctx, TFileStoreComponents::TABLET,
                    "%s CreateSession c:%s, s:%s, seqno:%lu recovered by session",
                    LogTag.c_str(),
                    clientId.c_str(),
                    session->GetSessionId().c_str(),
                    seqNo);
            } else {
                args.Error = MakeError(E_INVALID_STATE, "session seqno is too old");
                LOG_ERROR(ctx, TFileStoreComponents::TABLET,
                    "%s CreateSession c:%s, s:%s, seqno:%lu failed to restore session: %s",
                    LogTag.c_str(),
                    clientId.c_str(),
                    args.SessionId.c_str(),
                    seqNo,
                    FormatError(args.Error).c_str());
            }
        } else {
            args.Error = MakeError(E_INVALID_STATE, "session client id mismatch");
        }
        return;
    }

    // check if there is existing session for the client
    if (args.Request.GetRestoreClientSession()) {
        auto* session = FindSessionByClientId(clientId);
        if (session && isBeingChanged(session->GetSessionId())) {
            return;
        }

        if (session) {
            LOG_INFO(ctx, TFileStoreComponents::TABLET,
                "%s CreateSession c:%s, s:%s recovered by client",
                LogTag.c_str(),
                clientId.c_str(),
                session->GetSessionId().c_str());

            args.SessionId = session->GetSessionId();

            // the session the way the shards know it, see
            // CompleteTx_CreateSession
            const auto shardSessionInfo =
                BuildShardSessionInfo(args.SessionId);

            DoRecoverSession(
                *db,
                *this,
                session,
                clientId,
                args.SessionId,
                checkpointId,
                seqNo,
                readOnly,
                owner,
                ctx);
            args.ShardSessionChanged = !shardSessionInfo
                || !IsShardSessionInfoActual(*shardSessionInfo);
            const auto subSession =
                session->SubSessions.GetSubSessionBySeqNo(seqNo);
            if (subSession) {
                args.OwnerGeneration = subSession->OwnerGeneration;
            } else {
                LOG_ERROR(ctx, TFileStoreComponents::TABLET,
                    "%s CreateSession c:%s, s:%s, seqno:%lu recovered by "
                    "client, but subsession was not found",
                    LogTag.c_str(),
                    clientId.c_str(),
                    session->GetSessionId().c_str(),
                    seqNo);
            }
            args.SessionInterrupted = true;

            return;
        }

        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s CreateSession: no session available for client c: %s",
            LogTag.c_str(),
            clientId.c_str());
    }

    if (!sessionId) {
        args.Error = MakeError(E_ARGUMENT, "empty session id");
        return;
    }

    args.SessionId = sessionId;
    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s CreateSession c:%s, s:%s, n:%lu creating new session",
        LogTag.c_str(),
        clientId.c_str(),
        args.SessionId.c_str(),
        seqNo);

    const auto sessionOptions = TSession::CreateSessionOptions(Config);

    auto* newSession = CreateSession(
        *db,
        clientId,
        args.SessionId,
        checkpointId,
        originFqdn,
        seqNo,
        readOnly,
        owner,
        sessionOptions);
    const auto subSession =
        newSession->SubSessions.GetSubSessionBySeqNo(seqNo);
    if (subSession) {
        args.OwnerGeneration = subSession->OwnerGeneration;
    } else {
        LOG_ERROR(ctx, TFileStoreComponents::TABLET,
            "%s CreateSession c:%s, s:%s, seqno:%lu created new session, "
            "but subsession was not found",
            LogTag.c_str(),
            clientId.c_str(),
            newSession->GetSessionId().c_str(),
            seqNo);
    }
}

void TIndexTabletActor::CompleteTx_CreateSession(
    const TActorContext& ctx,
    TTxIndexTablet::TCreateSession& args)
{
    RemoveInFlightRequest(*args.RequestInfo);

    if (args.SessionInterrupted) {
        DeleteUnconfirmedDataForSession(args.SessionId, ctx);
    }

    using TResponse = TEvIndexTablet::TEvCreateSessionResponse;

    if (HasError(args.Error)) {
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s CreateSession failed (%s)",
            LogTag.c_str(),
            FormatError(args.Error).c_str());

        auto response = std::make_unique<TResponse>(std::move(args.Error));
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    auto* session = FindSession(args.SessionId);
    if (!session) {
        auto message = TStringBuilder() << "Session " << args.SessionId
            << " destroyed during creation";
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s %s",
            LogTag.c_str(),
            message.c_str());

        auto response =
            std::make_unique<TResponse>(MakeError(E_REJECTED, message));
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    auto response = std::make_unique<TResponse>(args.Error);
    response->Record.SetSessionId(std::move(args.SessionId));
    response->Record.SetSessionState(session->GetSessionState());
    response->Record.SetOwnerGeneration(args.OwnerGeneration);
    if (!args.Request.GetNoFileStoreInfo()) {
        auto& fileStore = *response->Record.MutableFileStore();
        Convert(GetFileSystem(), GetNodesCount(), fileStore);
        FillFeatures(
            GetFileSystem(),
            GetFileSystemStats(),
            *Config,
            fileStore);

        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET,
            "%s New session TFileStoreFeatures '%s'",
            LogTag.c_str(),
            fileStore.GetFeatures().ShortDebugString().c_str());
    }

    if (GetFileSystem().GetIsFastShard()) {
        response->Record.SetAdapterModeEnabled(true);
        if (!args.Request.GetNoFileStoreInfo()) {
            auto* f = response->Record.MutableFileStore()->MutableFeatures();
            ProcessAdapterModeFeatures(*f);
        }
    }

    TVector<TString> shardIds;
    // there's no point in returning shard list unless it's main filesystem
    // tablet (in which case shard list is needed to perform request forwarding
    // to shards in TStorageServiceActor)
    if (IsMainTablet()) {
        for (const auto& shardId: GetFileSystem().GetShardFileSystemIds()) {
            shardIds.push_back(shardId);
        }
    }
    if (shardIds.empty()) {
        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s CreateSession completed (%s)",
            LogTag.c_str(),
            FormatError(args.Error).c_str());

        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    if (ShouldSyncShardSessionRequests()) {
        //
        // Sending a request to each of the shards upon each CreateSession
        // request doesn't scale: there may be thousands of shards and
        // thousands of clients which connect simultaneously. The session is
        // delivered to the shards by the shard sessions sync instead: a
        // round sends a single request per shard with the sessions of all
        // the clients which are waiting.
        //
        // The client still gets the response only after all the shards have
        // got the session - the same way as if this tablet sent the request
        // to each of them. That's what the client relies upon:
        // * the session can be used in any shard right away;
        // * if the session has been recovered, i.e. the client has started
        //   anew, then no shard keeps anything of its previous incarnation,
        //   see TShardSessionInfo::Interrupted. Otherwise an unconfirmed
        //   write of the previous incarnation might be applied (upon a
        //   restart of a shard) over the data which the new one writes
        //   later.
        //
        // The shards which work in adapter mode are the exception: they
        // can't process the requests of the sync. They get the request from
        // this tablet after the rest of the shards, see
        // CompletePendingCreateSession.
        //

        // the id is about to be moved together with the request
        const TString sessionId = session->GetSessionId();
        // see TPendingShardSessionRequest::RequestId
        const ui64 requestId = args.Request.GetHeaders().GetRequestId();

        bool interrupted = args.SessionInterrupted;
        // The version of the request if it shouldn't get a new one: the
        // shards which have been synced at this version (or later) have the
        // session the way this request needs it.
        ui64 version = 0;
        bool retryOfPendingRequest = false;

        auto it = PendingShardSessionRequests.find(sessionId);
        if (it != PendingShardSessionRequests.end()) {
            auto& pending = it->second;

            if (pending.Type != EShardSessionRequestType::Create) {
                // ResetSession or DestroySession managed to get here while
                // the tx of this request was in progress
                auto error = MakeError(E_REJECTED, TStringBuilder()
                    << "session " << sessionId.Quote()
                    << " is being reset or destroyed");

                LOG_WARN(ctx, TFileStoreComponents::TABLET,
                    "%s CreateSession failed (%s)",
                    LogTag.c_str(),
                    FormatError(error).c_str());

                NCloud::Reply(
                    ctx,
                    *args.RequestInfo,
                    std::make_unique<TResponse>(std::move(error)));
                return;
            }

            //
            // One more CreateSession for this session is waiting for the
            // shards. The session is the way this request has made it now,
            // so this request takes the place of that one.
            //
            // It may be a retry of the very same request: the client hasn't
            // got the response in time or has reconnected to this tablet.
            // Such a request doesn't start over. It's still the same
            // incarnation of the client, so it needs just what its previous
            // attempt needed: if this attempt hasn't changed anything for
            // the shards, then the version stays the same and the shards
            // which have got the session are not waited for once again.
            //

            const bool retry = requestId && pending.RequestId == requestId;

            ReplyToReplacedShardSessionRequest(
                ctx,
                pending,
                *args.RequestInfo,
                MakeError(
                    E_REJECTED,
                    retry
                        ? "replaced by a retry of the same request"
                        : "replaced by another CreateSession request"));

            if (retry) {
                retryOfPendingRequest = true;
                interrupted = pending.Interrupted;
                if (!args.ShardSessionChanged) {
                    version = pending.Version;
                }
            } else {
                // some of the shards might have not got the request which is
                // being replaced
                interrupted = interrupted || pending.Interrupted;
            }

            PendingShardSessionRequests.erase(it);
        } else if (requestId
                && session->CompletedCreateRequestId == requestId)
        {
            //
            // A retry of a request which has been completed: the shards
            // have got the session and have dropped what the previous
            // incarnation of the client had left (see
            // TShardSessionInfo::Interrupted) - it happened after this
            // incarnation had sent the request for the first time. There's
            // nothing to do unless something has changed since then.
            //

            interrupted = false;
            if (!args.ShardSessionChanged) {
                version = session->ShardSyncVersion;
            }
        }

        TPendingShardSessionRequest request;
        request.Type = EShardSessionRequestType::Create;
        request.RequestId = requestId;
        request.SessionSeqNo = args.Request.GetMountSeqNumber();
        request.Interrupted = interrupted;
        request.RequestInfo = std::move(args.RequestInfo);
        request.CreateRequest = std::move(args.Request);
        request.CreateResponse = std::move(response);

        if (!version) {
            EnqueueShardSessionRequest(ctx, sessionId, std::move(request));
            return;
        }

        request.Version = version;

        if (version <= GetShardSessionsSyncedVersion()) {
            LOG_INFO(ctx, TFileStoreComponents::TABLET,
                "%s CreateSession s:%s n:%lu is a retry of request %lu"
                ", the shards have been synced at version %lu already",
                LogTag.c_str(),
                sessionId.Quote().c_str(),
                request.SessionSeqNo,
                requestId,
                version);

            CompletePendingCreateSession(ctx, sessionId, std::move(request));
            return;
        }

        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s CreateSession s:%s n:%lu is a retry of request %lu"
            ", it keeps waiting for the shard sessions sync, version: %lu",
            LogTag.c_str(),
            sessionId.Quote().c_str(),
            request.SessionSeqNo,
            requestId,
            version);

        PendingShardSessionRequests[sessionId] = std::move(request);

        if (retryOfPendingRequest) {
            // The rounds go on while there are pending requests, see
            // HandleSyncSessionsCompleted: the retry shouldn't make them
            // more frequent. That's just to be on the safe side - it does
            // nothing if a round is in progress or is scheduled.
            ScheduleOutOfScheduleSyncSessions(ctx);
        } else {
            // the shards which are behind might be waiting for a round
            // which nobody is in a hurry to start
            ScheduleUrgentSyncSessions(ctx);
        }
        return;
    }

    if (Config->GetLazyShardSessionCreationEnabled()
            && response->Record.HasFileStore())
    {
        // All the shards work in adapter mode: their sessions are managed
        // by the per-session requests.
        auto* f = response->Record.MutableFileStore()->MutableFeatures();
        ProcessAdapterModeFeatures(*f);
    }

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s CreateSession completed - local (%s)",
        LogTag.c_str(),
        FormatError(args.Error).c_str());

    CreateSessionsInShards(
        ctx,
        std::move(args.RequestInfo),
        std::move(args.Request),
        std::move(response),
        std::move(shardIds));
}

void TIndexTabletActor::CompletePendingCreateSession(
    const TActorContext& ctx,
    const TString& sessionId,
    TPendingShardSessionRequest request)
{
    using TResponse = TEvIndexTablet::TEvCreateSessionResponse;

    auto* session = FindSession(sessionId);
    if (!session) {
        auto message = TStringBuilder() << "Session " << sessionId
            << " destroyed during creation";
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s %s",
            LogTag.c_str(),
            message.c_str());

        auto response =
            std::make_unique<TResponse>(MakeError(E_REJECTED, message));
        NCloud::Reply(ctx, *request.RequestInfo, std::move(response));
        return;
    }

    // a retry of this request doesn't have to wait for these shards once
    // again, see CompleteTx_CreateSession
    session->CompletedCreateRequestId = request.RequestId;

    auto response = std::move(request.CreateResponse);

    // The sync might have found out that some of the shards work in adapter
    // mode (see HandleBuildShardSessionsDiff) while the request was waiting.
    auto shardIds = GetAdapterModeShardIds();
    if (shardIds.empty()) {
        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s CreateSession completed, the shards have got session s:%s"
            ", version: %lu",
            LogTag.c_str(),
            sessionId.Quote().c_str(),
            request.Version);

        NCloud::Reply(ctx, *request.RequestInfo, std::move(response));
        return;
    }

    if (response->Record.HasFileStore()) {
        auto* f = response->Record.MutableFileStore()->MutableFeatures();
        ProcessAdapterModeFeatures(*f);
    }

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s CreateSession: the shards have got session s:%s, version: %lu"
        ", the shards which work in adapter mode are left",
        LogTag.c_str(),
        sessionId.Quote().c_str(),
        request.Version);

    CreateSessionsInShards(
        ctx,
        std::move(request.RequestInfo),
        std::move(request.CreateRequest),
        std::move(response),
        std::move(shardIds));
}

void TIndexTabletActor::HandleBuildShardSessionsDiff(
    const TEvIndexTabletPrivate::TEvBuildShardSessionsDiffRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    const bool adapterMode = msg->AdapterMode;
    if (adapterMode) {
        // remembering it: the sessions of such a shard should be created
        // upon each CreateSession request
        AdapterModeShardIds.insert(msg->ShardId);
    }

    if (Config->GetLazyShardSessionCreationEnabled() && !adapterMode) {
        //
        // Nothing is sent to the shard from here: there may be lots of
        // shards and the requester knows better how many of them can be
        // processed simultaneously.
        //

        if (msg->ChangesOnly) {
            //
            // The requester hasn't asked the shard for its sessions: this
            // tablet remembers which version of its sessions the shard has
            // been synced at and tells what has been changed since then.
            //

            using TResponse =
                TEvIndexTabletPrivate::TEvBuildShardSessionsDiffResponse;
            auto response = std::make_unique<TResponse>();

            const auto* syncState = ShardSyncStates.FindPtr(msg->ShardId);
            if (!syncState || !SyncSessionsInProgress) {
                // nothing is known about the sessions of this shard - they
                // have to be compared with the sessions of this tablet
                response->NeedShardSessions = true;
            } else {
                const auto& delta = GetShardSessionsDelta(syncState->Version);
                response->ShardGeneration = syncState->Generation;
                response->SessionCount =
                    static_cast<ui32>(delta.SessionsToSync.size());
                response->SessionsToSync = delta.SessionsToSync;
                response->SessionsToReset = delta.SessionsToReset;
                response->SessionsToDestroy = delta.SessionsToDestroy;
            }

            response->ShardId = std::move(msg->ShardId);
            NCloud::Reply(ctx, *ev, std::move(response));
            return;
        }

        auto diff = BuildShardSessionsDiff(msg->ShardSessions);

        // CreateSession, ResetSession and DestroySession requests of the
        // clients which this shard hasn't processed yet
        AddPendingShardSessionRequests(
            msg->ShardId,
            msg->ShardSessions,
            diff);

        using TResponse =
            TEvIndexTabletPrivate::TEvBuildShardSessionsDiffResponse;
        auto response = std::make_unique<TResponse>();
        response->ShardId = std::move(msg->ShardId);
        response->SessionCount =
            static_cast<ui32>(diff.SessionsToSync.size());
        response->SessionsToSync = std::move(diff.SessionsToSync);
        response->SessionsToReset = std::move(diff.SessionsToReset);
        response->SessionsToDestroy = std::move(diff.SessionsToDestroy);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    THashSet<TString> filter;
    for (auto& s: *ev->Get()->Sessions.MutableSessions()) {
        if (!s.GetIsOrphan()) {
            filter.insert(*s.MutableSessionId());
        }

        // The shard has a session the main tablet doesn't know about at
        // all (not even as orphan) - stale, remove it. We don't touch a
        // session that is orphan on main: it may still come back before
        // its InactivityDeadline.
        if (!FindSession(s.GetSessionId())) {
            NProtoPrivate::TDestroySessionRequest request;
            request.MutableHeaders()->SetClientId(s.GetClientId());
            request.MutableHeaders()->SetSessionId(s.GetSessionId());

            auto actor = std::make_unique<TDestroyStaleShardSessionActor>(
                LogTag,
                SelfId(),
                nullptr,   // requestInfo
                std::move(request),
                TVector<TString>{ev->Get()->ShardId},
                nullptr);   // response

            auto actorId = NCloud::Register(ctx, std::move(actor));
            WorkerActors.insert(actorId);
        }
    }
    using TResponse = TEvIndexTabletPrivate::TEvBuildShardSessionsDiffResponse;
    auto response = std::make_unique<TResponse>();
    response->ShardId = std::move(msg->ShardId);
    for (auto& request: BuildCreateSessionRequests(filter)) {
        CreateSessionsInShards(
            ctx,
            nullptr, // requestInfo
            std::move(request),
            nullptr, // response
            {response->ShardId});

        ++response->SessionCount;
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TIndexTabletActor::CreateSessionsInShards(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    NProtoPrivate::TCreateSessionRequest request,
    std::unique_ptr<TEvIndexTablet::TEvCreateSessionResponse> response,
    TVector<TString> shardIds)
{
    TString logTag = TStringBuilder() << LogTag
        << " s=" << request.GetHeaders().GetSessionId()
        << " c=" << request.GetHeaders().GetClientId();

    LOG_INFO(ctx, TFileStoreComponents::TABLET,
        "%s Creating shard sessions (%s)",
        logTag.c_str(),
        JoinSeq(",", shardIds).c_str());

    //
    // The returned FileStore structure can be huge for filesystems with many
    // shards. At the same time it's actually not used at all - it's only needed
    // by the storage service layer, not by other tablets. And the storage
    // service layer gets it from the main tablet.
    //

    request.SetNoFileStoreInfo(true);

    auto actor = std::make_unique<TCreateShardSessionsActor>(
        std::move(logTag),
        SelfId(),
        std::move(requestInfo),
        std::move(request),
        std::move(shardIds),
        std::move(response));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

}   // namespace NCloud::NFileStore::NStorage
