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
    features->SetThreeStageWriteEnabled(config.GetThreeStageWriteEnabled());
    features->SetTwoStageReadDisabledForHDD(
        config.GetTwoStageReadDisabledForHDD());
    features->SetThreeStageWriteDisabledForHDD(
        config.GetThreeStageWriteDisabledForHDD());
    features->SetEntryTimeout(config.GetEntryTimeout().MilliSeconds());
    features->SetNegativeEntryTimeout(
        config.GetNegativeEntryTimeout().MilliSeconds());
    features->SetAttrTimeout(config.GetAttrTimeout().MilliSeconds());
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
    features->SetAsyncHandleOperationPeriod(
        config.GetAsyncHandleOperationPeriod().MilliSeconds());

    features->SetGuestWriteBackCacheEnabled(
        config.GetGuestWriteBackCacheEnabled());

    features->SetGuestPageCacheDisabled(config.GetGuestPageCacheDisabled());
    features->SetExtendedAttributesDisabled(
        config.GetExtendedAttributesDisabled());

    features->SetServerWriteBackCacheEnabled(
        config.GetServerWriteBackCacheEnabled());

    features->SetParentlessFilesOnly(config.GetParentlessFilesOnly());
    features->SetAllowHandlelessIO(config.GetAllowHandlelessIO());

    features->SetDirectoryCreationInShardsEnabled(
        fileSystem.GetDirectoryCreationInShardsEnabled());

    // HasXAttrs is false only if LazyXAttrsEnabled == true and we know for sure that
    // there are no XAttrs in the filesystem
    const bool hasXAttrs =
        !config.GetLazyXAttrsEnabled() ||
        fileSystemStats.GetHasXAttrs() !=
            static_cast<ui64>(TIndexTabletActor::EHasXAttrs::False);
    features->SetHasXAttrs(hasXAttrs);
}

////////////////////////////////////////////////////////////////////////////////

TActorId DoRecoverSession(
    TIndexTabletDatabase& db,
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
    NProto::TFileStore& fileStore)
{
    fileStore.SetFileSystemId(fileSystem.GetFileSystemId());
    fileStore.SetProjectId(fileSystem.GetProjectId());
    fileStore.SetFolderId(fileSystem.GetFolderId());
    fileStore.SetCloudId(fileSystem.GetCloudId());
    fileStore.SetBlockSize(fileSystem.GetBlockSize());
    fileStore.SetBlocksCount(fileSystem.GetBlocksCount());
    // TODO need set ConfigVersion?
    fileStore.SetNodesCount(fileSystem.GetNodesCount());
    fileStore.SetStorageMediaKind(fileSystem.GetStorageMediaKind());
    fileStore.MutableShardFileSystemIds()->CopyFrom(
        fileSystem.GetShardFileSystemIds());
}

////////////////////////////////////////////////////////////////////////////////

using TCreateShardSessionsActor = TShardRequestActor<
    TEvIndexTablet::TEvCreateSessionRequest,
    TEvIndexTablet::TEvCreateSessionResponse>;

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
        DumpMessage(msg->Record).c_str());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    const auto expectedShardCount = CalculateExpectedShardCount();
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

    AddTransaction<TEvIndexTablet::TCreateSessionMethod>(*requestInfo);

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

    TIndexTabletDatabase db(tx.DB);

    // check if client reconnecting with known session id
    auto* session = FindSession(sessionId);
    if (session) {
        if (session->GetClientId() == clientId) {
            args.SessionId = session->GetSessionId();
            auto toKill = DoRecoverSession(
                db,
                *this,
                session,
                clientId,
                args.SessionId,
                checkpointId,
                seqNo,
                readOnly,
                owner,
                ctx);
            if (toKill != owner) {
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
        if (session) {
            LOG_INFO(ctx, TFileStoreComponents::TABLET,
                "%s CreateSession c:%s, s:%s recovered by client",
                LogTag.c_str(),
                clientId.c_str(),
                session->GetSessionId().c_str());

            args.SessionId = session->GetSessionId();

            DoRecoverSession(
                db,
                *this,
                session,
                clientId,
                args.SessionId,
                checkpointId,
                seqNo,
                readOnly,
                owner,
                ctx);

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

    auto sessionOptions = TSession::CreateSessionOptions(Config);

    CreateSession(
        db,
        clientId,
        args.SessionId,
        checkpointId,
        originFqdn,
        seqNo,
        readOnly,
        owner,
        sessionOptions);
}

void TIndexTabletActor::CompleteTx_CreateSession(
    const TActorContext& ctx,
    TTxIndexTablet::TCreateSession& args)
{
    RemoveTransaction(*args.RequestInfo);

    using TResponse = TEvIndexTablet::TEvCreateSessionResponse;

    if (HasError(args.Error)) {
        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s CreateSession failed (%s)",
            LogTag.c_str(),
            FormatError(args.Error).c_str());

        auto response = std::make_unique<TResponse>(args.Error);
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
    auto& fileStore = *response->Record.MutableFileStore();
    Convert(GetFileSystem(), fileStore);
    FillFeatures(GetFileSystem(), GetFileSystemStats(), *Config, fileStore);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s New session TFileStoreFeatures '%s'",
        LogTag.c_str(),
        fileStore.GetFeatures().ShortDebugString().c_str());

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

void TIndexTabletActor::HandleSyncShardSessions(
    const TEvIndexTabletPrivate::TEvSyncShardSessionsRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    THashSet<TString> filter;
    for (auto& s: *ev->Get()->Sessions.MutableSessions()) {
        if (!s.GetIsOrphan()) {
            filter.insert(*s.MutableSessionId());
        }
    }
    TEvIndexTabletPrivate::TShardSessionsInfo info;
    info.ShardId = std::move(ev->Get()->ShardId);
    for (auto& request: BuildCreateSessionRequests(filter)) {
        CreateSessionsInShards(
            ctx,
            nullptr, // requestInfo
            std::move(request),
            nullptr, // response
            {info.ShardId});

        ++info.SessionCount;
    }

    using TResponse = TEvIndexTabletPrivate::TEvSyncShardSessionsResponse;
    auto response = std::make_unique<TResponse>();
    response->Info = std::move(info);
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
