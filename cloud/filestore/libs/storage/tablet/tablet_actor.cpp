#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>
#include <cloud/filestore/libs/storage/tablet/model/throttler_logger.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/throttling/tablet_throttler.h>
#include <cloud/storage/core/libs/throttling/tablet_throttler_logger.h>

#include <contrib/ydb/core/base/tablet_pipe.h>
#include <contrib/ydb/core/node_whiteboard/node_whiteboard.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NNodeWhiteboard;
using namespace NTabletFlatExecutor;

using namespace NCloud::NStorage;

////////////////////////////////////////////////////////////////////////////////

const TIndexTabletActor::TStateInfo TIndexTabletActor::States[STATE_MAX] = {
    { "Boot",   (IActor::TReceiveFunc)&TIndexTabletActor::StateBoot   },
    { "Init",   (IActor::TReceiveFunc)&TIndexTabletActor::StateInit   },
    { "Work",   (IActor::TReceiveFunc)&TIndexTabletActor::StateWork   },
    { "Zombie", (IActor::TReceiveFunc)&TIndexTabletActor::StateZombie },
    { "Broken", (IActor::TReceiveFunc)&TIndexTabletActor::StateBroken },
};

////////////////////////////////////////////////////////////////////////////////

TIndexTabletActor::TIndexTabletActor(
        const TActorId& owner,
        TTabletStorageInfoPtr storage,
        TStorageConfigPtr config,
        IProfileLogPtr profileLog,
        ITraceSerializerPtr traceSerializer,
        NMetrics::IMetricsRegistryPtr metricsRegistry,
        bool useNoneCompactionPolicy)
    : TActor(&TThis::StateBoot)
    , TTabletBase(owner, std::move(storage))
    , Metrics{std::move(metricsRegistry)}
    , ProfileLog(std::move(profileLog))
    , TraceSerializer(std::move(traceSerializer))
    , ThrottlerLogger(
        [this](ui32 opType, TDuration time) {
            UpdateDelayCounter(
                static_cast<TThrottlingPolicy::EOpType>(opType),
                time);
        }
    )
    , Config(std::move(config))
    , UseNoneCompactionPolicy(useNoneCompactionPolicy)
    , BlobCodec(NBlockCodecs::Codec(Config->GetBlobCompressionCodec()))
{
    UpdateLogTag();
}

TIndexTabletActor::~TIndexTabletActor()
{
    ReleaseTransactions();
}

TString TIndexTabletActor::GetStateName(ui32 state)
{
    if (state < STATE_MAX) {
        return States[state].Name;
    }
    return "<unknown>";
}

void TIndexTabletActor::Enqueue(STFUNC_SIG)
{
    ALOG_ERROR(TFileStoreComponents::TABLET,
        LogTag
        << " IGNORING message type# " << ev->GetTypeRewrite()
        << " from Sender# " << ToString(ev->Sender)
        << " in StateBoot");
}

void TIndexTabletActor::DefaultSignalTabletActive(const TActorContext& ctx)
{
    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " Activated executor");
}

void TIndexTabletActor::Suicide(const TActorContext& ctx)
{
    NCloud::Send(ctx, Tablet(), std::make_unique<TEvents::TEvPoisonPill>());
    BecomeAux(ctx, STATE_ZOMBIE);

    // Must be done after BecomeAux(), because all requests from throttler
    // should be rejected by the tablet.
    if (Throttler) {
        Throttler->OnShutDown(ctx);
    }
}

void TIndexTabletActor::BecomeAux(const TActorContext& ctx, EState state)
{
    TABLET_VERIFY(state < STATE_MAX);

    Become(States[state].Func);
    CurrentState = state;

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s Switched to state %s (system: %s, user: %s, executor: %s)",
        LogTag.c_str(),
        States[state].Name.c_str(),
        ToString(Tablet()).c_str(),
        ToString(SelfId()).c_str(),
        ToString(ExecutorID()).c_str());

    ReportTabletState(ctx);
}

void TIndexTabletActor::ReportTabletState(const TActorContext& ctx)
{
    auto service = MakeNodeWhiteboardServiceId(SelfId().NodeId());

    auto request = std::make_unique<TEvWhiteboard::TEvTabletStateUpdate>(
        TabletID(),
        CurrentState);

    NCloud::Send(ctx, service, std::move(request));
}

void TIndexTabletActor::OnActivateExecutor(const TActorContext& ctx)
{
    BecomeAux(ctx, STATE_INIT);

    RegisterCounters(ctx);

    if (!Executor()->GetStats().IsFollower) {
        ExecuteTx<TInitSchema>(ctx, UseNoneCompactionPolicy);
    }
}

bool TIndexTabletActor::ReassignChannelsEnabled() const
{
    return true;
}

void TIndexTabletActor::ReassignDataChannelsIfNeeded(
    const NActors::TActorContext& ctx)
{
    auto channels = GetChannelsToMove(
        Config->GetReassignChannelsPercentageThreshold());

    if (channels.empty()) {
        return;
    }

    if (ReassignRequestSentTs.GetValue()) {
        const auto timeout = TDuration::Minutes(1);
        if (ReassignRequestSentTs + timeout < ctx.Now()) {
            LOG_WARN(ctx, TFileStoreComponents::TABLET,
                "%s No reaction to reassign request in %lu seconds, retrying",
                LogTag.c_str(),
                timeout.Seconds());
            ReassignRequestSentTs = TInstant::Zero();
        } else {
            return;
        }
    }

    {
        TStringBuilder sb;
        for (const auto channel: channels) {
            if (sb.Size()) {
                sb << ", ";
            }

            sb << channel;
        }

        LOG_WARN(ctx, TFileStoreComponents::TABLET,
            "%s Reassign request sent for channels: %s",
            LogTag.c_str(),
            sb.c_str());
    }

    Metrics.ReassignCount.fetch_add(
        channels.size(),
        std::memory_order_relaxed);

    NCloud::Send<TEvHiveProxy::TEvReassignTabletRequest>(
        ctx,
        MakeHiveProxyServiceId(),
        0,  // cookie
        TabletID(),
        std::move(channels));

    ReassignRequestSentTs = ctx.Now();
}

bool TIndexTabletActor::CheckSessionForDestroy(const TSession* session, ui64 seqNo)
{
    return session->GetSessionSeqNo() == seqNo &&
        session->GetSessionRwSeqNo() == seqNo;
}

bool TIndexTabletActor::OnRenderAppHtmlPage(
    NMon::TEvRemoteHttpInfo::TPtr ev,
    const TActorContext& ctx)
{
    if (!Executor() || !Executor()->GetStats().IsActive) {
        return false;
    }

    if (ev) {
        HandleHttpInfo(ev, ctx);
    }
    return true;
}

void TIndexTabletActor::OnDetach(const TActorContext& ctx)
{
    Counters = nullptr;

    Die(ctx);
}

void TIndexTabletActor::OnTabletDead(
    TEvTablet::TEvTabletDead::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    TerminateTransactions(ctx);

    for (const auto& actor: WorkerActors) {
        ctx.Send(actor, new TEvents::TEvPoisonPill());
    }

    auto writeBatch = DequeueWriteBatch();
    for (const auto& request: writeBatch) {
        TRequestInfo& requestInfo = *request.RequestInfo;
        requestInfo.CancelRoutine(ctx, requestInfo);
    }

    WorkerActors.clear();
    UnregisterFileStore(ctx);

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::AddTransaction(
    TRequestInfo& transaction,
    TRequestInfo::TCancelRoutine cancelRoutine)
{
    transaction.CancelRoutine = cancelRoutine;

    transaction.Ref();

    TABLET_VERIFY(transaction.Empty());
    ActiveTransactions.PushBack(&transaction);
}

void TIndexTabletActor::RemoveTransaction(TRequestInfo& requestInfo)
{
    TABLET_VERIFY(!requestInfo.Empty());
    requestInfo.Unlink();

    TABLET_VERIFY(requestInfo.RefCount() > 1);
    requestInfo.UnRef();
}

void TIndexTabletActor::TerminateTransactions(const TActorContext& ctx)
{
    while (ActiveTransactions) {
        TRequestInfo* requestInfo = ActiveTransactions.PopFront();
        TABLET_VERIFY(requestInfo->RefCount() >= 1);

        requestInfo->CancelRequest(ctx);
        requestInfo->UnRef();
    }
}

void TIndexTabletActor::ReleaseTransactions()
{
    while (ActiveTransactions) {
        TRequestInfo* requestInfo = ActiveTransactions.PopFront();
        TABLET_VERIFY(requestInfo->RefCount() >= 1);
        requestInfo->UnRef();
    }
}

////////////////////////////////////////////////////////////////////////////////

using TThresholds = TIndexTabletState::TBackpressureThresholds;
TThresholds TIndexTabletActor::BuildBackpressureThresholds() const
{
    return {
        Config->GetFlushThresholdForBackpressure(),
        Config->GetFlushBytesThresholdForBackpressure(),
        ScaleCompactionThreshold(
            Config->GetCompactionThresholdForBackpressure()),
        Config->GetCleanupThresholdForBackpressure(),
    };
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::ResetThrottlingPolicy()
{
    ThrottlerLogger.SetupLogTag(LogTag);
    if (!Throttler) {
        Throttler = CreateTabletThrottler(
            *this,
            ThrottlerLogger,
            AccessThrottlingPolicy());
    } else {
        Throttler->ResetPolicy(AccessThrottlingPolicy());
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest>
NProto::TError TIndexTabletActor::ValidateWriteRequest(
    const TActorContext& ctx,
    const TRequest& request,
    const TByteRange& range)
{
    if (auto error = ValidateRange(range); HasError(error)) {
        return error;
    }

    auto* handle = FindHandle(request.GetHandle());
    if (!handle || handle->GetSessionId() != GetSessionId(request)) {
        return ErrorInvalidHandle(request.GetHandle());
    }

    TString message;
    if (!IsWriteAllowed(BuildBackpressureThresholds(), &message)) {
        EnqueueFlushIfNeeded(ctx);
        EnqueueBlobIndexOpIfNeeded(ctx);

        if (CompactionStateLoadStatus.Finished &&
            ++BackpressureErrorCount >=
                Config->GetMaxBackpressureErrorsBeforeSuicide())
        {
            LOG_WARN(
                ctx,
                TFileStoreComponents::TABLET_WORKER,
                "%s Suiciding after %u backpressure errors",
                LogTag.c_str(),
                BackpressureErrorCount);

            Suicide(ctx);
        }

        return MakeError(
            E_REJECTED,
            TStringBuilder() << "rejected due to backpressure: " << message);
    }

    return NProto::TError{};
}

template NProto::TError
TIndexTabletActor::ValidateWriteRequest<NProto::TWriteDataRequest>(
    const TActorContext& ctx,
    const NProto::TWriteDataRequest& request,
    const TByteRange& range);

template NProto::TError
TIndexTabletActor::ValidateWriteRequest<NProtoPrivate::TGenerateBlobIdsRequest>(
    const TActorContext& ctx,
    const NProtoPrivate::TGenerateBlobIdsRequest& request,
    const TByteRange& range);

template NProto::TError
TIndexTabletActor::ValidateWriteRequest<NProtoPrivate::TAddDataRequest>(
    const TActorContext& ctx,
    const NProtoPrivate::TAddDataRequest& request,
    const TByteRange& range);

////////////////////////////////////////////////////////////////////////////////

NProto::TError TIndexTabletActor::IsDataOperationAllowed() const
{
    if (!CompactionStateLoadStatus.Finished) {
        return MakeError(E_REJECTED, "compaction state not loaded yet");
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

ui32 TIndexTabletActor::ScaleCompactionThreshold(ui32 t) const
{
    // Max needed for the freshly created FS case - GetBlockSize() returns 0
    // before we process our first EvUpdateConfig event.
    const ui32 blockSize = Max(GetBlockSize(), DefaultBlockSize);

    // Blob size has a limit specified in bytes whereas the capacity of a
    // single compaction range is actually specified in blocks - see
    // BlockGroupSize. That's why we need to scale the limit on the number
    // of blobs per range by something that's linear w.r.t. BlockSize.
    //
    // See issue #95.
    const ui64 factor = blockSize / DefaultBlockSize;
    return Min<ui64>(Max<ui32>(), factor * t);
}

TCompactionInfo TIndexTabletActor::GetCompactionInfo() const
{
    auto [compactRangeId, compactionScore] = GetRangeToCompact();

    const auto compactionThreshold =
        ScaleCompactionThreshold(Config->GetCompactionThreshold());
    const auto compactionThresholdAverage =
        ScaleCompactionThreshold(Config->GetCompactionThresholdAverage());

    const auto& stats = GetFileSystemStats();
    const auto compactionStats = GetCompactionMapStats(0);
    const auto used = stats.GetUsedBlocksCount();
    auto alive = stats.GetMixedBlocksCount();
    if (alive > stats.GetGarbageBlocksCount()) {
        alive -= stats.GetGarbageBlocksCount();
    } else {
        alive = 0;
    }
    const auto avgGarbagePercentage = used && alive > used
        ? 100 * static_cast<double>(alive - used) / used
        : 0;
    const auto rangeCount = compactionStats.UsedRangesCount;
    const auto avgCompactionScore = rangeCount
        ? static_cast<double>(stats.GetMixedBlobsCount()) / rangeCount
        : 0;
    // TODO: use GarbageCompactionThreshold

    const bool shouldCompact =
        avgGarbagePercentage >= Config->GetGarbageCompactionThresholdAverage()
        || avgCompactionScore >= compactionThresholdAverage;

    return {
        compactionThreshold,
        compactionThresholdAverage,
        Config->GetGarbageCompactionThreshold(),
        Config->GetGarbageCompactionThresholdAverage(),
        compactionScore,
        compactRangeId,
        avgGarbagePercentage,
        avgCompactionScore,
        Config->GetNewCompactionEnabled(),
        compactionScore >= compactionThreshold
            || Config->GetNewCompactionEnabled()
            && compactionScore > 1 && shouldCompact,
    };
}

TCleanupInfo TIndexTabletActor::GetCleanupInfo() const
{
    auto [cleanupRangeId, cleanupScore] = GetRangeToCleanup();
    const auto& stats = GetFileSystemStats();
    const auto compactionStats = GetCompactionMapStats(0);
    const auto rangeCount = compactionStats.UsedRangesCount;
    const auto avgCleanupScore = rangeCount
        ? static_cast<double>(stats.GetDeletionMarkersCount()) / rangeCount
        : 0;
    const bool shouldCleanup =
        avgCleanupScore >= Config->GetCleanupThresholdAverage();

    return {
        Config->GetCleanupThreshold(),
        Config->GetCleanupThresholdAverage(),
        cleanupScore,
        cleanupRangeId,
        avgCleanupScore,
        Config->GetNewCleanupEnabled(),
        cleanupScore >= Config->GetCleanupThreshold()
            || Config->GetNewCleanupEnabled()
            && cleanupScore && shouldCleanup,
    };
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    Throttler->StartFlushing(ctx);
}

void TIndexTabletActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_INFO_S(ctx, TFileStoreComponents::TABLET,
        LogTag << " Stop tablet because of PoisonPill request");

    Suicide(ctx);
}

void TIndexTabletActor::HandleTabletMetrics(
    const TEvLocal::TEvTabletMetrics::TPtr& ev,
    const TActorContext& ctx)
{
    // TODO
    Y_UNUSED(ev);
    Y_UNUSED(ctx);
}

void TIndexTabletActor::HandleSessionDisconnected(
    const TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
    const TActorContext& ctx)
{
    OrphanSession(ev->Sender, ctx.Now());
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleGetFileSystemConfig(
    const TEvIndexTablet::TEvGetFileSystemConfigRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response = std::make_unique<TEvIndexTablet::TEvGetFileSystemConfigResponse>();
    Convert(GetFileSystem(), *response->Record.MutableConfig());

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TIndexTabletActor::HandleGetStorageConfigFields(
    const TEvIndexTablet::TEvGetStorageConfigFieldsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response =
        std::make_unique<TEvIndexTablet::TEvGetStorageConfigFieldsResponse>();
    auto& protoFieldsToValues =
        *response->Record.MutableStorageConfigFieldsToValues();

    const auto* msg = ev->Get();

    for (const auto& field: msg->Record.GetStorageConfigFields()) {
        const auto configValue = Config->GetValueByName(field);
        switch (configValue.Status) {
            using TStatus = TStorageConfig::TValueByName::ENameStatus;
            case TStatus::FoundInDefaults:
                protoFieldsToValues[field] = "Default";
                break;
            case TStatus::FoundInProto:
                protoFieldsToValues[field] = configValue.Value;
                break;
            case TStatus::NotFound:
                protoFieldsToValues[field] = "Not found";
                break;
        }
    }
    NCloud::Reply(ctx, *ev, std::move(response));
}

void TIndexTabletActor::HandleGetStorageConfig(
    const TEvIndexTablet::TEvGetStorageConfigRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response = std::make_unique<TEvIndexTablet::TEvGetStorageConfigResponse>();
    *response->Record.MutableStorageConfig() = Config->GetStorageConfigProto();

    NCloud::Reply(
        ctx,
        *ev,
        std::move(response));
}

void TIndexTabletActor::HandleDescribeSessions(
    const TEvIndexTablet::TEvDescribeSessionsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto response =
        std::make_unique<TEvIndexTablet::TEvDescribeSessionsResponse>();

    auto sessionInfos = DescribeSessions();
    for (auto& si: sessionInfos) {
        *response->Record.AddSessions() = std::move(si);
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TIndexTabletActor::HandleForcedOperation(
    const TEvIndexTablet::TEvForcedOperationRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& request = ev->Get()->Record;
    using EMode = TEvIndexTabletPrivate::EForcedRangeOperationMode;
    EMode mode{};
    NProto::TError e;
    switch (request.GetOpType()) {
        case NProtoPrivate::TForcedOperationRequest::E_COMPACTION: {
            mode = EMode::Compaction;
            break;
        }

        case NProtoPrivate::TForcedOperationRequest::E_CLEANUP: {
            mode = EMode::Cleanup;
            break;
        }

        case NProtoPrivate::TForcedOperationRequest::E_DELETE_EMPTY_RANGES: {
            mode = EMode::DeleteZeroCompactionRanges;
            break;
        }

        default: {
            e = MakeError(E_ARGUMENT, "unsupported mode");
        }
    }

    if (e.GetCode() == S_OK && IsForcedRangeOperationRunning()) {
        const auto currentMode = GetForcedRangeOperationState()->Mode;
        if (currentMode == mode) {
            e = MakeError(S_ALREADY, "already launched");
        } else {
            e = MakeError(E_TRY_AGAIN, TStringBuilder() << "mode mismatch: "
                << static_cast<int>(mode)
                << " != " << static_cast<int>(currentMode));
        }
    }

    using TResponse = TEvIndexTablet::TEvForcedOperationResponse;
    auto response = std::make_unique<TResponse>(std::move(e));
    if (e.GetCode() == S_OK) {
        TVector<ui32> ranges;
        if (mode == EMode::DeleteZeroCompactionRanges) {
            const auto zeroRanges = GetZeroScoreRanges();
            ui32 i = 0;
            while (i < zeroRanges.size()) {
                ranges.push_back(i);
                i += Config->GetMaxDeleteZeroCompactionRangesPerTx();
            }
        } else {
            ranges = request.GetProcessAllRanges()
                ? GetAllCompactionRanges()
                : GetNonEmptyCompactionRanges();
        }
        response->Record.SetRangeCount(ranges.size());
        EnqueueForcedRangeOperation(mode, std::move(ranges));
        EnqueueForcedRangeOperationIfNeeded(ctx);
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::HandleRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        FILESTORE_SERVICE_REQUESTS(FILESTORE_HANDLE_REQUEST, TEvService)

        FILESTORE_TABLET_REQUESTS(FILESTORE_HANDLE_REQUEST, TEvIndexTablet)
        FILESTORE_TABLET_REQUESTS_PRIVATE(FILESTORE_HANDLE_REQUEST, TEvIndexTabletPrivate)

        default:
            return false;
    }

    return true;
}

bool TIndexTabletActor::HandleCompletions(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        FILESTORE_TABLET_REQUESTS_PRIVATE_ASYNC(FILESTORE_HANDLE_COMPLETION, TEvIndexTabletPrivate)

        default:
            return false;
    }

    return true;
}

bool TIndexTabletActor::IgnoreCompletions(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        FILESTORE_TABLET_REQUESTS_PRIVATE_ASYNC(FILESTORE_IGNORE_COMPLETION, TEvIndexTabletPrivate)

        default:
            return false;
    }

    return true;
}

bool TIndexTabletActor::RejectRequests(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        FILESTORE_SERVICE_REQUESTS(FILESTORE_REJECT_REQUEST, TEvService)

        FILESTORE_TABLET_REQUESTS(FILESTORE_REJECT_REQUEST, TEvIndexTablet)
        FILESTORE_TABLET_REQUESTS_PRIVATE(FILESTORE_REJECT_REQUEST, TEvIndexTabletPrivate)

        default:
            return false;
    }

    return true;
}

bool TIndexTabletActor::RejectRequestsByBrokenTablet(STFUNC_SIG)
{
    switch (ev->GetTypeRewrite()) {
        FILESTORE_SERVICE_REQUESTS(FILESTORE_REJECT_REQUEST_BY_BROKEN_TABLET, TEvService)

        FILESTORE_TABLET_REQUESTS(FILESTORE_REJECT_REQUEST_BY_BROKEN_TABLET, TEvIndexTablet)
        FILESTORE_TABLET_REQUESTS_PRIVATE(FILESTORE_REJECT_REQUEST_BY_BROKEN_TABLET, TEvIndexTabletPrivate)

        default:
            return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TIndexTabletActor::StateBoot)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);

        IgnoreFunc(TEvLocal::TEvTabletMetrics);
        IgnoreFunc(TEvIndexTabletPrivate::TEvUpdateCounters);
        IgnoreFunc(TEvIndexTabletPrivate::TEvUpdateLeakyBucketCounters);
        IgnoreFunc(TEvIndexTabletPrivate::TEvReleaseCollectBarrier);
        IgnoreFunc(TEvIndexTabletPrivate::TEvForcedRangeOperationProgress);

        IgnoreFunc(TEvHiveProxy::TEvReassignTabletResponse);

        FILESTORE_HANDLE_REQUEST(WaitReady, TEvIndexTablet)

        default:
            StateInitImpl(ev, SelfId());
            break;
    }
}

STFUNC(TIndexTabletActor::StateInit)
{
    switch (ev->GetTypeRewrite()) {
        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);
        IgnoreFunc(TEvHiveProxy::TEvReassignTabletResponse);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvLocal::TEvTabletMetrics, HandleTabletMetrics);
        HFunc(TEvFileStore::TEvUpdateConfig, HandleUpdateConfig);
        HFunc(TEvIndexTabletPrivate::TEvUpdateCounters, HandleUpdateCounters);
        HFunc(TEvIndexTabletPrivate::TEvUpdateLeakyBucketCounters, HandleUpdateLeakyBucketCounters);
        HFunc(TEvIndexTabletPrivate::TEvReleaseCollectBarrier, HandleReleaseCollectBarrier);
        HFunc(
            TEvIndexTabletPrivate::TEvForcedRangeOperationProgress,
            HandleForcedRangeOperationProgress);
        HFunc(
            TEvIndexTabletPrivate::TEvNodeCreatedInFollower,
            HandleNodeCreatedInFollower);
        HFunc(
            TEvIndexTabletPrivate::TEvNodeUnlinkedInFollower,
            HandleNodeUnlinkedInFollower);

        FILESTORE_HANDLE_REQUEST(WaitReady, TEvIndexTablet)

        default:
            if (!RejectRequests(ev) &&
                !HandleDefaultEvents(ev, SelfId()))
            {
                HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET);
            }
            break;
    }
}

STFUNC(TIndexTabletActor::StateWork)
{
    // user related requests & events completion
    if (HandleRequests(ev) || HandleCompletions(ev)) {
        return;
    }

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvIndexTabletPrivate::TEvReadDataCompleted, HandleReadDataCompleted);
        HFunc(TEvIndexTabletPrivate::TEvWriteDataCompleted, HandleWriteDataCompleted);
        HFunc(TEvIndexTabletPrivate::TEvAddDataCompleted, HandleAddDataCompleted);

        HFunc(TEvIndexTabletPrivate::TEvUpdateCounters, HandleUpdateCounters);
        HFunc(TEvIndexTabletPrivate::TEvUpdateLeakyBucketCounters, HandleUpdateLeakyBucketCounters);

        HFunc(TEvIndexTabletPrivate::TEvReleaseCollectBarrier, HandleReleaseCollectBarrier);
        HFunc(
            TEvIndexTabletPrivate::TEvForcedRangeOperationProgress,
            HandleForcedRangeOperationProgress);
        HFunc(
            TEvIndexTabletPrivate::TEvNodeCreatedInFollower,
            HandleNodeCreatedInFollower);
        HFunc(
            TEvIndexTabletPrivate::TEvNodeUnlinkedInFollower,
            HandleNodeUnlinkedInFollower);

        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);

        HFunc(TEvLocal::TEvTabletMetrics, HandleTabletMetrics);
        HFunc(TEvFileStore::TEvUpdateConfig, HandleUpdateConfig);

        // ignoring errors - will resend reassign request after a timeout anyway
        IgnoreFunc(TEvHiveProxy::TEvReassignTabletResponse);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET);
            }
            break;
    }
}

STFUNC(TIndexTabletActor::StateZombie)
{
    // user related requests & events completion
    if (RejectRequests(ev) || IgnoreCompletions(ev)) {
        return;
    }

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);
        HFunc(TEvTabletPipe::TEvServerDisconnected, HandleSessionDisconnected);

        IgnoreFunc(TEvFileStore::TEvUpdateConfig);

        // private api
        IgnoreFunc(TEvIndexTabletPrivate::TEvUpdateCounters);
        IgnoreFunc(TEvIndexTabletPrivate::TEvUpdateLeakyBucketCounters);

        IgnoreFunc(TEvIndexTabletPrivate::TEvReleaseCollectBarrier);
        IgnoreFunc(TEvIndexTabletPrivate::TEvForcedRangeOperationProgress);

        IgnoreFunc(TEvIndexTabletPrivate::TEvReadDataCompleted);
        IgnoreFunc(TEvIndexTabletPrivate::TEvWriteDataCompleted);
        IgnoreFunc(TEvIndexTabletPrivate::TEvAddDataCompleted);

        // tablet related requests
        IgnoreFunc(TEvents::TEvPoisonPill);
        IgnoreFunc(TEvTabletPipe::TEvServerConnected);

        IgnoreFunc(TEvLocal::TEvTabletMetrics);
        IgnoreFunc(TEvHiveProxy::TEvReassignTabletResponse);

        HFunc(
            TEvIndexTabletPrivate::TEvNodeCreatedInFollower,
            HandleNodeCreatedInFollower);
        HFunc(
            TEvIndexTabletPrivate::TEvNodeUnlinkedInFollower,
            HandleNodeUnlinkedInFollower);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET);
            break;
    }
}

STFUNC(TIndexTabletActor::StateBroken)
{
    if (RejectRequestsByBrokenTablet(ev) || IgnoreCompletions(ev)) {
        return;
    }

    switch (ev->GetTypeRewrite()) {
        IgnoreFunc(TEvIndexTabletPrivate::TEvUpdateCounters);
        IgnoreFunc(TEvIndexTabletPrivate::TEvUpdateLeakyBucketCounters);
        IgnoreFunc(TEvIndexTabletPrivate::TEvReleaseCollectBarrier);
        IgnoreFunc(TEvIndexTabletPrivate::TEvForcedRangeOperationProgress);

        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvTablet::TEvTabletDead, HandleTabletDead);

        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        HFunc(TEvTabletPipe::TEvServerDisconnected, HandleSessionDisconnected);

        IgnoreFunc(TEvLocal::TEvTabletMetrics);
        IgnoreFunc(TEvFileStore::TEvUpdateConfig);

        IgnoreFunc(TEvIndexTabletPrivate::TEvReadDataCompleted);
        IgnoreFunc(TEvIndexTabletPrivate::TEvWriteDataCompleted);
        IgnoreFunc(TEvIndexTabletPrivate::TEvAddDataCompleted);

        IgnoreFunc(TEvHiveProxy::TEvReassignTabletResponse);

        HFunc(
            TEvIndexTabletPrivate::TEvNodeCreatedInFollower,
            HandleNodeCreatedInFollower);
        HFunc(
            TEvIndexTabletPrivate::TEvNodeUnlinkedInFollower,
            HandleNodeUnlinkedInFollower);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::RebootTabletOnCommitOverflow(
    const TActorContext& ctx,
    const TString& request)
{
    LOG_ERROR(ctx, TFileStoreComponents::TABLET,
        "%s CommitId overflow in %s. Restarting",
        LogTag.c_str(),
        request.c_str());

    ReportTabletCommitIdOverflow();
    Suicide(ctx);
}

void TIndexTabletActor::RegisterFileStore(const NActors::TActorContext& ctx)
{
    if (!GetFileSystemId()) {
        // not ready yet
        return;
    }

    NProtoPrivate::TFileSystemConfig config;
    Convert(GetFileSystem(), config);

    auto request = std::make_unique<TEvService::TEvRegisterLocalFileStoreRequest>(
        GetFileSystemId(),
        TabletID(),
        GetGeneration(),
        std::move(config));

    ctx.Send(MakeStorageServiceId(), request.release());
}

void TIndexTabletActor::UnregisterFileStore(const NActors::TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvUnregisterLocalFileStoreRequest>(
        GetFileSystemId(),
        GetGeneration());

    ctx.Send(MakeStorageServiceId(), request.release());
}

void TIndexTabletActor::UpdateLogTag()
{
    if (GetFileSystemId()) {
        TIndexTabletState::UpdateLogTag(
            Sprintf("[f:%s][t:%lu]",
                GetFileSystemId().c_str(),
                TabletID()));
    } else {
        TIndexTabletState::UpdateLogTag(
            Sprintf("[t:%lu]",
                TabletID()));
    }
}

////////////////////////////////////////////////////////////////////////////////

i64 TIndexTabletActor::TMetrics::TakeTotalRequestBytes()
{
    i64 sumRequestBytes = 0;
    for (auto* metric: AllRequestMetrics) {
        sumRequestBytes += metric->RequestBytes;
    }
    auto delta = sumRequestBytes - LastNetworkMetric;
    LastNetworkMetric = sumRequestBytes;
    return delta;
}

}   // namespace NCloud::NFileStore::NStorage
