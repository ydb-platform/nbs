#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/storage/tablet/tablet_state.h>

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/kikimr/tenant.h>
#include <cloud/storage/core/libs/throttling/tablet_throttler.h>
#include <cloud/storage/core/libs/viewer/tablet_monitoring.h>
#include <cloud/storage/core/libs/xsl_render/xsl_render.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/util/is_equal.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <contrib/ydb/library/actors/protos/actors.pb.h>

#include <util/generic/xrange.h>
#include <util/stream/str.h>
#include <util/string/join.h>
#include <util/system/hostname.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NActors::NMon;
using namespace NKikimr;

namespace {
    const char* const xslTemplate = {
        #include "xsl_templates/tablet_actor_monitoring.xsl"
    };

    const char* const xslTemplateRange = {
        #include "xsl_templates/tablet_actor_monitoring_range.xsl"
    };

////////////////////////////////////////////////////////////////////////////////

enum class EAlertLevel
{
    SUCCESS,
    INFO,
    WARNING,
    DANGER
};

////////////////////////////////////////////////////////////////////////////////

TCgiParameters GatherHttpParameters(const TEvRemoteHttpInfo& msg)
{
    auto params = msg.Cgi();
    if (const auto& ext = msg.ExtendedQuery;
        ext && ext->GetMethod() == HTTP_METHOD_POST)
    {
        for (const auto& param : ext->GetPostParams()) {
            params.emplace(param.GetKey(), param.GetValue());
        }
    }
    return params;
}

[[maybe_unused]] TCgiParameters GetHttpMethodParameters(
    const TEvRemoteHttpInfo& msg)
{
    if (msg.GetMethod() != HTTP_METHOD_POST) {
        return msg.Cgi();
    }

    TCgiParameters params;
    if (const auto& ext = msg.ExtendedQuery;
        ext && ext->GetMethod() == HTTP_METHOD_POST)
    {
        for (const auto& param : ext->GetPostParams()) {
            params.emplace(param.GetKey(), param.GetValue());
        }
    }

   return params;
}

HTTP_METHOD GetHttpMethodType(const NActors::NMon::TEvRemoteHttpInfo& msg)
{
    if (const auto& ext = msg.ExtendedQuery; ext) {
        return static_cast<HTTP_METHOD>(ext->GetMethod());
    }

    return msg.GetMethod();
}

TStringBuf AlertClassFromLevel(EAlertLevel alertLevel)
{
    switch (alertLevel) {
        case EAlertLevel::SUCCESS:
            return "alert-success";
        case EAlertLevel::INFO:
            return "alert-info";
        case EAlertLevel::WARNING:
            return "alert-warning";
        case EAlertLevel::DANGER:
            return "alert-danger";
        default:
            return {};
    }
}

void BuildNotifyPageWithRedirect(
    IOutputStream& out,
    const TString& message,
    const TString& redirect,
    EAlertLevel alertLevel)
{
    out << "<div class='jumbotron";

    auto alertClass = AlertClassFromLevel(alertLevel);
    if (alertClass) {
        out << " " << alertClass;
    }

    out << "'>";
    out << "<h2>";
    out << message;
    out << "</h2>";
    out << "</div>";
    out << "<script type='text/javascript'>\n";
    out << "$(function() {\n";
    out << "    setTimeout(function() {";
    out << "        window.location.href = '";
    out << redirect;
    out << "'";
    out << "    }, 2000);";
    out << "});\n";
    out << "</script>\n";
}

void BuildTabletNotifyPageWithRedirect(
    IOutputStream& out,
    const TString& message,
    ui64 tabletId,
    EAlertLevel alertLevel)
{
    BuildNotifyPageWithRedirect(
        out,
        message,
        TStringBuilder() << "../tablets/app?TabletID=" << tabletId,
        alertLevel);
}

void SendHttpResponse(
    const TActorContext& ctx,
    ui64 tablet,
    TRequestInfo& requestInfo,
    TString message,
    EAlertLevel alertLevel)
{
    TStringStream out;
    BuildTabletNotifyPageWithRedirect(out, message, tablet, alertLevel);
    NCloud::Reply(ctx, requestInfo, std::make_unique<NMon::TEvRemoteHttpInfoRes>(
        std::move(out.Str())));
}

void RejectHttpRequest(
    const TActorContext& ctx,
    ui64 tablet,
    TRequestInfo& requestInfo,
    TString message)
{
    LOG_ERROR_S(ctx, TFileStoreComponents::TABLET, message);
    SendHttpResponse(ctx, tablet, requestInfo, std::move(message), EAlertLevel::DANGER);
}

////////////////////////////////////////////////////////////////////////////////

void DumpTabletNotReady(
    IOutputStream& out,
    const TTabletStorageInfo& storage)
{
    HTML(out) {
        TAG(TH3) {
            out << "Tablet <a href='../tablets?TabletID="
                << storage.TabletID << "</a> not ready yet";
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void DumpOperationState(
    const TString& opName,
    const TOperationState& state,
    NXml::TNode root)
{
    NCloud::NStorage::NTNodeWrapper::TFieldAdder(root.AddChild("cd", " "))
        ("name", opName)
        ("state", static_cast<ui32>(state.GetOperationState()))
        ("timestamp", state.GetStateChanged())
        ("completed", state.GetCompleted())
        ("failed", state.GetFailed())
        ("backoff", state.GetBackoffTimeout().ToString());
}

////////////////////////////////////////////////////////////////////////////////

void DumpCompactionRangeInfo(
    NXml::TNode root,
    const TVector<TCompactionRangeInfo>& ranges)
{
    auto rangesXml = NCloud::NStorage::NTNodeWrapper::TFieldAdder(root.AddChild("ranges", " "));
    for (const auto& range: ranges) {
        rangesXml.AddFieldIn("cd", " ")
            ("id", range.RangeId)
            ("blobs", range.Stats.BlobsCount)
            ("deletions", range.Stats.DeletionsCount);
    }
}

void DumpCompactionMap(
    NXml::TNode root,
    const TCompactionMapStats& stats)
{
    root.AddChild("used_ranges_count", stats.UsedRangesCount);
    root.AddChild("allocated_ranges_count", stats.AllocatedRangesCount);
    ui32 percents = 0;
    if (stats.AllocatedRangesCount) {
        percents = (stats.UsedRangesCount * 100 / stats.AllocatedRangesCount);
    }
    root.AddChild("ranges_percents", percents);

    auto info = root.AddChild("compaction_range_info", " ");
    auto compaction = info.AddChild("cd", " ");
    compaction.AddChild("name", "Top ranges by compaction score");
    DumpCompactionRangeInfo(compaction, stats.TopRangesByCompactionScore);

    auto cleanup = info.AddChild("cd", " ");
    cleanup.AddChild("name", "Top ranges by cleanup score");
    DumpCompactionRangeInfo(cleanup, stats.TopRangesByCleanupScore);

    auto garbage = info.AddChild("cd", " ");
    garbage.AddChild("name", "Top ranges by garbage score");
    DumpCompactionRangeInfo(garbage, stats.TopRangesByGarbageScore);
}

////////////////////////////////////////////////////////////////////////////////

void DumpProfillingAllocatorStats(
    const TFileStoreAllocRegistry& registry,
    NXml::TNode root)
{
    auto allocStats = NCloud::NStorage::NTNodeWrapper::TFieldAdder(root.AddChild("alloc_stats", " "));
    ui64 allBytes = 0;
    for (ui32 i = 0; i < static_cast<ui32>(EAllocatorTag::Max); ++i) {
        auto tag = static_cast<EAllocatorTag>(i);
        const ui64 bytes = registry.GetAllocator(tag)->GetBytesAllocated();
        allocStats.AddFieldIn("cd", " ")
            ("name", tag)
            ("value", FormatByteSize(bytes));
        allBytes += bytes;
    }
    allocStats.AddFieldIn("cd", " ")
        ("name", "Summary")
        ("value", FormatByteSize(allBytes));
}

////////////////////////////////////////////////////////////////////////////////

void DumpPerformanceProfile(
    bool storageThrottlingEnabled,
    const NProto::TFileStorePerformanceProfile& profile,
    NXml::TNode root)
{
    NCloud::NStorage::NTNodeWrapper::TFieldAdder adder(root);
#define DUMP_PROFILE_STAT(name, value)                                      \
    adder.AddFieldIn("cd", " ")("name", name)("value", value);              \
// DUMP_PROFILE_STAT

    DUMP_PROFILE_STAT("StorageThrottlingEnabled", storageThrottlingEnabled)
    DUMP_PROFILE_STAT("ThrottlingEnabled", profile.GetThrottlingEnabled())
    DUMP_PROFILE_STAT("MaxReadIops", profile.GetMaxReadIops())
    DUMP_PROFILE_STAT("MaxWriteIops", profile.GetMaxWriteIops())
    DUMP_PROFILE_STAT("MaxReadBandwidth", profile.GetMaxReadBandwidth())
    DUMP_PROFILE_STAT("MaxWriteBandwidth", profile.GetMaxWriteBandwidth())
    DUMP_PROFILE_STAT("BoostTime", profile.GetBoostTime())
    DUMP_PROFILE_STAT("BoostRefillTime", profile.GetBoostRefillTime())
    DUMP_PROFILE_STAT("BoostPercentage", profile.GetBoostPercentage())
    DUMP_PROFILE_STAT("BurstPercentage", profile.GetBurstPercentage())
    DUMP_PROFILE_STAT("DefaultPostponedRequestWeight", profile.GetDefaultPostponedRequestWeight())
    DUMP_PROFILE_STAT("MaxPostponedWeight", profile.GetMaxPostponedWeight())
    DUMP_PROFILE_STAT("MaxWriteCostMultiplier", profile.GetMaxWriteCostMultiplier())
    DUMP_PROFILE_STAT("MaxPostponedTime", profile.GetMaxPostponedTime())
    DUMP_PROFILE_STAT("MaxPostponedCount", profile.GetMaxPostponedCount())

#undef DUMP_PROFILE_STAT
}

////////////////////////////////////////////////////////////////////////////////

void DumpThrottlingState(
    const ITabletThrottler* throttler,
    const TThrottlingPolicy& policy,
    NXml::TNode root)
{
    const auto& config = policy.GetConfig();
    auto state = NCloud::NStorage::NTNodeWrapper::TFieldAdder(root.AddChild("throttler_state", " "));

#define DUMP_THROTTING_STAT(name, value)                                    \
    state.AddFieldIn("cd", " ")("name", name)("value", value);              \
// DUMP_THROTTING_STAT

    DUMP_THROTTING_STAT("Throttling enabled", config.ThrottlingEnabled)
    DUMP_THROTTING_STAT("Config version", policy.GetVersion())
    {
        const auto& p = config.DefaultParameters;
        DUMP_THROTTING_STAT("MaxReadIops", p.MaxReadIops)
        DUMP_THROTTING_STAT("MaxWriteIops", p.MaxWriteIops)
        DUMP_THROTTING_STAT("MaxReadBandwidth", p.MaxReadBandwidth)
        DUMP_THROTTING_STAT("MaxWriteBandwidth", p.MaxWriteBandwidth)
    }
    {
        const auto& p = config.BoostParameters;
        DUMP_THROTTING_STAT("BoostTime", p.BoostTime.MilliSeconds())
        DUMP_THROTTING_STAT("BoostRefillTime", p.BoostRefillTime.MilliSeconds())
        DUMP_THROTTING_STAT("BoostPercentage", p.BoostPercentage)
    }
    DUMP_THROTTING_STAT("BurstPercentage", config.BurstPercentage)
    DUMP_THROTTING_STAT("DefaultPostponedRequestWeight", config.DefaultPostponedRequestWeight)
    {
        const auto& l = config.DefaultThresholds;
        DUMP_THROTTING_STAT("MaxPostponedWeight", l.MaxPostponedWeight)
        DUMP_THROTTING_STAT("MaxWriteCostMultiplier", l.MaxWriteCostMultiplier)
        DUMP_THROTTING_STAT("MaxPostponedTime", l.MaxPostponedTime.MilliSeconds())
        DUMP_THROTTING_STAT("MaxPostponedCount", l.MaxPostponedCount)
    }
    if (throttler) {
        DUMP_THROTTING_STAT("PostponedQueueSize", throttler->GetPostponedRequestsCount())
    }
    DUMP_THROTTING_STAT("PostponedQueueWeight", policy.CalculatePostponedWeight())
    DUMP_THROTTING_STAT("WriteCostMultiplier", policy.GetWriteCostMultiplier())
    DUMP_THROTTING_STAT("CurrentBoostBudget", policy.GetCurrentBoostBudget())
    
#undef DUMP_THROTTING_STAT

    auto params = NCloud::NStorage::NTNodeWrapper::TFieldAdder(root.AddChild("throttler_params", " "));

#define DUMP_THROTTING_STAT(name, value)                                    \
    params.AddFieldIn("cd", " ")("name", name)("value", value);             \
// DUMP_THROTTING_STAT
    DUMP_THROTTING_STAT("ReadC1", policy.C1(TThrottlingPolicy::EOpType::Read))
    DUMP_THROTTING_STAT("ReadC2", FormatByteSize(policy.C2(TThrottlingPolicy::EOpType::Read)))
    DUMP_THROTTING_STAT("WriteC1", policy.C1(TThrottlingPolicy::EOpType::Write))
    DUMP_THROTTING_STAT("WriteC2", FormatByteSize(policy.C2(TThrottlingPolicy::EOpType::Write)))

#undef DUMP_THROTTING_STAT
}

////////////////////////////////////////////////////////////////////////////////

/**
 * @brief Display last `limit` lines of `sessionHistory`
 */
void DumpSessionHistory(
    NXml::TNode root,
    const TSessionHistoryList& sessionHistory,
    size_t limit = 100)
{
    NCloud::NStorage::NTNodeWrapper::TFieldAdder sessionsAdder(root.AddChild("session_history", " "));
    for (auto it = sessionHistory.rbegin();
                 it != sessionHistory.rend() && limit;
                 ++it, --limit)
    {
        sessionsAdder.AddFieldIn("cd", " ")
            ("client_id", it->GetClientId())
            ("fqdn", it->GetOriginFqdn())
            ("timestamp", TInstant::MicroSeconds(it->GetTimestampUs()))
            ("session_id", it->GetSessionId())
            ("action_type", it->GetEntryTypeString());
    }
}

////////////////////////////////////////////////////////////////////////////////

/**
 * @brief Display sessions the tablet knows
 */
void DumpSessions(NXml::TNode root, const TVector<TMonSessionInfo>& sessions)
{
    NCloud::NStorage::NTNodeWrapper::TFieldAdder sessionsAdder(root.AddChild("sessions", " "));
    for (const auto& session: sessions) {
        const auto recoveryTimestamp = TInstant::MicroSeconds(
            session.ProtoInfo.GetRecoveryTimestampUs());
        TStringStream recovery;
        if (recoveryTimestamp) {
            recovery << recoveryTimestamp;
        }
        for (const auto& ss: session.SubSessions) {
            sessionsAdder.AddFieldIn("cd", " ")
                ("client_id", session.ProtoInfo.GetClientId())
                ("fqdn", session.ProtoInfo.GetOriginFqdn())
                ("session_id", session.ProtoInfo.GetSessionId())
                ("recovery", recovery.Str())
                ("seq_no", ss.SeqNo)
                ("readonly", ss.ReadOnly ? "True" : "False")
                ("owner", ToString(ss.Owner));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void DumpChannels(
    NXml::TNode root,
    const TVector<NCloud::NStorage::TChannelMonInfo>& channelInfos,
    const TTabletStorageInfo& storage,
    ui64 hiveTabletId)
{
    NCloud::NStorage::DumpChannelsXml(
        root,
        channelInfos,
        storage,
        [&] (ui32 groupId, const TString& storagePool) {
            // TODO: group mon url
            Y_UNUSED(groupId);
            Y_UNUSED(storagePool);
            return TString();
        },
        [&] (NXml::TNode cd, ui64 hiveTabletId, ui64 tabletId, ui32 c) {
            // TODO: reassign button
            Y_UNUSED(cd);
            Y_UNUSED(hiveTabletId);
            Y_UNUSED(tabletId);
            Y_UNUSED(c);
        },
        hiveTabletId);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TIndexTabletMonitoringActor
    : public TActorBootstrapped<TIndexTabletMonitoringActor<T>>
{
    using TThis = TIndexTabletMonitoringActor<T>;

    TRequestInfoPtr RequestInfo;
    const TActorId Owner;
    const ui64 TabletId;

    std::unique_ptr<T> Request;

    TIndexTabletMonitoringActor(
            TRequestInfoPtr requestInfo,
            TActorId owner,
            ui64 tablet,
            std::unique_ptr<T> request)
        : RequestInfo(std::move(requestInfo))
        , Owner(owner)
        , TabletId(tablet)
        , Request(std::move(request))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        ctx.Send(Owner, Request.release());
        TThis::Become(&TThis::StateWork);
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvIndexTabletPrivate::TEvDumpCompactionRangeResponse, HandleDumpCompactionRange);

            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

            default:
                HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET_WORKER);
                break;
        }
    }

    void HandleDumpCompactionRange(
        const TEvIndexTabletPrivate::TEvDumpCompactionRangeResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        TStringStream out;
        GenerateResponsePage(out, *msg);
        ReplyAndDie(ctx, {}, std::move(out.Str()));
    }

    void GenerateResponsePage(
        IOutputStream& out,
        const TEvIndexTabletPrivate::TEvDumpCompactionRangeResponse& msg)
    {
        NXml::TDocument data("root", NXml::TDocument::RootName);
        NCloud::NStorage::NTNodeWrapper::TFieldAdder root = data.Root();
        root("tablet_id", TabletId)
            ("node_id", Owner.NodeId())
            ("hostname", HostName())
            ("range_id", msg.RangeId);
        auto blocks = root.AddFieldIn("blocks", " ");
        for (const auto& blob: msg.Blobs) {
            for (const auto& block: blob.Blocks) {
                blocks.AddFieldIn("cd", " ")
                    ("node_id", block.NodeId)
                    ("block_index", block.BlockIndex)
                    ("blob_id", blob.BlobId)
                    ("min_commit_id", block.MinCommitId)
                    ("max_commit_id", block.MaxCommitId);
            }
        }

        NCloud::NStorage::NXSLRender::NXSLRender(xslTemplateRange, data, out);
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"), {});
    }

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error,
        TString out)
    {
        FILESTORE_TRACK(
            ResponseSent_TabletWorker,
            RequestInfo->CallContext,
            "DumpCompactionRange");

        {
            // notify tablet
            using TCompletion =
                TEvIndexTabletPrivate::TEvDumpCompactionRangeCompleted;
            auto response = std::make_unique<TCompletion>();
            NCloud::Send(ctx, Owner, std::move(response));
        }

        if (!HasError(error)) {
            NCloud::Reply(
                ctx,
                *RequestInfo,
                std::make_unique<NMon::TEvRemoteHttpInfoRes>(std::move(out)));
        } else {
            RejectHttpRequest(
                ctx,
                TabletId,
                *RequestInfo,
                "Dump compaction range request was cancelled");
        }

        TThis::Die(ctx);
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleHttpInfo(
    const NMon::TEvRemoteHttpInfo::TPtr& ev,
    const TActorContext& ctx)
{
    using THttpHandler = void(TIndexTabletActor::*)(
        const NActors::TActorContext&,
        const TCgiParameters&,
        TRequestInfoPtr);

    using THttpHandlers = THashMap<TString, THttpHandler>;

    static const THttpHandlers postActions = {{
        {"forceOperationAll",
         &TIndexTabletActor::HandleHttpInfo_ForceOperation},
    }};

    static const THttpHandlers getActions {{
        {"dumpRange",       &TIndexTabletActor::HandleHttpInfo_DumpCompactionRange },
    }};

    const auto* msg = ev->Get();
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s HTTP request: %s",
        LogTag.c_str(),
        msg->Query.Quote().c_str());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());
    requestInfo->StartedTs = ctx.Now();

    if (IsStateLoaded()) {
        auto methodType = GetHttpMethodType(*msg);
        auto params = GatherHttpParameters(*msg);
        const auto& action = params.Get("action");

        if (auto* handler = postActions.FindPtr(action)) {
            if (methodType != HTTP_METHOD_POST) {
                RejectHttpRequest(ctx, TabletID(), *requestInfo, "Wrong HTTP method");
                return;
            }

            std::invoke(*handler, this, ctx, params, requestInfo);
            return;
        }

        if (auto* handler = getActions.FindPtr(action)) {
            if (methodType != HTTP_METHOD_GET) {
                RejectHttpRequest(ctx, TabletID(), *requestInfo, "Wrong HTTP method");
                return;
            }

            std::invoke(*handler, this, ctx, params, requestInfo);
            return;
        }

        if (action) {
            RejectHttpRequest(ctx, TabletID(), *requestInfo, "Wrong action: " + action);
            return;
        }

        HandleHttpInfo_Default(ctx, params, requestInfo);
        return;
    }

    TStringStream out;
    DumpTabletNotReady(out, *Info());

    NCloud::Reply(
        ctx,
        *requestInfo,
        std::make_unique<NMon::TEvRemoteHttpInfoRes>(std::move(out.Str())));
}

void TIndexTabletActor::HandleHttpInfo_Default(
    const NActors::TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    Y_UNUSED(params);
    TStringStream out;

    NXml::TDocument data("root", NXml::TDocument::RootName);
    
    auto root = data.Root();
    root.AddChild("tablet_id", TabletID());
    root.AddChild("node_id", SelfId().NodeId());
    root.AddChild("header_hostname", HostName());
    root.AddChild("fs_id", GetFileSystemId());
    root.AddChild("block_size", GetBlockSize());
    out << GetBlocksCount() << " (" <<
            FormatByteSize(GetBlocksCount() * GetBlockSize()) << ")";
    root.AddChild("blocks_count", out.Str());
    out.Clear();
    root.AddChild("tablet_host", FQDNHostName());
    const auto& shardIds = GetFileSystem().GetShardFileSystemIds();
    if (shardIds.size()) {
        NCloud::NStorage::NTNodeWrapper::TFieldAdder shards = root.AddChild("shards", " ");
        ui32 shardNo = 0;
        for (const auto& shardId: shardIds) {
            shards.AddFieldIn("cd", " ")("shard_no", ++shardNo)("shard_id", shardId);
        }
    }
    root.AddChild("curr_commit_id", GetCurrentCommitId());
    {
        auto ops_state = root.AddChild("ops_state", " ");
        DumpOperationState("Flush", FlushState, ops_state);
        DumpOperationState("BlobIndexOp", BlobIndexOpState, ops_state);
        DumpOperationState("CollectGarbage", CollectGarbageState, ops_state);
    }
    DumpStats(out);
    root.AddChild("stats", out.Str());
    out.Clear();
    {
        const ui32 topSize = FromStringWithDefault(params.Get("top-size"), 1);
        DumpCompactionMap(root, GetCompactionMapStats(topSize));
    }
    const auto backpressureThresholds = BuildBackpressureThresholds();
    const auto backpressureValues = GetBackpressureValues();
    TString message;
    bool isWriteAllowed = IsWriteAllowed(
        backpressureThresholds,
        backpressureValues,
        &message);
    
    if (isWriteAllowed) {
        root.AddChild("write");
    } else {
        root.AddChild("write_msg", message);
    }
    root.AddChild("backpressure_period_start", BackpressurePeriodStart);
    root.AddChild("backpressure_error_count", BackpressureErrorCount);
    root.AddChild("backpressure_period", ctx.Now() - BackpressurePeriodStart);

    {
        NCloud::NStorage::NTNodeWrapper::TFieldAdder backpressure = root.AddChild("backpressure", " ");

#define DUMP_BACKPRESSURE_FIELD(name)                                   \
        backpressure.AddFieldIn("cd", " ")                              \
            ("name", #name)                                             \
            ("value", backpressureValues.name)                          \
            ("threshold", backpressureThresholds.name);                 \
// DUMP_BACKPRESSURE_FIELD

        DUMP_BACKPRESSURE_FIELD(Flush);
        DUMP_BACKPRESSURE_FIELD(FlushBytes);
        DUMP_BACKPRESSURE_FIELD(CompactionScore);
        DUMP_BACKPRESSURE_FIELD(CleanupScore);

#undef DUMP_BACKPRESSURE_FIELD
    }

    {
        const auto compactionInfo = GetCompactionInfo();
        NCloud::NStorage::NTNodeWrapper::TFieldAdder compaction = root.AddChild("compaction", " ");

        const auto cleanupInfo = GetCleanupInfo();
        NCloud::NStorage::NTNodeWrapper::TFieldAdder cleanup = root.AddChild("cleanup", " ");

#define DUMP_INFO_FIELD(adder, info, name)                                      \
        adder.AddFieldIn("cd", " ")("name", #name)("value", info.name);           \
// DUMP_INFO_FIELD

        DUMP_INFO_FIELD(compaction, compactionInfo, Threshold);
        DUMP_INFO_FIELD(compaction, compactionInfo, ThresholdAverage);
        DUMP_INFO_FIELD(compaction, compactionInfo, GarbageThreshold);
        DUMP_INFO_FIELD(compaction, compactionInfo, GarbageThresholdAverage);
        DUMP_INFO_FIELD(compaction, compactionInfo, Score);
        DUMP_INFO_FIELD(compaction, compactionInfo, RangeId);
        DUMP_INFO_FIELD(compaction, compactionInfo, GarbagePercentage);
        DUMP_INFO_FIELD(compaction, compactionInfo, AverageScore);
        DUMP_INFO_FIELD(compaction, compactionInfo, NewCompactionEnabled);
        DUMP_INFO_FIELD(compaction, compactionInfo, ShouldCompact);

        DUMP_INFO_FIELD(cleanup, cleanupInfo, Threshold);
        DUMP_INFO_FIELD(cleanup, cleanupInfo, ThresholdAverage);
        DUMP_INFO_FIELD(cleanup, cleanupInfo, Score);
        DUMP_INFO_FIELD(cleanup, cleanupInfo, RangeId);
        DUMP_INFO_FIELD(cleanup, cleanupInfo, AverageScore);
        DUMP_INFO_FIELD(cleanup, cleanupInfo, LargeDeletionMarkersThreshold);
        DUMP_INFO_FIELD(cleanup, cleanupInfo, LargeDeletionMarkerCount);
        DUMP_INFO_FIELD(cleanup, cleanupInfo, PriorityRangeIdCount);
        DUMP_INFO_FIELD(cleanup, cleanupInfo, IsPriority);
        DUMP_INFO_FIELD(cleanup, cleanupInfo, NewCleanupEnabled);
        DUMP_INFO_FIELD(cleanup, cleanupInfo, ShouldCleanup);

#undef DUMP_INFO_FIELD
    }

    if (IsForcedRangeOperationRunning()) {
        root.AddChild("forced_op");
        auto state = GetForcedRangeOperationState();
        ui32 curr = state->Current, max = state->RangesToCompact.size();
        root.AddChild("curr_compact", curr);
        root.AddChild("max_compact", max);
        root.AddChild("compact_percents", (curr * 100) / max);
    }

    ui64 hiveTabletId = Config->GetTenantHiveTabletId();
    if (!hiveTabletId) {
        hiveTabletId = NCloud::NStorage::GetHiveTabletId(ctx);
    }

    DumpChannels(
        root,
        MakeChannelMonInfos(),
        *Info(),
        hiveTabletId);

    DumpProfillingAllocatorStats(GetFileStoreProfilingRegistry(), root);

    const auto storageThrottlingEnabled = Config->GetThrottlingEnabled();
    const auto& fsPerfProfile = GetFileSystem().GetPerformanceProfile();
    {
        auto profileStats = root.AddChild("profile_stats", " ");
        DumpPerformanceProfile(storageThrottlingEnabled, fsPerfProfile, profileStats);
    }
    const auto& usedPerfProfile = GetPerformanceProfile();
    if (!NProtoBuf::IsEqual(fsPerfProfile, usedPerfProfile)) {
        auto userProfileStats = root.AddChild("used_profile_stats", " ");
        DumpPerformanceProfile(
            storageThrottlingEnabled,
            usedPerfProfile,
            userProfileStats
        );
    }
    DumpThrottlingState(Throttler.get(), GetThrottlingPolicy(), root);

    if (StorageConfigOverride.ByteSize()) {
        auto storageConfig = root.AddChild("storage_config", " ");
        TStorageConfig config(StorageConfigOverride);
        config.DumpOverridesXml(storageConfig);
    }
    DumpSessions(root, GetActiveSessions());
    DumpSessionHistory(root, GetSessionHistoryList());

    NCloud::NStorage::NXSLRender::NXSLRender(xslTemplate, data, out);

    NCloud::Reply(
        ctx,
        *requestInfo,
        std::make_unique<NMon::TEvRemoteHttpInfoRes>(std::move(out.Str())));
}

void TIndexTabletActor::HandleHttpInfo_ForceOperation(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    if (IsForcedRangeOperationRunning()) {
        SendHttpResponse(
            ctx,
            TabletID(),
            *requestInfo,
            "Compaction is already running",
            EAlertLevel::WARNING);
        return;
    }

    TEvIndexTabletPrivate::EForcedRangeOperationMode mode;
    if (params.Get("mode") == "cleanup") {
        mode = TEvIndexTabletPrivate::EForcedRangeOperationMode::Cleanup;
    } else if (params.Get("mode") == "compaction") {
        mode = TEvIndexTabletPrivate::EForcedRangeOperationMode::Compaction;
    } else if (params.Get("mode") == "deleteZeroCompactionRanges") {
        mode = TEvIndexTabletPrivate::EForcedRangeOperationMode
            ::DeleteZeroCompactionRanges;
    } else {
        RejectHttpRequest(
            ctx,
            TabletID(),
            *requestInfo,
            TStringBuilder() << "Invalid mode: " << params.Get("mode"));
        return;
    }

    TVector<ui32> ranges;
    if (params.Has("RangeIndex") && params.Has("RangesCount")) {
        ui64 rangeIndex = 0;
        if (const auto& param = params.Get("RangeIndex"); !TryFromString(param, rangeIndex)) {
            RejectHttpRequest(ctx, TabletID(), *requestInfo, "Invalid range index");
            return;
        }

        ui32 rangesCount = Max<ui32>();
        if (const auto& param = params.Get("RangesCount"); !TryFromString(param, rangesCount)) {
            RejectHttpRequest(ctx, TabletID(), *requestInfo, "Invalid range count");
            return;
        }

        ranges = TVector<ui32>(
            ::xrange(
                rangeIndex,
                rangeIndex + rangesCount,
                1));
    } else {
        if (mode == TEvIndexTabletPrivate::EForcedRangeOperationMode
                ::DeleteZeroCompactionRanges)
        {
            ranges = GenerateForceDeleteZeroCompactionRanges();
        } else {
            ranges = GetNonEmptyCompactionRanges();
        }
    }

    EnqueueForcedRangeOperation(mode, std::move(ranges));
    EnqueueForcedRangeOperationIfNeeded(ctx);

    SendHttpResponse(
        ctx,
        TabletID(),
        *requestInfo,
        "Compaction has been started",
        EAlertLevel::SUCCESS);
}

void TIndexTabletActor::HandleHttpInfo_DumpCompactionRange(
    const TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    if (!params.Has("rangeId")) {
        RejectHttpRequest(ctx, TabletID(), *requestInfo, "You should specify rangeId");
        return;
    }

    ui32 rangeId = 0;
    if (const auto& param = params.Get("rangeId"); !TryFromString(param, rangeId)) {
        RejectHttpRequest(ctx, TabletID(), *requestInfo, "Invalid rangeId");
        return;
    }

    FILESTORE_TRACK(
        BackgroundTaskStarted_Tablet,
        requestInfo->CallContext,
        "DumpCompactionRange",
        requestInfo->CallContext->FileSystemId,
        GetFileSystem().GetStorageMediaKind());

    using TRequest = TEvIndexTabletPrivate::TEvDumpCompactionRangeRequest;
    auto request = std::make_unique<TRequest>(
        rangeId);
    request->CallContext = requestInfo->CallContext;

    auto actor = std::make_unique<TIndexTabletMonitoringActor<TRequest>>(
        std::move(requestInfo),
        ctx.SelfID,
        TabletID(),
        std::move(request));

    auto actorId = ctx.Register(actor.release());
    WorkerActors.insert(actorId);
}

}   // namespace NCloud::NFileStore::NStorage
