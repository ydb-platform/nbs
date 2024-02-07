#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/storage/tablet/tablet_state.h>

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/throttling/tablet_throttler.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/util/is_equal.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/xrange.h>
#include <util/stream/str.h>
#include <util/string/join.h>
#include <util/system/hostname.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NActors::NMon;
using namespace NKikimr;

namespace {

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

[[maybe_unused]] TCgiParameters GetHttpMethodParameters(const TEvRemoteHttpInfo& msg)
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

void BuildMenuButton(IOutputStream& out, const TString& menuItems)
{
    out << "<span class='glyphicon glyphicon-list'"
        << " data-toggle='collapse' data-target='#"
        << menuItems << "' style='padding-right: 5px'>"
        << "</span>";
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

void BuildConfirmActionDialog(
    IOutputStream& out,
    const TString& id,
    const TString& title,
    const TString& message,
    const TString& onClickScript)
{
    out << "<div class='modal fade' id='" << id << "' role='dialog'>";
    out << R"___(
                <div class='modal-dialog'>
                    <div class='modal-content'>
                        <div class='modal-header'>
                            <button type='button' class='close' data-dismiss='modal'>&times;</button>
                            <h4 class='modal-title'>
            )___";

    out << title;

    out << R"___(
                            </h4>
                        </div>
                        <div class='modal-body'>
                            <div class='row'>
                                <div class='col-sm-6 col-md-6'>
            )___";

    out << message;

    out << R"___(
                                </div>
                            </div>
                        </div>
                        <div class='modal-footer'>
                            <button type='submit' class='btn btn-default' data-dismiss='modal' onclick='
            )___";

    out << onClickScript << "'>Confirm</button>";

    out << R"___(
                            <button type='button' class='btn btn-default' data-dismiss='modal'>Cancel</button>
                        </div>
                    </div>
                </div>
            </div>
            )___";
}

void BuildForceCompactionButton(IOutputStream& out, ui64 tabletId)
{
    out << "<p><a href='' data-toggle='modal' data-target='#force-compaction'>Force Full Compaction</a></p>"
        << "<form method='POST' name='ForceCompaction' style='display:none'>"
        << "<input type='hidden' name='TabletID' value='" << tabletId << "'/>"
        << "<input type='hidden' name='action' value='forceOperationAll'/>"
        << "<input type='hidden' name='mode' value='compaction'/>"
        << "<input class='btn btn-primary' type='button' value='Compact ALL ranges'"
        << " data-toggle='modal' data-target='#force-compaction'/>"
        << "</form>";

    BuildConfirmActionDialog(
        out,
        "force-compaction",
        "Force compaction",
        "Are you sure you want to force compaction for ALL ranges?",
        "forceCompactionAll();");

    out << "<p><a href='' data-toggle='modal' "
           "data-target='#force-cleanup'>Force Full Cleanup</a></p>"
        << "<form method='POST' name='ForceCleanup' style='display:none'>"
        << "<input type='hidden' name='TabletID' value='" << tabletId << "'/>"
        << "<input type='hidden' name='action' value='forceOperationAll'/>"
        << "<input type='hidden' name='mode' value='cleanup'/>"
        << "<input class='btn btn-primary' type='button' value='Cleanup ALL "
           "ranges'"
        << " data-toggle='modal' data-target='#force-cleanup'/>"
        << "</form>";

    BuildConfirmActionDialog(
        out,
        "force-cleanup",
        "Force cleanup",
        "Are you sure you want to force cleanup for ALL ranges?",
        "forceCleanupAll();");
}

////////////////////////////////////////////////////////////////////////////////

void DumpProgress(IOutputStream& out, ui64 progress, ui64 total)
{
    HTML(out) {
        DIV_CLASS("progress") {
            ui32 percents = (progress * 100 / total);
            out << "<div class='progress-bar' role='progressbar' aria-valuemin='0'"
                << " style='width: " << percents << "%'"
                << " aria-valuenow='" << progress
                << "' aria-valuemax='" << total << "'>"
                << percents << "%</div>";
        }
        out << progress << " of " << total;
    }
}

////////////////////////////////////////////////////////////////////////////////

void DumpDefaultHeader(
    IOutputStream& out,
    ui64 tabletId,
    ui32 nodeId)
{
    TString hostname = HostName();

    HTML(out) {
        TAG(TH3) {
            out << "Tablet <a href='../tablets?TabletID=" << tabletId
                << "'>" << tabletId << "</a>"
                << " running on node " << hostname << "[" << nodeId << "]"
                ;
        }
    }
}

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
    IOutputStream& out)
{
    out << opName
        << " state: " << static_cast<ui32>(state.GetOperationState())
        << ", Timestamp: " << state.GetStateChanged()
        << ", Completed: " << state.GetCompleted()
        << ", Failed: " << state.GetFailed()
        << ", Backoff: " << state.GetBackoffTimeout().ToString();
}

////////////////////////////////////////////////////////////////////////////////

void DumpCompactionInfo(
    IOutputStream& out,
    const TIndexTabletState::TForcedRangeOperationState& state)
{
    DumpProgress(out, state.Current.load(), state.RangesToCompact.size());
}

void DumpRangeId(IOutputStream& out, ui64 tabletId, ui32 rangeId)
{
    HTML(out) {
        DIV_CLASS("col-lg-9") {
            out << "<a href='../tablets/app?TabletID=" << tabletId
                << "&action=dumpRange&rangeId=" << rangeId
                << "'>" << rangeId << "</a>";
        }
    }
}

void DumpCompactionRangeInfo(
    IOutputStream& out,
    ui64 tabletId,
    const TVector<TCompactionRangeInfo>& ranges)
{
    HTML(out) {
        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Range"; }
                    TABLEH() { out << "Blobs"; }
                    TABLEH() { out << "Deletions"; }
                }
            }

            for (const auto& range: ranges) {
                TABLER() {
                    TABLED() { DumpRangeId(out, tabletId, range.RangeId); }
                    TABLED() { out << range.Stats.BlobsCount; }
                    TABLED() { out << range.Stats.DeletionsCount; }
                }
            }
        }
    }
}

void DumpCompactionMap(
    IOutputStream& out,
    ui64 tabletId,
    const TCompactionMapStats& stats)
{
    HTML(out) {
        TABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Used ranges count"; }
                    TABLEH() { out << "Allocated ranges count"; }
                    TABLEH() { out << "Compaction map density"; }
                }
            }

            TABLER() {
                TABLED() { out << stats.UsedRangesCount; }
                TABLED() { out << stats.AllocatedRangesCount; }
                TABLED() {
                    DIV_CLASS("progress") {
                        ui32 percents = 0;
                        if (stats.AllocatedRangesCount) {
                            percents = (stats.UsedRangesCount * 100 / stats.AllocatedRangesCount);
                        }

                        out << "<div class='progress-bar' role='progressbar' aria-valuemin='0'"
                            << " style='width: " << percents << "%'"
                            << " aria-valuenow='" << stats.UsedRangesCount
                            << "' aria-valuemax='" << stats.AllocatedRangesCount << "'>"
                            << percents << "%</div>";
                    }
                }
            }
        }

        TAG(TH4) {
            out << "Top ranges by compaction score";
        }
        DumpCompactionRangeInfo(out, tabletId, stats.TopRangesByCompactionScore);

        TAG(TH4) {
            out << "Top ranges by cleanup score";
        }
        DumpCompactionRangeInfo(out, tabletId, stats.TopRangesByCleanupScore);
    }
}

////////////////////////////////////////////////////////////////////////////////

void DumpProfillingAllocatorStats(
    const TFileStoreAllocRegistry& registry,
    IOutputStream& out)
{
    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                ui64 allBytes = 0;
                for (ui32 i = 0; i < static_cast<ui32>(EAllocatorTag::Max); ++i) {
                    EAllocatorTag tag = static_cast<EAllocatorTag>(i);
                    const ui64 bytes = registry.GetAllocator(tag)->GetBytesAllocated();
                    TABLER() {
                        TABLED() { out << tag; }
                        TABLED() { out << FormatByteSize(bytes); }
                    }
                    allBytes += bytes;
                }
                TABLER() {
                    TABLED() { out << "Summary"; }
                    TABLED() { out << FormatByteSize(allBytes); }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void DumpPerformanceProfile(
    bool storageThrottlingEnabled,
    const NProto::TFileStorePerformanceProfile& profile,
    IOutputStream& out)
{
    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                TABLER() {
                    TABLED() { out << "StorageThrottlingEnabled"; }
                    TABLED() { out << storageThrottlingEnabled; }
                }
                TABLER() {
                    TABLED() { out << "ThrottlingEnabled"; }
                    TABLED() { out << profile.GetThrottlingEnabled(); }
                }
                TABLER() {
                    TABLED() { out << "MaxReadIops"; }
                    TABLED() { out << profile.GetMaxReadIops(); }
                }
                TABLER() {
                    TABLED() { out << "MaxWriteIops"; }
                    TABLED() { out << profile.GetMaxWriteIops(); }
                }
                TABLER() {
                    TABLED() { out << "MaxReadBandwidth"; }
                    TABLED() { out << profile.GetMaxReadBandwidth(); }
                }
                TABLER() {
                    TABLED() { out << "MaxWriteBandwidth"; }
                    TABLED() { out << profile.GetMaxWriteBandwidth(); }
                }
                TABLER() {
                    TABLED() { out << "BoostTime"; }
                    TABLED() { out << profile.GetBoostTime(); }
                }
                TABLER() {
                    TABLED() { out << "BoostRefillTime"; }
                    TABLED() { out << profile.GetBoostRefillTime(); }
                }
                TABLER() {
                    TABLED() { out << "BoostPercentage"; }
                    TABLED() { out << profile.GetBoostPercentage(); }
                }
                TABLER() {
                    TABLED() { out << "BurstPercentage"; }
                    TABLED() { out << profile.GetBurstPercentage(); }
                }
                TABLER() {
                    TABLED() { out << "DefaultPostponedRequestWeight"; }
                    TABLED() { out << profile.GetDefaultPostponedRequestWeight(); }
                }
                TABLER() {
                    TABLED() { out << "MaxPostponedWeight"; }
                    TABLED() { out << profile.GetMaxPostponedWeight(); }
                }
                TABLER() {
                    TABLED() { out << "MaxWriteCostMultiplier"; }
                    TABLED() { out << profile.GetMaxWriteCostMultiplier(); }
                }
                TABLER() {
                    TABLED() { out << "MaxPostponedTime"; }
                    TABLED() { out << profile.GetMaxPostponedTime(); }
                }
                TABLER() {
                    TABLED() { out << "MaxPostponedCount"; }
                    TABLED() { out << profile.GetMaxPostponedCount(); }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void DumpThrottlingState(
    const ITabletThrottler* throttler,
    const TThrottlingPolicy& policy,
    IOutputStream& out)
{
    const auto& config = policy.GetConfig();
    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                TABLER() {
                    TABLED() { out << "Throttling enabled"; }
                    TABLED() { out << config.ThrottlingEnabled; }
                }
                TABLER() {
                    TABLED() { out << "Config version"; }
                    TABLED() { out << policy.GetVersion(); }
                }
                {
                    // Default parameters.
                    const auto& p = config.DefaultParameters;
                    TABLER() {
                        TABLED() { out << "MaxReadIops"; }
                        TABLED() { out << p.MaxReadIops; }
                    }
                    TABLER() {
                        TABLED() { out << "MaxWriteIops"; }
                        TABLED() { out << p.MaxWriteIops; }
                    }
                    TABLER() {
                        TABLED() { out << "MaxReadBandwidth"; }
                        TABLED() { out << p.MaxReadBandwidth; }
                    }
                    TABLER() {
                        TABLED() { out << "MaxWriteBandwidth"; }
                        TABLED() { out << p.MaxWriteBandwidth; }
                    }
                }
                {
                    // Boost parameters.
                    const auto& p = config.BoostParameters;
                    TABLER() {
                        TABLED() { out << "BoostTime"; }
                        TABLED() { out << p.BoostTime.MilliSeconds(); }
                    }
                    TABLER() {
                        TABLED() { out << "BoostRefillTime"; }
                        TABLED() { out << p.BoostRefillTime.MilliSeconds(); }
                    }
                    TABLER() {
                        TABLED() { out << "BoostPercentage"; }
                        TABLED() { out << p.BoostPercentage; }
                    }
                }
                TABLER() {
                    TABLED() { out << "BurstPercentage"; }
                    TABLED() { out << config.BurstPercentage; }
                }
                TABLER() {
                    TABLED() { out << "DefaultPostponedRequestWeight"; }
                    TABLED() { out << config.DefaultPostponedRequestWeight; }
                }
                {
                    // Default limits.
                    const auto& l = config.DefaultThresholds;
                    TABLER() {
                        TABLED() { out << "MaxPostponedWeight"; }
                        TABLED() { out << l.MaxPostponedWeight; }
                    }
                    TABLER() {
                        TABLED() { out << "MaxWriteCostMultiplier"; }
                        TABLED() { out << l.MaxWriteCostMultiplier; }
                    }
                    TABLER() {
                        TABLED() { out << "MaxPostponedTime"; }
                        TABLED() { out << l.MaxPostponedTime.MilliSeconds(); }
                    }
                    TABLER() {
                        TABLED() { out << "MaxPostponedCount"; }
                        TABLED() { out << l.MaxPostponedCount; }
                    }
                }
                if (throttler) {
                    TABLER() {
                        TABLED() { out << "PostponedQueueSize"; }
                        TABLED() { out << throttler->GetPostponedRequestsCount(); }
                    }
                }
                TABLER() {
                    TABLED() { out << "PostponedQueueWeight"; }
                    TABLED() { out << policy.CalculatePostponedWeight(); }
                }
                TABLER() {
                    TABLED() { out << "WriteCostMultiplier"; }
                    TABLED() { out << policy.GetWriteCostMultiplier(); }
                }
                TABLER() {
                    TABLED() { out << "CurrentBoostBudget"; }
                    TABLED() { out << policy.GetCurrentBoostBudget(); }
                }
                TABLER() {
                    TABLED() { out << "ThrottlerParams"; }
                    TABLED() {
                        TABLE_CLASS("table table-condensed") {
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() { out << "ReadC1"; }
                                    TABLED() { out << policy.C1(TThrottlingPolicy::EOpType::Read); }
                                }
                                TABLER() {
                                    TABLED() { out << "ReadC2"; }
                                    TABLED() { out << FormatByteSize(policy.C2(TThrottlingPolicy::EOpType::Read)); }
                                }
                                TABLER() {
                                    TABLED() { out << "WriteC1"; }
                                    TABLED() { out << policy.C1(TThrottlingPolicy::EOpType::Write); }
                                }
                                TABLER() {
                                    TABLED() { out << "WriteC2"; }
                                    TABLED() { out << FormatByteSize(policy.C2(TThrottlingPolicy::EOpType::Write)); }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

/**
 * @brief Display last `limit` lines of `sessionHistory`
 */
void DumpSessionHistory(
    IOutputStream& out,
    const TSessionHistoryList& sessionHistory,
    size_t limit = 100)
{
    HTML(out) {
        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "ClientId";}
                    TABLEH() { out << "FQDN"; }
                    TABLEH() { out << "Timestamp"; }
                    TABLEH() { out << "SessionId"; }
                    TABLEH() { out << "ActionType"; }
                }
            }
            for (auto it = sessionHistory.rbegin();
                 it != sessionHistory.rend() && limit;
                 ++it, --limit)
            {
                TABLER() {
                    TABLED() { out << it->GetClientId(); }
                    TABLED()
                    {
                        out << it->GetOriginFqdn();
                    }
                    TABLED() { out << TInstant::MicroSeconds(it->GetTimestampUs()); }
                    TABLED() { out << it->GetSessionId(); }
                    TABLED() { out << it->GetEntryTypeString(); }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void GenerateActionsJS(IOutputStream& out)
{
    out << R"___(
        <script type='text/javascript'>
        function forceCompactionAll() {
            document.forms['ForceCompaction'].submit();
        }
        </script>
    )___";

    out << R"___(
        <script type='text/javascript'>
        function forceCleanupAll() {
            document.forms['ForceCleanup'].submit();
        }
        </script>
    )___";
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
    {
        TThis::ActivityType = TFileStoreComponents::TABLET_WORKER;
    }

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
        HTML(out) {
            DumpDefaultHeader(out, TabletId, Owner.NodeId());
            TAG(TH3) { out << "RangeId: " << msg.RangeId; }
            TABLE_SORTABLE() {
                TABLEHEAD() {
                    TABLER() {
                        TABLED() { out << "# NodeId"; }
                        TABLED() { out << "BlockIndex"; }
                        TABLED() { out << "BlobId"; }
                        TABLED() { out << "MinCommitId"; }
                        TABLED() { out << "MaxCommitId"; }
                    }
                }
                TABLEBODY() {
                    for (const auto& blob: msg.Blobs) {
                        for (const auto& block: blob.Blocks) {
                            TABLER() {
                                TABLED() { out << block.NodeId; }
                                TABLED() { out << block.BlockIndex; }
                                TABLED() { out << blob.BlobId; }
                                TABLED() { out << block.MinCommitId; }
                                TABLED() { out << block.MaxCommitId; }
                            }
                        }
                    }
                }
            }
        }
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"), {});
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
            auto response = std::make_unique<TEvIndexTabletPrivate::TEvDumpCompactionRangeCompleted>();
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
    TStringStream out;

    HTML(out) {
        DumpDefaultHeader(out, TabletID(), SelfId().NodeId());

        TAG(TH3) { out << "Info"; }
        DIV() { out << "Filesystem Id: " << GetFileSystemId(); }
        DIV() { out << "Block size: " << GetBlockSize(); }
        DIV() { out << "Blocks: " << GetBlocksCount() << " (" <<
            FormatByteSize(GetBlocksCount() * GetBlockSize()) << ")";
        }
        DIV() { out << "Tablet host: " << FQDNHostName(); }

        TAG(TH3) { out << "State"; }
        DIV() { out << "Current commitId: " << GetCurrentCommitId(); }
        DIV() { DumpOperationState("Flush", FlushState, out); }
        DIV() { DumpOperationState("BlobIndexOp", BlobIndexOpState, out); }
        DIV() { DumpOperationState("CollectGarbage", CollectGarbageState, out); }

        TAG(TH3) { out << "Stats"; }
        PRE() { DumpStats(out); }

        const ui32 topSize = FromStringWithDefault(params.Get("top-size"), 1);
        TAG(TH3) { out << "CompactionMap"; }
        DumpCompactionMap(out, TabletID(), GetCompactionMapStats(topSize));

        TAG(TH3) {
            if (!IsForcedRangeOperationRunning()) {
                BuildMenuButton(out, "compact-all");
            }
            out << "CompactionQueue";
        }

        if (IsForcedRangeOperationRunning()) {
            DumpCompactionInfo(out, *GetForcedRangeOperationState());
        } else {
            out << "<div class='collapse form-group' id='compact-all'>";
            BuildForceCompactionButton(out, TabletID());
            out << "</div>";
        }

        TAG(TH3) { out << "Profilling allocator stats"; }
        DumpProfillingAllocatorStats(GetFileStoreProfilingRegistry(), out);

        TAG(TH3) { out << "Blob index stats"; }

        const auto storageThrottlingEnabled = Config->GetThrottlingEnabled();

        const auto& fsPerfProfile = GetFileSystem().GetPerformanceProfile();
        TAG(TH3) { out << "Performance profile"; }
        DumpPerformanceProfile(storageThrottlingEnabled, fsPerfProfile, out);

        const auto& usedPerfProfile = GetPerformanceProfile();
        if (!NProtoBuf::IsEqual(fsPerfProfile, usedPerfProfile)) {
            TAG(TH3) { out << "Used performance profile"; }
            DumpPerformanceProfile(
                storageThrottlingEnabled,
                usedPerfProfile,
                out
            );
        }

        TAG(TH3) { out << "Throttler state"; }
        DumpThrottlingState(Throttler.get(), GetThrottlingPolicy(), out);

        if (StorageConfigOverride.ByteSize()) {
            TAG(TH3) { out << "StorageConfig overrides"; }
            TStorageConfig config(StorageConfigOverride);
            config.DumpOverridesHtml(out);
        }

        TAG(TH3)
        {
            out << "Session history";
        }
        DumpSessionHistory(out, GetSessionHistoryList());

        GenerateActionsJS(out);
    }

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
        ranges = GetNonEmptyCompactionRanges();
    }

    TEvIndexTabletPrivate::EForcedRangeOperationMode mode =
        params.Get("mode") == "cleanup"
            ? TEvIndexTabletPrivate::EForcedRangeOperationMode::Cleanup
            : TEvIndexTabletPrivate::EForcedRangeOperationMode::Compaction;

    EnqueueForcedRangeOperation(std::move(ranges), mode);
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
