#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/storage/tablet/tablet_state.h>

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/kikimr/tenant.h>
#include <cloud/storage/core/libs/throttling/tablet_throttler.h>
#include <cloud/storage/core/libs/viewer/tablet_monitoring.h>

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

    out << "<p><a href='' data-toggle='modal' "
           "data-target='#force-delete-zero-compaction-ranges'>Force Delete zero compaction ranges</a></p>"
        << "<form method='POST' name='ForceDeleteZeroCompactionRanges' style='display:none'>"
        << "<input type='hidden' name='TabletID' value='" << tabletId << "'/>"
        << "<input type='hidden' name='action' value='forceOperationAll'/>"
        << "<input type='hidden' name='mode' value='deleteZeroCompactionRanges'/>"
        << "<input class='btn btn-primary' type='button' value='Delete zero "
           "compaction ranges'"
        << " data-toggle='modal' data-target='#force-delete-zero-compaction-ranges'/>"
        << "</form>";

    BuildConfirmActionDialog(
        out,
        "force-delete-zero-compaction-ranges",
        "Force delete zero compaction ranges",
        "Are you sure you want to delete ALL zero compaction ranges?",
        "forceDeleteZeroCompactionRanges();");
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
    DumpProgress(out, state.Current, state.RangesToCompact.size());
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

        TAG(TH4) {
            out << "Top ranges by garbage score";
        }
        DumpCompactionRangeInfo(out, tabletId, stats.TopRangesByGarbageScore);
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

/**
 * @brief Display sessions the tablet knows
 */
void DumpSessions(
    IOutputStream& out,
    const TVector<TMonSessionInfo>& sessions,
    bool dumpSessionRegardlessOfSubSessions)
{
    HTML(out) {
        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "ClientId";}
                    TABLEH() { out << "FQDN";}
                    TABLEH() { out << "SessionId"; }
                    TABLEH() { out << "Recovery"; }
                    TABLEH() { out << "SeqNo"; }
                    TABLEH() { out << "ReadOnly"; }
                    TABLEH() { out << "Owner"; }
                    TABLEH() { out << "Deadline"; }
                }
            }
            for (const auto& session: sessions) {
                const auto recoveryTimestamp = TInstant::MicroSeconds(
                    session.ProtoInfo.GetRecoveryTimestampUs());

                auto dumpSubSession =
                    [&](IOutputStream& out, const TSubSession& ss)
                {
                    TABLER() {
                        TABLED() { out << session.ProtoInfo.GetClientId(); }
                        TABLED() { out << session.ProtoInfo.GetOriginFqdn(); }
                        TABLED() { out << session.ProtoInfo.GetSessionId(); }
                        if (recoveryTimestamp) {
                            TABLED() { out << recoveryTimestamp; }
                        } else {
                            TABLED() { out << ""; }
                        }
                        TABLED() { out << ss.SeqNo; }
                        TABLED() { out << (ss.ReadOnly ? "True" : "False"); }
                        TABLED() { out << ToString(ss.Owner); }
                        TABLED() { out << session.Deadline.ToString(); }
                    }
                };
                if (session.SubSessions.empty()) {
                    if (dumpSessionRegardlessOfSubSessions) {
                        dumpSubSession(out, TSubSession{});
                    }
                } else {
                    for (const auto& ss: session.SubSessions) {
                        dumpSubSession(out, ss);
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void DumpChannels(
    IOutputStream& out,
    const TVector<NCloud::NStorage::TChannelMonInfo>& channelInfos,
    const TTabletStorageInfo& storage,
    ui64 hiveTabletId)
{
    NCloud::NStorage::DumpChannels(
        out,
        channelInfos,
        storage,
        [&] (ui32 groupId, const TString& storagePool) {
            // TODO: group mon url
            Y_UNUSED(groupId);
            Y_UNUSED(storagePool);
            return TString();
        },
        [&] (ui32 groupId) {
            // TODO: dashboard url
            Y_UNUSED(groupId);
            return TString();
        },
        [&] (IOutputStream& out, ui64 hiveTabletId, ui64 tabletId, ui32 c) {
            // TODO: reassign button
            Y_UNUSED(out);
            Y_UNUSED(hiveTabletId);
            Y_UNUSED(tabletId);
            Y_UNUSED(c);
        },
        hiveTabletId);
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

    out << R"___(
        <script type='text/javascript'>
        function forceDeleteZeroCompactionRanges() {
            document.forms['ForceDeleteZeroCompactionRanges'].submit();
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

        const auto& shardIds = GetFileSystem().GetShardFileSystemIds();
        if (shardIds.size()) {
            TAG(TH3) { out << "Shards"; }
            TABLE_SORTABLE_CLASS("table table-bordered") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "ShardNo"; }
                        TABLEH() { out << "FileSystemId"; }
                        TABLEH() { out << "UsedBytesCount"; }
                        TABLEH() { out << "FreeBytesCount"; }
                        TABLEH() { out << "CurrentLoad"; }
                        TABLEH() { out << "Suffer"; }
                    }
                }

                ui32 shardNo = 0;
                for (const auto& shardId: shardIds) {
                    TShardStats ss;
                    if (shardNo < CachedShardStats.size()) {
                        ss = CachedShardStats[shardNo];
                    }
                    TABLER() {
                        TABLED() { out << ++shardNo; }
                        TABLED() {
                            out << "<a href='../filestore/service?action=search"
                                << "&Filesystem=" << shardId << "'>"
                                << shardId << "</a>";
                        }
                        TABLED() {
                            out << ss.UsedBlocksCount * GetBlockSize();
                        }
                        TABLED() {
                            out << (ss.TotalBlocksCount - ss.UsedBlocksCount)
                                * GetBlockSize();
                        }
                        TABLED() { out << ss.CurrentLoad; }
                        TABLED() { out << ss.Suffer; }
                    }
                }
            }
        }

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

        const auto backpressureThresholds = BuildBackpressureThresholds();
        const auto backpressureValues = GetBackpressureValues();
        TString message;
        bool isWriteAllowed = IsWriteAllowed(
            backpressureThresholds,
            backpressureValues,
            &message);
        TAG(TH3) { out << "Backpressure"; }
        if (!isWriteAllowed) {
            DIV_CLASS("alert alert-danger") {
                out << "Write NOT allowed: " << message;
            }
        } else {
            DIV_CLASS("alert") {
                out << "Write allowed";
            }
        }
        if (BackpressurePeriodStart || BackpressureErrorCount) {
            DIV_CLASS("alert") {
                out << "Backpressure errors: " << BackpressureErrorCount;
            }
            DIV_CLASS("alert") {
                out << "Backpressure period start: " << BackpressurePeriodStart;
            }
            DIV_CLASS("alert") {
                out << "Backpressure period: "
                    << (ctx.Now() - BackpressurePeriodStart);
            }
        }

        TABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Name"; }
                    TABLEH() { out << "Value"; }
                    TABLEH() { out << "Threshold"; }
                }
            }
#define DUMP_BACKPRESSURE_FIELD(name)                                          \
            TABLER() {                                                         \
                TABLED() { out << #name; }                                     \
                TABLED() { out << backpressureValues.name; }                   \
                TABLED() { out << backpressureThresholds.name; }               \
            }                                                                  \
// DUMP_BACKPRESSURE_FIELD

            DUMP_BACKPRESSURE_FIELD(Flush);
            DUMP_BACKPRESSURE_FIELD(FlushBytes);
            DUMP_BACKPRESSURE_FIELD(CompactionScore);
            DUMP_BACKPRESSURE_FIELD(CleanupScore);

#undef DUMP_BACKPRESSURE_FIELD
        }

#define DUMP_INFO_FIELD(info, name)                                            \
        TABLER() {                                                             \
            TABLED() { out << #name; }                                         \
            TABLED() { out << info.name; }                                     \
        }                                                                      \
// DUMP_INFO_FIELD

        TAG(TH3) { out << "CompactionInfo"; }
        TABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Parameter"; }
                    TABLEH() { out << "Value"; }
                }
            }

            const auto compactionInfo = GetCompactionInfo();
            DUMP_INFO_FIELD(compactionInfo, Threshold);
            DUMP_INFO_FIELD(compactionInfo, ThresholdAverage);
            DUMP_INFO_FIELD(compactionInfo, GarbageThreshold);
            DUMP_INFO_FIELD(compactionInfo, GarbageThresholdAverage);
            DUMP_INFO_FIELD(compactionInfo, Score);
            DUMP_INFO_FIELD(compactionInfo, RangeId);
            DUMP_INFO_FIELD(compactionInfo, GarbagePercentage);
            DUMP_INFO_FIELD(compactionInfo, AverageScore);
            DUMP_INFO_FIELD(compactionInfo, NewCompactionEnabled);
            DUMP_INFO_FIELD(compactionInfo, ShouldCompact);
        }

        TAG(TH3) { out << "CleanupInfo"; }
        TABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Parameter"; }
                    TABLEH() { out << "Value"; }
                }
            }

            const auto cleanupInfo = GetCleanupInfo();
            DUMP_INFO_FIELD(cleanupInfo, Threshold);
            DUMP_INFO_FIELD(cleanupInfo, ThresholdAverage);
            DUMP_INFO_FIELD(cleanupInfo, Score);
            DUMP_INFO_FIELD(cleanupInfo, RangeId);
            DUMP_INFO_FIELD(cleanupInfo, AverageScore);
            DUMP_INFO_FIELD(cleanupInfo, LargeDeletionMarkersThreshold);
            DUMP_INFO_FIELD(cleanupInfo, LargeDeletionMarkerCount);
            DUMP_INFO_FIELD(cleanupInfo, PriorityRangeIdCount);
            DUMP_INFO_FIELD(cleanupInfo, IsPriority);
            DUMP_INFO_FIELD(cleanupInfo, NewCleanupEnabled);
            DUMP_INFO_FIELD(cleanupInfo, ShouldCleanup);
        }

#undef DUMP_INFO_FIELD

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

        TAG(TH3) {
            out << "Channels";
        }

        ui64 hiveTabletId = Config->GetTenantHiveTabletId();
        if (!hiveTabletId) {
            hiveTabletId = NCloud::NStorage::GetHiveTabletId(ctx);
        }

        DumpChannels(
            out,
            MakeChannelMonInfos(),
            *Info(),
            hiveTabletId);

        TAG(TH3) { out << "Profiling allocator stats"; }
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
            out << "Active Sessions";
        }
        DumpSessions(out, GetActiveSessionInfos(), false);

        TAG(TH3)
        {
            out << "Orphan Sessions";
        }
        DumpSessions(out, GetOrphanSessionInfos(), true);

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
