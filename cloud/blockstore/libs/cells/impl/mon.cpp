#include "mon.h"

#include <cloud/blockstore/libs/cells/iface/inbound_activity.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/hostname.h>
#include <cloud/blockstore/libs/kikimr/components.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/mon/mon.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/mon.h>

#include <library/cpp/html/pcdata/pcdata.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/generic/utility.h>
#include <util/string/builder.h>
#include <util/stream/str.h>

namespace NCloud::NBlockStore::NCells {

using namespace NActors;
using namespace NKikimr;
using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

// the mon disk search never waits longer than this, and falls back to it when
// no describe timeout is configured - a zero would fire the result timer at
// once while the describes kept running on their own (grpc) default
constexpr TDuration MonitoringSearchTimeout = TDuration::Seconds(10);

////////////////////////////////////////////////////////////////////////////////

struct TEvPrivate
{
    enum EEv
    {
        EvSearchCompleted = TBlockStorePrivateEvents::CELLS_START,
        EvEnd
    };

    struct TEvSearchCompleted
        : public TEventLocal<TEvSearchCompleted, EvSearchCompleted>
    {
        const TActorId ReplyTo;
        const ui64 Cookie;
        const TString DiskId;
        const TVector<TCellDescribeResult> Results;

        TEvSearchCompleted(
                TActorId replyTo,
                ui64 cookie,
                TString diskId,
                TVector<TCellDescribeResult> results)
            : ReplyTo(replyTo)
            , Cookie(cookie)
            , DiskId(std::move(diskId))
            , Results(std::move(results))
        {}
    };
};

////////////////////////////////////////////////////////////////////////////////

// the id is url-encoded, or a '#'/'&' in it would truncate or split the
// Volume query parameter
TString VolumeSearchPath(const TString& diskId)
{
    return "service?action=search&Volume=" + CGIEscapeRet(diskId);
}

// the disk's page on a remote host, under the deployment's hostname scheme
// (bastion, viewer, ...)
TString RemoteVolumeSearchUrl(
    const TString& fqdn,
    const TString& diskId,
    const TDiagnosticsConfig& diagnosticsConfig)
{
    return GetExternalHostUrl(fqdn, EHostService::Nbs, diagnosticsConfig) +
           "blockstore/" + VolumeSearchPath(diskId);
}

void RenderLink(IOutputStream& out, const TString& url, const TString& text)
{
    out << "<a href='" << EncodeHtmlPcdata(url)
        << "' target='_blank' rel='noopener'>" << EncodeHtmlPcdata(text)
        << "</a>";
}

// a few rules bootstrap does not have; everything else is its classes, and
// folding is plain <details>, so the page needs no script of its own
constexpr TStringBuf PageStyle =
    "<style>"
    ".cells-page summary{cursor:pointer}"
    ".cells-page .stat{font-size:24px}"
    ".cells-page .panel>table{margin-bottom:0}"
    "</style>";

void RenderLabel(IOutputStream& out, TStringBuf kind, const TString& text)
{
    out << "<span class='label label-" << kind << "'>"
        << EncodeHtmlPcdata(text) << "</span>";
}

void RenderSearchPanel(
    IOutputStream& out,
    const TString& diskId,
    const TString& resultHtml)
{
    out << "<div class='panel panel-default'>"
        << "<div class='panel-heading'>"
        << "<h3 class='panel-title'>Search volume</h3></div>"
        << "<div class='panel-body'>"
        << "<form method='GET' class='form-inline'>"
        << "<div class='input-group'>"
        << "<span class='input-group-addon'>Volume</span>"
        << "<input type='text' class='form-control' name='Volume' value='"
        << EncodeHtmlPcdata(diskId) << "'/>"
        << "<span class='input-group-btn'>"
        << "<button class='btn btn-primary' type='submit'>Search</button>"
        << "</span></div>"
        << "<input type='hidden' name='action' value='search'/>"
        << "</form>" << resultHtml << "</div></div>";
}

TString RenderSearchResultTable(
    const TVector<TCellDescribeResult>& results,
    const TDiagnosticsConfig& diagnosticsConfig,
    const TString& localCellId,
    const TString& diskId)
{
    TStringStream out;
    HTML(out) {
        TAG(TH4) { out << "Search result for " << EncodeHtmlPcdata(diskId); }
        TABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Cell"; }
                    TABLEH() { out << "Volume"; }
                    TABLEH() { out << "Host"; }
                    TABLEH() { out << "Status"; }
                }
            }
            TABLEBODY() {
                for (const auto& result: results) {
                    TABLER() {
                        TABLED() {
                            if (result.CellId) {
                                out << EncodeHtmlPcdata(*result.CellId);
                            } else {
                                out << "local ("
                                    << EncodeHtmlPcdata(localCellId) << ")";
                            }
                        }
                        TABLED() {
                            if (result.Status == ECellDescribeStatus::Found) {
                                // the local disk is on this same node: linked
                                // relative to /blockstore/Cells so the Viewer
                                // node prefix is preserved
                                RenderLink(
                                    out,
                                    result.CellId
                                        ? RemoteVolumeSearchUrl(
                                              result.Fqdn,
                                              diskId,
                                              diagnosticsConfig)
                                        : VolumeSearchPath(diskId),
                                    diskId);
                            }
                        }
                        TABLED() { out << EncodeHtmlPcdata(result.Fqdn); }
                        TABLED() {
                            switch (result.Status) {
                                case ECellDescribeStatus::Found:
                                    RenderLabel(out, "success", "found");
                                    break;
                                case ECellDescribeStatus::MigrationDestination:
                                    RenderLabel(out, "info", "migration copy");
                                    break;
                                case ECellDescribeStatus::NotFound:
                                    RenderLabel(out, "default", "not found");
                                    break;
                                case ECellDescribeStatus::Unavailable:
                                    RenderLabel(out, "warning", "unavailable");
                                    break;
                                case ECellDescribeStatus::Failed:
                                    RenderLabel(out, "danger", "failed");
                                    out << " "
                                        << EncodeHtmlPcdata(
                                               FormatError(result.Error));
                                    break;
                            }
                        }
                    }
                }
            }
        }
    }
    return out.Str();
}

void RenderStat(
    IOutputStream& out,
    TStringBuf box,
    const TString& value,
    const TString& caption)
{
    out << "<div class='col-sm-3'><div class='" << box << "'>"
        << "<div class='stat'>" << value << "</div>"
        << "<small class='text-muted'>" << caption << "</small>"
        << "</div></div>";
}

void RenderSummary(
    IOutputStream& out,
    const TCellsConfig& config,
    const TCellsSnapshot& snapshot)
{
    // a host nobody pings is only "not found dead yet", so it is kept out of
    // the alive count rather than vouched for
    ui32 alive = 0;
    ui32 total = 0;
    ui32 notProbed = 0;
    for (const auto& [cellId, statuses]: snapshot.HostStatuses) {
        const auto* cellConfig = config.GetCells().FindPtr(cellId);
        if (!cellConfig || !(*cellConfig)->GetHostMigrationEnabled()) {
            notProbed += statuses.size();
            continue;
        }
        total += statuses.size();
        alive += CountIf(statuses, [](const auto& s) { return s.Alive; });
    }

    THashSet<TString> peers;
    for (const auto& row: snapshot.InboundActivity) {
        peers.insert(row.Peer);
    }

    out << "<div class='row'>";
    RenderStat(
        out,
        "well well-sm",
        ToString(config.GetCells().size()),
        "remote cells");
    RenderStat(
        out,
        alive < total ? "alert alert-warning" : "well well-sm",
        TStringBuilder() << alive << " / " << total,
        notProbed ? TStringBuilder() << "hosts alive, " << notProbed
                                     << " not probed"
                  : TStringBuilder() << "hosts alive");
    RenderStat(
        out,
        "well well-sm",
        ToString(snapshot.Mounts.size()),
        "remote mounts");
    RenderStat(out, "well well-sm", ToString(peers.size()), "inbound peers");
    out << "</div>";
}

TString DescribeTransport(
    NProto::ECellDataTransport transport,
    bool grpcDataFallbackEnabled)
{
    switch (transport) {
        case NProto::CELL_DATA_TRANSPORT_GRPC:
            return "grpc";
        case NProto::CELL_DATA_TRANSPORT_RDMA:
            return grpcDataFallbackEnabled ? "rdma + grpc fallback" : "rdma";
        default:
            return "unknown";
    }
}

TString FormatPort(ui32 port)
{
    return port ? ToString(port) : TString("&mdash;");
}

void RenderCell(
    IOutputStream& out,
    const TString& cellId,
    const TCellConfig& cellConfig,
    const TVector<TCellHostStatus>& statuses)
{
    const auto alive = static_cast<ui32>(
        CountIf(statuses, [](const auto& s) { return s.Alive; }));
    const auto total = static_cast<ui32>(statuses.size());

    // without pings a host is alive only in the sense that nothing has said
    // otherwise, so such a cell is never shown as healthy
    const bool probed = cellConfig.GetHostMigrationEnabled();

    TStringBuf health = "warning";
    if (!total || (alive == total && !probed)) {
        health = "default";
    } else if (alive == total) {
        health = "success";
    } else if (!alive) {
        health = "danger";
    }

    // a healthy cell folds away: its heading already says all there is
    out << "<details class='panel panel-" << health << "'"
        << (health == "success" ? "" : " open") << ">"
        << "<summary class='panel-heading'><strong>"
        << EncodeHtmlPcdata(cellId) << "</strong> ";
    // a host can override it, see the hosts' own column
    RenderLabel(
        out,
        "default",
        "default: " + DescribeTransport(
                          cellConfig.GetTransport(),
                          cellConfig.GetGrpcDataFallbackEnabled()));
    out << " ";
    if (probed) {
        RenderLabel(
            out,
            health,
            TStringBuilder() << alive << " / " << total << " alive");
    } else {
        RenderLabel(out, "default", "not probed");
    }
    out << "</summary>";

    HTML(out) {
        TABLE_CLASS("table table-striped table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Host"; }
                    TABLEH() { out << "State"; }
                    TABLEH() { out << "Connections"; }
                    TABLEH() { out << "Transport"; }
                    TABLEH() { out << "gRPC"; }
                    TABLEH() { out << "Secure gRPC"; }
                    TABLEH() { out << "RDMA"; }
                }
            }
            TABLEBODY() {
                for (const auto& status: statuses) {
                    // a host found by discovery takes the cell's ports
                    NProto::TCellHostConfig proto;
                    proto.SetFqdn(status.Fqdn);
                    const auto* known =
                        cellConfig.GetHosts().FindPtr(status.Fqdn);
                    const auto host =
                        known ? *known : TCellHostConfig(proto, cellConfig);

                    TABLER() {
                        TABLED() { out << EncodeHtmlPcdata(status.Fqdn); }
                        TABLED() {
                            if (!status.Alive) {
                                RenderLabel(out, "danger", "down");
                            } else if (!probed) {
                                RenderLabel(out, "default", "not probed");
                            } else {
                                RenderLabel(out, "success", "alive");
                            }
                            if (status.Warm) {
                                out << " ";
                                RenderLabel(out, "info", "warm");
                            }
                        }
                        TABLED() {
                            out << "<span class='badge'>"
                                << status.Connections << "</span>";
                        }
                        TABLED() {
                            out << DescribeTransport(
                                host.GetTransport(),
                                host.GetGrpcDataFallbackEnabled());
                        }
                        TABLED() { out << FormatPort(host.GetGrpcPort()); }
                        TABLED() {
                            out << FormatPort(host.GetSecureGrpcPort());
                        }
                        TABLED() { out << FormatPort(host.GetRdmaPort()); }
                    }
                }
            }
        }
    }
    out << "</details>";
}

void RenderCells(
    IOutputStream& out,
    const TCellsConfig& config,
    const THashMap<TString, TVector<TCellHostStatus>>& hostStatuses)
{
    HTML(out) {
        TAG(TH3) { out << "Cells"; }
    }

    TVector<TString> cellIds;
    for (const auto& [cellId, cellConfig]: config.GetCells()) {
        Y_UNUSED(cellConfig);
        cellIds.push_back(cellId);
    }
    Sort(cellIds);

    const TVector<TCellHostStatus> none;
    for (const auto& cellId: cellIds) {
        const auto* statuses = hostStatuses.FindPtr(cellId);
        RenderCell(
            out,
            cellId,
            *config.GetCells().at(cellId),
            statuses ? *statuses : none);
    }
}

TStringBuf TransportLabelKind(const TString& transport)
{
    if (transport == "rdma") {
        return "success";
    }
    if (transport == "grpc fallback") {
        return "warning";
    }
    return "default";
}

void RenderMounts(
    IOutputStream& out,
    const TVector<TCellMountStatus>& mounts,
    const TDiagnosticsConfig& diagnosticsConfig)
{
    out << "<h3>Remote mounts <small>disks this node serves through other "
           "cells</small></h3>";
    if (mounts.empty()) {
        out << "<p class='text-muted'>None.</p>";
        return;
    }

    HTML(out) {
        TABLE_SORTABLE_CLASS("table table-striped table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Disk"; }
                    TABLEH() { out << "Client"; }
                    TABLEH() { out << "Cell"; }
                    TABLEH() { out << "Host"; }
                    TABLEH() { out << "Transport"; }
                    TABLEH() { out << "Tablet"; }
                }
            }
            TABLEBODY() {
                for (const auto& mount: mounts) {
                    TABLER() {
                        TABLED() { out << EncodeHtmlPcdata(mount.DiskId); }
                        TABLED() { out << EncodeHtmlPcdata(mount.ClientId); }
                        TABLED() { out << EncodeHtmlPcdata(mount.CellId); }
                        TABLED() {
                            RenderLink(
                                out,
                                RemoteVolumeSearchUrl(
                                    mount.Host,
                                    mount.DiskId,
                                    diagnosticsConfig),
                                mount.Host);
                        }
                        TABLED() {
                            RenderLabel(
                                out,
                                TransportLabelKind(mount.DataTransport),
                                mount.DataTransport);
                        }
                        TABLED() {
                            // empty when the cell is older than the field
                            if (mount.TabletHost == mount.Host) {
                                out << "<span class='text-muted'>"
                                       "same host</span>";
                            } else if (mount.TabletHost) {
                                RenderLink(
                                    out,
                                    RemoteVolumeSearchUrl(
                                        mount.TabletHost,
                                        mount.DiskId,
                                        diagnosticsConfig),
                                    mount.TabletHost);
                                out << " ";
                                RenderLabel(out, "warning", "elsewhere");
                            }
                        }
                    }
                }
            }
        }
    }
}

TString FormatAgo(TDuration age)
{
    if (age < TDuration::Minutes(1)) {
        return TStringBuilder() << age.Seconds() << " s ago";
    }
    return TStringBuilder() << age.Minutes() << " min ago";
}

void RenderInbound(
    IOutputStream& out,
    const TVector<TCellInboundActivity::TRow>& rows,
    TInstant now)
{
    out << "<h3>Inbound <small>other cells mounting disks here, last "
        << TCellInboundActivity::Ttl.Minutes() << " min</small></h3>";
    if (rows.empty()) {
        out << "<p class='text-muted'>None.</p>";
        return;
    }

    HTML(out) {
        TABLE_SORTABLE_CLASS("table table-striped table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Peer"; }
                    TABLEH() { out << "Disk"; }
                    TABLEH() { out << "Client"; }
                    TABLEH() { out << "Last mount"; }
                }
            }
            TABLEBODY() {
                for (const auto& row: rows) {
                    TABLER() {
                        TABLED() { out << EncodeHtmlPcdata(row.Peer); }
                        TABLED() { out << EncodeHtmlPcdata(row.DiskId); }
                        TABLED() { out << EncodeHtmlPcdata(row.ClientId); }
                        TABLED() { out << FormatAgo(now - row.LastSeen); }
                    }
                }
            }
        }
    }
}

void RenderConfig(IOutputStream& out, const TCellsConfig& config)
{
    out << "<details class='panel panel-default'>"
        << "<summary class='panel-heading'><strong>Cells config</strong> "
        << "<span class='text-muted'>raw</span></summary>"
        << "<div class='panel-body'>";
    config.DumpHtml(out);
    for (const auto& [cellId, cellConfig]: config.GetCells()) {
        out << "<h4>" << EncodeHtmlPcdata(cellId) << "</h4>";
        cellConfig->DumpHtml(out);
    }
    out << "</div></details>";
}

////////////////////////////////////////////////////////////////////////////////

// The /blockstore/Cells page. Renders the manager's snapshot on a plain open;
// delegates a disk search to the manager's SearchVolume and renders the result
// once its future completes, so the mon thread never waits on RPCs.
class TCellsMonActor final
    : public TActorBootstrapped<TCellsMonActor>
{
private:
    const ICellManagerPtr CellManager;
    const TDiagnosticsConfigPtr DiagnosticsConfig;

public:
    TCellsMonActor(
            ICellManagerPtr cellManager,
            TDiagnosticsConfigPtr diagnosticsConfig)
        : CellManager(std::move(cellManager))
        , DiagnosticsConfig(std::move(diagnosticsConfig))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        auto* mon = AppData(ctx)->Mon;
        if (mon) {
            auto* root = mon->RegisterIndexPage("blockstore", "BlockStore");
            mon->RegisterActorPage(
                root, "Cells", "Cells", false, ctx.ActorSystem(), SelfId());
        }
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(NMon::TEvHttpInfo, HandleHttpInfo);
            HFunc(TEvPrivate::TEvSearchCompleted, HandleSearchCompleted);
            default:
                break;
        }
    }

private:
    void HandleHttpInfo(
        const NMon::TEvHttpInfo::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto& params = ev->Get()->Request.GetParams();

        if (params.Get("action") == "search" && params.Get("Volume")) {
            StartSearch(ctx, ev->Sender, ev->Cookie, params.Get("Volume"));
            return;
        }

        TStringStream out;
        RenderCellsPage(
            out,
            *CellManager->Config,
            CellManager->GetSnapshot(),
            *DiagnosticsConfig);

        ctx.Send(
            ev->Sender,
            new NMon::TEvHttpInfoRes(out.Str()),
            0,
            ev->Cookie);
    }

    void StartSearch(
        const TActorContext& ctx,
        TActorId replyTo,
        ui64 cookie,
        const TString& diskId)
    {
        auto timeout = CellManager->Config->GetDescribeVolumeTimeout();
        if (!timeout || timeout > MonitoringSearchTimeout) {
            timeout = MonitoringSearchTimeout;
        }

        auto future =
            CellManager->SearchVolume(diskId, timeout);

        auto* actorSystem = ctx.ActorSystem();
        const auto self = SelfId();
        future.Subscribe(
            [actorSystem, self, replyTo, cookie, diskId] (const auto& f)
            {
                actorSystem->Send(
                    self,
                    new TEvPrivate::TEvSearchCompleted(
                        replyTo,
                        cookie,
                        diskId,
                        f.GetValue()));
            });
    }

    void HandleSearchCompleted(
        const TEvPrivate::TEvSearchCompleted::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();

        TStringStream out;
        RenderCellsSearchResult(
            out,
            msg->Results,
            *DiagnosticsConfig,
            CellManager->Config->GetCellId(),
            msg->DiskId);

        ctx.Send(
            msg->ReplyTo,
            new NMon::TEvHttpInfoRes(out.Str()),
            0,
            msg->Cookie);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void RenderCellsPage(
    IOutputStream& out,
    const TCellsConfig& config,
    const TCellsSnapshot& snapshot,
    const TDiagnosticsConfig& diagnosticsConfig)
{
    out << PageStyle << "<div class='cells-page'>"
        << "<h2>Cells <span class='label label-primary'>this node: "
        << EncodeHtmlPcdata(config.GetCellId()) << "</span></h2>";
    RenderSummary(out, config, snapshot);
    RenderSearchPanel(out, {}, {});
    RenderCells(out, config, snapshot.HostStatuses);
    RenderMounts(out, snapshot.Mounts, diagnosticsConfig);
    RenderInbound(out, snapshot.InboundActivity, snapshot.Taken);
    RenderConfig(out, config);
    out << "</div>";
}

void RenderCellsSearchResult(
    IOutputStream& out,
    const TVector<TCellDescribeResult>& results,
    const TDiagnosticsConfig& diagnosticsConfig,
    const TString& localCellId,
    const TString& diskId)
{
    out << PageStyle << "<div class='cells-page'>";
    RenderSearchPanel(
        out,
        diskId,
        RenderSearchResultTable(
            results,
            diagnosticsConfig,
            localCellId,
            diskId));
    out << "</div>";
}

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateCellsMonActor(
    ICellManagerPtr cellManager,
    TDiagnosticsConfigPtr diagnosticsConfig)
{
    return std::make_unique<TCellsMonActor>(
        std::move(cellManager),
        std::move(diagnosticsConfig));
}

}   // namespace NCloud::NBlockStore::NCells
